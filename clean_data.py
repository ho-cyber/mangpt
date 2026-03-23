import json
import os
import re
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("raw")
CLEANED_DIR = Path("cleaned")
CLEANED_DIR.mkdir(exist_ok=True)

SYSTEM_MESSAGE_TYPES = {
    20,
    23,
    46,
    "ThreadCreated",
    "ChannelPinnedMessage",
    "GuildMemberJoin",
    "GuildMemberRemove",
}

MIN_MESSAGE_LENGTH = 3
MAX_MESSAGE_LENGTH = 4096


def clean_content(content):
    """Comprehensive content cleaning"""
    if not content:
        return ""

    content = content.strip()

    content = content.replace("\u200b", "")  # zero-width space
    content = content.replace("\u200c", "")  # zero-width non-joiner
    content = content.replace("\u200d", "")  # zero-width joiner
    content = content.replace("\ufeff", "")  # zero-width no-break space
    content = content.replace("\u2800", "")  # braille blank
    content = content.replace("\xa0", " ")  # non-breaking space

    content = re.sub(r"<@[!&]?\d+>", "@user", content)  # user mentions
    content = re.sub(r"<#\d+>", "#channel", content)  # channel mentions
    content = re.sub(r"<a?:[a-zA-Z0-9_]+:\d+>", ":emoji:", content)  # custom emojis
    content = re.sub(r"@[\w.-]+", "@user", content)  # text mentions like @QOTD
    content = re.sub(r"#\S+", "#channel", content)  # text channel mentions

    content = re.sub(r"https?://\S+", "<URL>", content)  # URLs

    content = re.sub(r"\s+", " ", content)  # multiple spaces -> single space

    content = re.sub(r"\*{2,}[^\*]+\*{2,}", "[bold text]", content)  # bold
    content = re.sub(r"\*(?!\*)[^\*]+\*(?!\*)", "[italic text]", content)  # italic
    content = re.sub(r"_{2,}[^_]+_{2,}", "[underlined text]", content)  # underline
    content = re.sub(r"~~[^~]+~~", "[strikethrough]", content)  # strikethrough

    content = re.sub(r"```[\s\S]*?```", "[code block]", content)  # code blocks
    content = re.sub(r"`[^`\n]+`", "[code]", content)  # inline code

    content = re.sub(r">>>?\s?", "", content)  # block quotes

    content = content.strip()

    return content


def clean_code(match):
    code = match.group(0)
    if code.startswith("```"):
        return "[code]"
    return "[code]"


def should_keep_message(msg):
    if msg.get("type") in SYSTEM_MESSAGE_TYPES:
        return False

    if msg.get("author", {}).get("isBot", False):
        return False

    content = msg.get("content", "")
    if not content or len(content.strip()) == 0:
        return False

    cleaned = clean_content(content)
    if len(cleaned) < MIN_MESSAGE_LENGTH:
        return False
    if len(cleaned) > MAX_MESSAGE_LENGTH:
        return False

    return True


def process_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    channel_info = data.get("channel", {})
    messages = data.get("messages", [])

    cleaned_messages = []

    for msg in messages:
        if not should_keep_message(msg):
            continue

        author = msg.get("author", {})
        content = clean_content(msg.get("content", ""))

        if len(content) < MIN_MESSAGE_LENGTH:
            continue

        cleaned_msg = {
            "content": content,
            "author": (author.get("nickname") or author.get("name", "unknown")).strip(),
            "timestamp": msg.get("timestamp", ""),
            "channel": channel_info.get("name", ""),
            "category": channel_info.get("category", ""),
        }

        cleaned_messages.append(cleaned_msg)

    return cleaned_messages


def create_conversation_pairs(messages, max_gap_seconds=1800):
    pairs = []

    for i in range(len(messages) - 1):
        current = messages[i]
        next_msg = messages[i + 1]

        try:
            t1 = datetime.fromisoformat(
                current["timestamp"].replace("+05:30", "").replace("+00:00", "")
            )
            t2 = datetime.fromisoformat(
                next_msg["timestamp"].replace("+05:30", "").replace("+00:00", "")
            )
            gap = (t2 - t1).total_seconds()
        except:
            gap = 0

        if gap > max_gap_seconds:
            continue
        if current["author"] == next_msg["author"]:
            continue

        pairs.append(
            {
                "messages": [
                    {"role": "user", "content": current["content"]},
                    {"role": "assistant", "content": next_msg["content"]},
                ],
                "metadata": {
                    "channel": current["channel"],
                    "category": current["category"],
                },
            }
        )

    return pairs


def main():
    files = list(RAW_DIR.glob("*.json"))

    all_messages = []
    all_pairs = []
    stats = {"files_processed": 0, "messages_kept": 0, "pairs_created": 0}

    for fpath in sorted(files):
        try:
            messages = process_file(fpath)
            stats["files_processed"] += 1
            stats["messages_kept"] += len(messages)
            all_messages.extend(messages)

            pairs = create_conversation_pairs(messages)
            all_pairs.extend(pairs)
            stats["pairs_created"] += len(pairs)

        except Exception as e:
            print(f"Error: {fpath.name}: {e}")

    print(f"Files: {stats['files_processed']}")
    print(f"Messages: {stats['messages_kept']:,}")
    print(f"Pairs: {stats['pairs_created']:,}")

    with open(CLEANED_DIR / "messages.jsonl", "w", encoding="utf-8") as f:
        for msg in all_messages:
            f.write(json.dumps(msg, ensure_ascii=False) + "\n")

    with open(CLEANED_DIR / "pairs.jsonl", "w", encoding="utf-8") as f:
        for pair in all_pairs:
            f.write(json.dumps(pair, ensure_ascii=False) + "\n")

    print(f"\nSaved to {CLEANED_DIR}/")


if __name__ == "__main__":
    main()
