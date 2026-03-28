"""
Quick chat CLI — send messages between USA and Pakistan Claude Code instances.
Usage:
  python chat.py send "your message here"
  python chat.py read
  python chat.py read --unread
"""
import os
import sys
import json
from datetime import datetime, timezone

BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHAT_FILE = os.path.join(BRIDGE_DIR, "comms", "chat.jsonl")
SIDE = os.environ.get("CLAUDE_SIDE", "pakistan")  # set to "usa" on USA machine


def send(message, msg_type="message"):
    msg = {
        "from": SIDE,
        "type": msg_type,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        f"read_by_{'usa' if SIDE == 'pakistan' else 'pakistan'}": False,
    }
    with open(CHAT_FILE, "a") as f:
        f.write(json.dumps(msg) + "\n")
    print(f"[{SIDE}] Sent: {message}")


def read(unread_only=False):
    if not os.path.exists(CHAT_FILE):
        print("No messages yet.")
        return

    other = "usa" if SIDE == "pakistan" else "pakistan"
    with open(CHAT_FILE, "r") as f:
        lines = f.readlines()

    messages = []
    for line in lines:
        line = line.strip()
        if line:
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    if unread_only:
        messages = [m for m in messages if m.get("from") == other and not m.get(f"read_by_{SIDE}")]

    if not messages:
        print("No messages." if not unread_only else "No unread messages.")
        return

    for msg in messages:
        sender = msg.get("from", "?").upper()
        ts = msg.get("timestamp", "")[:19]
        text = msg.get("message", "")
        mtype = msg.get("type", "")
        prefix = f"[{sender}] [{ts}]"
        if mtype and mtype != "message":
            prefix += f" ({mtype})"
        print(f"{prefix} {text}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python chat.py send 'message' | python chat.py read [--unread]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "send" and len(sys.argv) > 2:
        send(" ".join(sys.argv[2:]))
    elif cmd == "read":
        unread = "--unread" in sys.argv
        read(unread_only=unread)
    else:
        print("Usage: python chat.py send 'message' | python chat.py read [--unread]")
