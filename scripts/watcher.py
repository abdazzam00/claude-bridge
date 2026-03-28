"""
Pakistan-side watcher cron job.
- Polls GitHub for new requests
- Scrapes profiles
- Pushes results back
- Updates status.json heartbeat
- Checks comms/chat.jsonl for messages from USA side and responds
"""
import os
import sys
import json
import time
import subprocess
from datetime import datetime, timezone

BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMS_DIR = os.path.join(BRIDGE_DIR, "comms")
STATUS_FILE = os.path.join(COMMS_DIR, "status.json")
CHAT_FILE = os.path.join(COMMS_DIR, "chat.jsonl")

# Add scripts to path
sys.path.insert(0, os.path.join(BRIDGE_DIR, "scripts"))
from scraper import scrape_linkedin_profile, save_to_neon, process_requests

NEON_CONN = os.environ.get("NEON_DATABASE_URL", "")


def git_pull():
    result = subprocess.run(
        ["git", "pull", "--rebase"],
        cwd=BRIDGE_DIR, capture_output=True, text=True, timeout=30
    )
    changed = "Already up to date" not in result.stdout
    if changed:
        print(f"[*] Git pull: new changes detected")
    return changed


def git_push(msg="auto: watcher update"):
    subprocess.run(["git", "add", "-A"], cwd=BRIDGE_DIR, capture_output=True)
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=BRIDGE_DIR, capture_output=True
    )
    if result.returncode != 0:  # there are staged changes
        subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=BRIDGE_DIR, capture_output=True
        )
        subprocess.run(
            ["git", "push"],
            cwd=BRIDGE_DIR, capture_output=True, timeout=30
        )
        print(f"[+] Pushed: {msg}")
        return True
    return False


def update_status(profiles_scraped=0, error=None):
    """Update pakistan side status with heartbeat."""
    try:
        status = {}
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE, "r") as f:
                status = json.load(f)

        pk = status.get("pakistan_side", {})
        pk["status"] = "online"
        pk["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
        pk["profiles_scraped_total"] = pk.get("profiles_scraped_total", 0) + profiles_scraped
        if profiles_scraped > 0:
            pk["last_scrape"] = datetime.now(timezone.utc).isoformat()
        if error:
            errors = pk.get("errors", [])
            errors.append({"time": datetime.now(timezone.utc).isoformat(), "msg": str(error)})
            pk["errors"] = errors[-10:]  # keep last 10 errors
        status["pakistan_side"] = pk

        with open(STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2)
    except Exception as e:
        print(f"[!] Status update error: {e}")


def check_chat():
    """Check for new messages from USA side and log them."""
    if not os.path.exists(CHAT_FILE):
        return []

    messages = []
    with open(CHAT_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    msg = json.loads(line)
                    messages.append(msg)
                except json.JSONDecodeError:
                    pass

    # Find unread messages from USA
    unread = [m for m in messages if m.get("from") == "usa" and not m.get("read_by_pakistan")]
    if unread:
        print(f"[*] {len(unread)} new message(s) from USA side:")
        for msg in unread:
            print(f"    > {msg.get('message', '')}")
            msg["read_by_pakistan"] = True

        # Rewrite chat file with updated read status
        with open(CHAT_FILE, "w") as f:
            for msg in messages:
                f.write(json.dumps(msg) + "\n")

    return unread


def send_chat(message, msg_type="status"):
    """Send a message to the chat log."""
    msg = {
        "from": "pakistan",
        "type": msg_type,
        "message": message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "read_by_usa": False,
    }
    with open(CHAT_FILE, "a") as f:
        f.write(json.dumps(msg) + "\n")
    print(f"[+] Chat sent: {message}")


def check_neon_stats():
    """Get profile count from Neon DB."""
    if not NEON_CONN:
        return 0
    try:
        import psycopg2
        conn = psycopg2.connect(NEON_CONN)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM profiles")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception:
        return 0


def run_cycle():
    """Single watcher cycle."""
    now = datetime.now(timezone.utc).isoformat()
    print(f"\n{'='*60}")
    print(f"[*] Watcher cycle: {now}")

    # Pull latest
    changed = git_pull()

    # Check for chat messages
    unread = check_chat()
    for msg in unread:
        content = msg.get("message", "").lower()
        # Auto-respond to common messages
        if "status" in content or "alive" in content or "ping" in content:
            db_count = check_neon_stats()
            send_chat(f"Online. {db_count} profiles in DB. Ready to scrape.", "response")
        elif "scrape" in content and "http" in msg.get("message", ""):
            # Extract URL and queue it
            import re
            urls = re.findall(r'https?://www\.linkedin\.com/in/[^\s"]+', msg.get("message", ""))
            if urls:
                send_chat(f"Got it, scraping {len(urls)} profile(s) now.", "response")
                for u in urls:
                    profile = scrape_linkedin_profile(u)
                    save_to_neon(profile)
                    result_slug = u.rstrip("/").split("/")[-1]
                    result_file = os.path.join(BRIDGE_DIR, "results", f"chat_{result_slug}.json")
                    with open(result_file, "w") as f:
                        json.dump(profile, f, indent=2)
                    send_chat(f"Done: {profile.get('name', 'unknown')} — {profile.get('headline', '')}", "result")

    # Process any pending request files
    count = process_requests()
    update_status(profiles_scraped=count)

    # Push if anything changed
    if count > 0 or unread or changed:
        git_push(f"auto: watcher cycle — {count} scraped, {len(unread)} msgs")

    print(f"[*] Cycle done: {count} profiles scraped, {len(unread)} messages handled")
    return count


def main(interval=30):
    """Main watcher loop."""
    print(f"[*] Pakistan-side watcher started")
    print(f"[*] Polling every {interval}s")
    print(f"[*] Repo: {BRIDGE_DIR}")
    print(f"[*] Neon DB: {'connected' if NEON_CONN else 'NOT SET'}")

    send_chat("Pakistan watcher started and online.", "startup")
    git_push("auto: pakistan watcher online")

    while True:
        try:
            run_cycle()
        except KeyboardInterrupt:
            send_chat("Pakistan watcher shutting down.", "shutdown")
            git_push("auto: pakistan watcher offline")
            print("\n[*] Watcher stopped.")
            break
        except Exception as e:
            print(f"[!] Cycle error: {e}")
            update_status(error=str(e))
        time.sleep(interval)


if __name__ == "__main__":
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    main(interval)
