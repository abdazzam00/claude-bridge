"""
Pakistan-side watcher — Crosby CRM sourcing pipeline.
Polls GitHub every 30s. Pure Hyperbrowser, no Playwright.
"""
import os
import sys
import json
import time
import re
import subprocess
from datetime import datetime, timezone

BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMS_DIR = os.path.join(BRIDGE_DIR, "comms")
STATUS_FILE = os.path.join(COMMS_DIR, "status.json")
CHAT_FILE = os.path.join(COMMS_DIR, "chat.jsonl")

sys.path.insert(0, os.path.join(BRIDGE_DIR, "scripts"))
from scraper import scrape_linkedin_profile, save_to_neon, process_requests, NEON_CONN


def git_pull():
    result = subprocess.run(["git", "pull", "--rebase"], cwd=BRIDGE_DIR, capture_output=True, text=True, timeout=30)
    return "Already up to date" not in result.stdout


def git_push(msg="auto: watcher update"):
    subprocess.run(["git", "add", "-A"], cwd=BRIDGE_DIR, capture_output=True)
    r = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=BRIDGE_DIR, capture_output=True)
    if r.returncode != 0:
        subprocess.run(["git", "commit", "-m", msg], cwd=BRIDGE_DIR, capture_output=True)
        subprocess.run(["git", "push"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
        print(f"[+] Pushed: {msg}")


def update_status(profiles_scraped=0, error=None):
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
            errors.append({"time": datetime.now(timezone.utc).isoformat(), "msg": str(error)[:200]})
            pk["errors"] = errors[-10:]
        status["pakistan_side"] = pk
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2)
    except Exception:
        pass


def send_chat(message, msg_type="status"):
    msg = {"from": "pakistan", "type": msg_type, "message": message,
           "timestamp": datetime.now(timezone.utc).isoformat(), "read_by_usa": False}
    with open(CHAT_FILE, "a") as f:
        f.write(json.dumps(msg) + "\n")


def check_chat():
    if not os.path.exists(CHAT_FILE):
        return []
    messages = []
    with open(CHAT_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    messages.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    unread = [m for m in messages if m.get("from") == "usa" and not m.get("read_by_pakistan")]
    if unread:
        for msg in unread:
            msg["read_by_pakistan"] = True
        with open(CHAT_FILE, "w") as f:
            for msg in messages:
                f.write(json.dumps(msg) + "\n")
    return unread


def handle_messages(unread):
    scraped = 0
    for msg in unread:
        content = msg.get("message", "")
        msg_type = msg.get("type", "message")
        lower = content.lower()
        print(f"[*] USA ({msg_type}): {content[:80]}")

        if "ping" in lower or "status" in lower or "alive" in lower:
            send_chat("Online. Ready to scrape.", "response")

        elif "linkedin.com" in content:
            urls = re.findall(r'https?://(?:www\.)?linkedin\.com/(?:in|sales/lead|sales/people)/[^\s"]+', content)
            if urls:
                send_chat(f"Scraping {len(urls)} profile(s)...", "response")
                for url in urls:
                    try:
                        profile = scrape_linkedin_profile(url)
                        save_to_neon(profile)
                        slug = url.rstrip("/").split("/")[-1]
                        with open(os.path.join(BRIDGE_DIR, "results", f"chat_{slug}.json"), "w") as f:
                            json.dump(profile, f, indent=2, default=str)
                        send_chat(f"Done: {profile.get('name', '?')} — {profile.get('headline', '')}",  "result")
                        scraped += 1
                    except Exception as e:
                        send_chat(f"Error: {str(e)[:100]}", "error")
        else:
            send_chat(f"Received: {content[:50]}", "ack")
    return scraped


def run_cycle():
    print(f"\n[*] Cycle: {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
    git_pull()
    count = process_requests()
    unread = check_chat()
    chat_scraped = handle_messages(unread) if unread else 0
    total = count + chat_scraped
    update_status(profiles_scraped=total)
    if total > 0 or unread:
        git_push(f"auto: {total} scraped, {len(unread)} msgs")
    return total


def main(interval=30):
    print(f"[*] Crosby CRM Pakistan Watcher — every {interval}s")
    print(f"[*] Neon: {'yes' if NEON_CONN else 'NO'}")
    send_chat("Pakistan watcher online.", "startup")
    git_push("auto: pakistan online")

    while True:
        try:
            run_cycle()
        except KeyboardInterrupt:
            send_chat("Shutting down.", "shutdown")
            git_push("auto: offline")
            break
        except Exception as e:
            print(f"[!] Error: {e}")
            update_status(error=str(e))
        time.sleep(interval)


if __name__ == "__main__":
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    main(interval)
