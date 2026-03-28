"""
Pakistan-side watcher — Crosby CRM sourcing pipeline.
Polls GitHub every 30s. Pure Hyperbrowser, no Playwright.
"""
import os
import sys
import json
import time
import re
import random
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


def poll_bridge_search_requests():
    """Poll Neon DB for pending search requests from Vercel/USA side."""
    if not NEON_CONN:
        return 0
    scraped = 0
    try:
        import psycopg2
        conn = psycopg2.connect(NEON_CONN)
        cur = conn.cursor()

        # Get pending requests (newest first — fresh queries matter most)
        cur.execute("SELECT id, job_title, companies, location, keywords, max_results, role, company, job_id, pipeline_run_id FROM bridge_search_requests WHERE status = 'pending' ORDER BY requested_at DESC LIMIT 5")
        rows = cur.fetchall()

        if not rows:
            cur.close()
            conn.close()
            return 0

        print(f"[*] {len(rows)} pending bridge search request(s)")

        for row in rows:
            req_id, job_title, companies, location, keywords, max_results, role, req_company, job_id, pipeline_run_id = row
            max_results = max_results or 10

            # Mark as processing
            cur.execute("UPDATE bridge_search_requests SET status = 'processing' WHERE id = %s", (req_id,))
            conn.commit()

            try:
                # Build search query for LinkedIn Sales Navigator
                search_parts = []
                if job_title:
                    search_parts.append(job_title)
                if keywords:
                    search_parts.append(keywords)
                if location:
                    search_parts.append(location)

                search_query = " ".join(search_parts)
                print(f"[*] Bridge search #{req_id}: '{search_query}'")

                # Use Hyperbrowser Extract to search LinkedIn
                from hyperbrowser import Hyperbrowser
                from hyperbrowser.models.extract import StartExtractJobParams
                from hyperbrowser.models.session import CreateSessionParams, CreateSessionProfile
                from scraper import HB_API_KEY, HB_PROFILE_ID
                from urllib.parse import quote

                hb = Hyperbrowser(api_key=HB_API_KEY)

                # Build Sales Nav search URL
                sn_keywords = quote(search_query)
                search_url = f"https://www.linkedin.com/sales/search/people?query=(keywords:{sn_keywords},spellCorrectionEnabled:true)"

                # Extract search results
                result = hb.extract.start_and_wait(StartExtractJobParams(
                    urls=[search_url],
                    prompt=f"""Extract all people/lead results from this LinkedIn Sales Navigator search page.
For each person found, extract: full name, headline/job title, current company, location, LinkedIn profile URL.
Look for search result cards/items on the page. Return up to {max_results} results.""",
                    schema_={
                        "type": "object",
                        "properties": {
                            "results": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "headline": {"type": "string"},
                                        "company": {"type": "string"},
                                        "location": {"type": "string"},
                                        "linkedin_url": {"type": "string"},
                                    }
                                }
                            },
                            "total_results": {"type": "integer"},
                        }
                    },
                    session_options=CreateSessionParams(
                        use_stealth=True,
                        solve_captchas=True,
                        accept_cookies=True,
                        profile=CreateSessionProfile(id=HB_PROFILE_ID),
                    ),
                    wait_for=5000,
                ))

                leads = result.data.get("results", []) if result.data else []
                print(f"[+] Found {len(leads)} leads for search #{req_id}")

                # Only scrape NEW profiles — skip ones already in DB
                new_leads = []
                for lead in leads[:max_results]:
                    url = lead.get("linkedin_url", "")
                    if url and "linkedin.com" in url:
                        cur.execute("SELECT id FROM profiles WHERE url = %s", (url,))
                        if cur.fetchone() is None:
                            new_leads.append(lead)
                        else:
                            print(f"[*] Skip (already in DB): {lead.get('name', '?')}")

                print(f"[*] {len(new_leads)} new profiles to scrape (skipped {len(leads) - len(new_leads)} existing)")

                for lead in new_leads:
                    url = lead.get("linkedin_url", "")
                    try:
                        profile = scrape_linkedin_profile(url)
                        # Tag profile with the request it came from
                        profile["sourced_from_request_id"] = req_id
                        profile["sourced_role"] = role or job_title or ""
                        profile["sourced_company"] = req_company or ""
                        profile["sourced_pipeline_run_id"] = pipeline_run_id or ""
                        save_to_neon(profile)
                        # Also update the profiles row with sourcing labels
                        cur.execute("""
                            UPDATE profiles SET
                                sourced_from_request_id = %s,
                                sourced_role = %s,
                                sourced_company = %s,
                                sourced_pipeline_run_id = %s
                            WHERE url = %s
                        """, (req_id, role or job_title, req_company, pipeline_run_id, url))
                        conn.commit()
                        scraped += 1
                        time.sleep(random.uniform(5, 12))
                    except Exception as e:
                        print(f"[!] Error scraping {lead.get('name')}: {e}")

                # Save raw search results (always, for tracking)
                cur.execute("CREATE TABLE IF NOT EXISTS search_results (id SERIAL PRIMARY KEY, query TEXT, name TEXT, headline TEXT, company TEXT, location TEXT, linkedin_url TEXT, searched_at TIMESTAMPTZ DEFAULT NOW())")
                for lead in leads:
                    cur.execute(
                        "INSERT INTO search_results (query, name, headline, company, location, linkedin_url) VALUES (%s,%s,%s,%s,%s,%s)",
                        (search_query, lead.get("name",""), lead.get("headline",""), lead.get("company",""), lead.get("location",""), lead.get("linkedin_url",""))
                    )

                # Mark completed
                cur.execute("UPDATE bridge_search_requests SET status = 'completed', completed_at = NOW(), result_count = %s WHERE id = %s", (len(leads), req_id))
                conn.commit()
                print(f"[+] Search #{req_id} done: {len(leads)} leads, {scraped} scraped")

            except Exception as e:
                print(f"[!] Search #{req_id} error: {e}")
                cur.execute("UPDATE bridge_search_requests SET status = 'error', error = %s WHERE id = %s", (str(e)[:500], req_id))
                conn.commit()

        cur.close()
        conn.close()
    except Exception as e:
        print(f"[!] Bridge poll error: {e}")
    return scraped


def run_cycle():
    print(f"\n[*] Cycle: {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
    git_pull()
    count = process_requests()
    unread = check_chat()
    chat_scraped = handle_messages(unread) if unread else 0

    # Poll Neon DB for bridge search requests from Vercel
    bridge_scraped = poll_bridge_search_requests()

    total = count + chat_scraped + bridge_scraped
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
