"""
Pakistan-side scraper: picks up profile requests from GitHub, scrapes via Hyperbrowser + Playwright,
writes results back to GitHub and Neon DB.
"""
import os
import sys
import json
import time
import glob
import subprocess
from datetime import datetime, timezone

from hyperbrowser import Hyperbrowser
from hyperbrowser.models.session import CreateSessionParams
from playwright.sync_api import sync_playwright

# --- Config ---
HB_API_KEY = os.environ.get("HB_API_KEY", "hb_c954e7f6d25b0107fefcee51319b")
NEON_CONN = os.environ.get("NEON_DATABASE_URL", "")
LI_AT_COOKIE = os.environ.get("LI_AT_COOKIE", "AQEDAUnx49UA-xrTAAABnTB8w8sAAAGdVIlHy00AqxqgZC8WppDYrvdKQKoRSODpqQUnTIoDYs3e3VALS4SE0xGmz1vWLZW1_eNCdMBkU1KwWZINiWtKkLTk1b44XLugNEFyJgz7wV4C1mZZUzgEGZ9N")
BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUESTS_DIR = os.path.join(BRIDGE_DIR, "requests")
RESULTS_DIR = os.path.join(BRIDGE_DIR, "results")


def scrape_linkedin_profile(url: str) -> dict:
    """Scrape a LinkedIn profile using Hyperbrowser session + Playwright CDP with li_at cookie."""
    hb = Hyperbrowser(api_key=HB_API_KEY)
    session = None

    try:
        print(f"[*] Creating Hyperbrowser session...")
        session = hb.sessions.create(
            CreateSessionParams(
                use_stealth=True,
                solve_captchas=True,
                accept_cookies=True,
            )
        )
        print(f"[+] Session created: {session.id}")
        ws_endpoint = session.ws_endpoint

        with sync_playwright() as pw:
            print(f"[*] Connecting Playwright via CDP...")
            browser = pw.chromium.connect_over_cdp(ws_endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()

            # Inject li_at cookie for LinkedIn auth
            context.add_cookies([{
                "name": "li_at",
                "value": LI_AT_COOKIE,
                "domain": ".linkedin.com",
                "path": "/",
                "httpOnly": True,
                "secure": True,
                "sameSite": "None",
            }])

            page = context.pages[0] if context.pages else context.new_page()

            print(f"[*] Navigating to: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for JS to render
            print("[*] Waiting for page to render...")
            page.wait_for_timeout(5000)

            # Extract page text
            raw_text = page.evaluate("document.body.innerText")

            if not raw_text or "Sign in" in raw_text[:200]:
                # Try waiting longer
                print("[*] Page may not be loaded, waiting more...")
                page.wait_for_timeout(5000)
                raw_text = page.evaluate("document.body.innerText")

            profile = parse_profile(raw_text, url)
            print(f"[+] Scraped: {profile.get('name', 'unknown')}")
            return profile

    except Exception as e:
        print(f"[!] Scrape error: {e}")
        return {"error": str(e), "url": url, "scraped_at": datetime.now(timezone.utc).isoformat()}

    finally:
        if session:
            try:
                hb.sessions.stop(session.id)
                print(f"[+] Session stopped: {session.id}")
            except Exception:
                pass


def parse_profile(text: str, url: str) -> dict:
    """Parse raw LinkedIn page text into structured profile data."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    profile = {
        "url": url,
        "raw_text": text[:5000],
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "name": "",
        "headline": "",
        "location": "",
        "about": "",
        "current_company": "",
        "experience": [],
        "education": [],
        "skills": [],
        "connections": "",
    }

    # Name is usually the first non-navigation line
    skip_prefixes = ("skip", "linkedin", "home", "my network", "jobs", "messaging", "notifications", "search")
    for line in lines:
        lower = line.lower()
        if any(lower.startswith(p) for p in skip_prefixes):
            continue
        if len(line) < 60 and not line.startswith("http"):
            profile["name"] = line
            break

    # Find headline (line after name, before location-like text)
    name_idx = -1
    for i, line in enumerate(lines):
        if line == profile["name"]:
            name_idx = i
            break

    if name_idx >= 0 and name_idx + 1 < len(lines):
        profile["headline"] = lines[name_idx + 1]
    if name_idx >= 0 and name_idx + 2 < len(lines):
        candidate = lines[name_idx + 2]
        if any(geo in candidate.lower() for geo in ["united states", "india", "uk", "canada", "area", "city", "new york", "san francisco", "london", "mumbai", "bangalore"]):
            profile["location"] = candidate

    # Extract sections
    current_section = ""
    section_lines = []
    section_keywords = {"experience", "education", "skills", "about", "licenses & certifications", "certifications"}

    for line in lines:
        lower = line.lower().strip()
        if lower in section_keywords:
            if current_section and section_lines:
                _assign_section(profile, current_section, section_lines)
            current_section = lower
            section_lines = []
        elif current_section:
            section_lines.append(line)

    if current_section and section_lines:
        _assign_section(profile, current_section, section_lines)

    # Connections
    for line in lines:
        if "connection" in line.lower() or "follower" in line.lower():
            profile["connections"] = line
            break

    return profile


def _assign_section(profile: dict, section: str, lines: list):
    """Assign parsed section data to profile dict."""
    text = "\n".join(lines)
    if section == "about":
        profile["about"] = text[:2000]
    elif section == "experience":
        profile["experience"] = _parse_entries(lines)
        if profile["experience"]:
            profile["current_company"] = profile["experience"][0].get("company", "")
    elif section == "education":
        profile["education"] = _parse_entries(lines)
    elif section in ("skills", "licenses & certifications", "certifications"):
        profile["skills"] = [l.strip("- •·").strip() for l in lines if l.strip("- •·").strip() and len(l) < 100][:20]


def _parse_entries(lines: list) -> list:
    """Parse experience/education entries from lines."""
    entries = []
    current = {}
    for line in lines:
        cleaned = line.strip("- •·").strip()
        if not cleaned or cleaned.startswith("http") or cleaned.startswith("Show "):
            continue
        # Date patterns indicate we're in an entry's detail
        has_date = any(month in cleaned for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]) or "Present" in cleaned
        if has_date and current:
            current["dates"] = cleaned
        elif len(cleaned) < 80 and not has_date:
            if current and "title" in current:
                if "company" not in current:
                    current["company"] = cleaned
                else:
                    entries.append(current)
                    current = {"title": cleaned}
            else:
                if current:
                    entries.append(current)
                current = {"title": cleaned}
        elif current:
            current["description"] = current.get("description", "") + " " + cleaned

    if current:
        entries.append(current)
    return entries[:10]


def save_to_neon(profile: dict):
    """Save profile data to Neon PostgreSQL."""
    if not NEON_CONN:
        print("[!] No NEON_DATABASE_URL set, skipping DB save")
        return

    try:
        import psycopg2
        conn = psycopg2.connect(NEON_CONN)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                name TEXT,
                headline TEXT,
                location TEXT,
                about TEXT,
                current_company TEXT,
                experience JSONB,
                education JSONB,
                skills JSONB,
                raw_text TEXT,
                scraped_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            INSERT INTO profiles (url, name, headline, location, about, current_company, experience, education, skills, raw_text, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                name = EXCLUDED.name,
                headline = EXCLUDED.headline,
                location = EXCLUDED.location,
                about = EXCLUDED.about,
                current_company = EXCLUDED.current_company,
                experience = EXCLUDED.experience,
                education = EXCLUDED.education,
                skills = EXCLUDED.skills,
                raw_text = EXCLUDED.raw_text,
                scraped_at = EXCLUDED.scraped_at,
                updated_at = NOW()
        """, (
            profile["url"],
            profile.get("name", ""),
            profile.get("headline", ""),
            profile.get("location", ""),
            profile.get("about", ""),
            profile.get("current_company", ""),
            json.dumps(profile.get("experience", [])),
            json.dumps(profile.get("education", [])),
            json.dumps(profile.get("skills", [])),
            profile.get("raw_text", "")[:5000],
            profile.get("scraped_at"),
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"[+] Saved to Neon DB: {profile.get('name', 'unknown')}")
    except Exception as e:
        print(f"[!] DB error: {e}")


def process_requests():
    """Watch requests/ dir, process new ones, write results."""
    request_files = sorted(glob.glob(os.path.join(REQUESTS_DIR, "*.json")))

    if not request_files:
        print("[*] No pending requests")
        return 0

    processed = 0
    for req_file in request_files:
        fname = os.path.basename(req_file)
        result_file = os.path.join(RESULTS_DIR, fname)

        if os.path.exists(result_file):
            continue

        with open(req_file, "r") as f:
            request = json.load(f)

        url = request.get("url", "")
        if not url:
            continue

        print(f"\n{'='*60}")
        print(f"[*] Processing request: {fname}")
        print(f"[*] URL: {url}")

        profile = scrape_linkedin_profile(url)
        save_to_neon(profile)

        with open(result_file, "w") as f:
            json.dump(profile, f, indent=2)

        print(f"[+] Result written: {result_file}")
        processed += 1
        time.sleep(5)

    return processed


def git_sync():
    """Pull latest requests."""
    try:
        subprocess.run(["git", "pull", "--rebase"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
    except Exception:
        pass


def git_push_results():
    """Commit and push results."""
    try:
        subprocess.run(["git", "add", "results/"], cwd=BRIDGE_DIR, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", f"results: scraped profiles {datetime.now().strftime('%Y%m%d_%H%M%S')}"],
            cwd=BRIDGE_DIR, capture_output=True
        )
        subprocess.run(["git", "push"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
        print("[+] Results pushed to GitHub")
    except Exception as e:
        print(f"[!] Git push error: {e}")


def run_once():
    """Single run: pull, process, push."""
    print(f"\n[*] Run at {datetime.now().isoformat()}")
    git_sync()
    count = process_requests()
    if count > 0:
        git_push_results()
    print(f"[*] Processed {count} profiles")
    return count


def run_watcher(interval=30):
    """Continuous watcher loop."""
    print(f"[*] Pakistan-side watcher started (polling every {interval}s)")
    print(f"[*] Watching: {REQUESTS_DIR}")
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[!] Error: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        run_watcher()
    elif len(sys.argv) > 1 and sys.argv[1] == "--url":
        url = sys.argv[2] if len(sys.argv) > 2 else "https://www.linkedin.com/in/atharva-kasar/"
        profile = scrape_linkedin_profile(url)
        save_to_neon(profile)
        print(json.dumps(profile, indent=2))
    else:
        run_once()
