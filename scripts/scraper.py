"""
Pakistan-side scraper: picks up profile requests from GitHub, scrapes via Hyperbrowser,
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
from hyperbrowser.models.scrape import StartScrapeJobParams, ScrapeOptions

# --- Config ---
HB_API_KEY = os.environ.get("HB_API_KEY", "hb_c954e7f6d25b0107fefcee51319b")
NEON_CONN = os.environ.get("NEON_DATABASE_URL", "")
LI_AT_COOKIE = os.environ.get("LI_AT_COOKIE", "AQEDAUnx49UA-xrTAAABnTB8w8sAAAGdVIlHy00AqxqgZC8WppDYrvdKQKoRSODpqQUnTIoDYs3e3VALS4SE0xGmz1vWLZW1_eNCdMBkU1KwWZINiWtKkLTk1b44XLugNEFyJgz7wV4C1mZZUzgEGZ9N")
BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUESTS_DIR = os.path.join(BRIDGE_DIR, "requests")
RESULTS_DIR = os.path.join(BRIDGE_DIR, "results")


def scrape_linkedin_profile(url: str) -> dict:
    """Scrape a LinkedIn profile using Hyperbrowser's scrape API."""
    client = Hyperbrowser(api_key=HB_API_KEY)

    print(f"[*] Scraping: {url}")
    try:
        result = client.scrape.start_and_wait(
            StartScrapeJobParams(
                url=url,
                scrape_options=ScrapeOptions(
                    formats=["markdown"],
                    only_main_content=True,
                    wait_for=5000,
                ),
                session_options={
                    "use_stealth": True,
                    "solve_captchas": True,
                    "accept_cookies": True,
                },
            )
        )

        raw_text = result.data.markdown if result.data else ""
        if not raw_text:
            return {"error": "No content returned", "url": url}

        profile = parse_profile(raw_text, url)
        return profile

    except Exception as e:
        print(f"[!] Scrape error: {e}")
        return {"error": str(e), "url": url}


def parse_profile(text: str, url: str) -> dict:
    """Parse raw LinkedIn text into structured profile data."""
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
    }

    # Try to extract name (usually first meaningful line)
    for line in lines[:5]:
        cleaned = line.strip("#").strip()
        if cleaned and len(cleaned) < 100 and not cleaned.startswith("http"):
            profile["name"] = cleaned
            break

    # Extract sections by scanning for headers
    current_section = ""
    section_lines = []

    for line in lines:
        lower = line.lower().strip("#").strip()
        if lower in ("experience", "education", "skills", "about", "activity"):
            if current_section and section_lines:
                _assign_section(profile, current_section, section_lines)
            current_section = lower
            section_lines = []
        else:
            section_lines.append(line)

    if current_section and section_lines:
        _assign_section(profile, current_section, section_lines)

    # Headline is often the second meaningful line
    if len(lines) > 1:
        profile["headline"] = lines[1].strip("#").strip()

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
    elif section == "skills":
        profile["skills"] = [l.strip("- •·").strip() for l in lines if l.strip("- •·").strip()]


def _parse_entries(lines: list) -> list:
    """Parse experience/education entries from lines."""
    entries = []
    current = {}
    for line in lines:
        cleaned = line.strip("- •·#").strip()
        if not cleaned:
            continue
        if cleaned.startswith("![") or cleaned.startswith("http"):
            continue
        # New entry heuristic: short line that's likely a title/company
        if len(cleaned) < 80 and not any(c in cleaned for c in ["·", "•", "yr", "mo"]):
            if current:
                entries.append(current)
            current = {"title": cleaned}
        elif current:
            if "company" not in current:
                current["company"] = cleaned
            elif "dates" not in current:
                current["dates"] = cleaned
            else:
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

        # Skip already processed
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

        # Rate limit between profiles
        time.sleep(5)

    return processed


def git_sync():
    """Pull latest requests, push results."""
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
        # Direct scrape mode
        url = sys.argv[2] if len(sys.argv) > 2 else "https://www.linkedin.com/in/atharva-kasar/"
        profile = scrape_linkedin_profile(url)
        save_to_neon(profile)
        print(json.dumps(profile, indent=2))
    else:
        run_once()
