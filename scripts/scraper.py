"""
Pakistan-side LinkedIn scraper — pure Hyperbrowser, no Playwright.

Uses:
- Hyperbrowser Extract API for structured data extraction
- Persistent browser profile with li_at cookie baked in
- No proxy needed (Pakistan direct)
- Anti-ban: stealth mode, captcha solving, rate limiting

Profile ID with LinkedIn cookies: 37791ee4-e006-43db-baee-03abbba062b1
"""
import os
import sys
import json
import time
import glob
import random
import subprocess
from datetime import datetime, timezone

from hyperbrowser import Hyperbrowser
from hyperbrowser.models.extract import StartExtractJobParams
from hyperbrowser.models.session import CreateSessionParams, CreateSessionProfile

# --- Config ---
HB_API_KEY = os.environ.get("HB_API_KEY", "hb_c954e7f6d25b0107fefcee51319b")
NEON_CONN = os.environ.get("NEON_DATABASE_URL", "")
HB_PROFILE_ID = os.environ.get("HB_PROFILE_ID", "37791ee4-e006-43db-baee-03abbba062b1")
BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUESTS_DIR = os.path.join(BRIDGE_DIR, "requests")
RESULTS_DIR = os.path.join(BRIDGE_DIR, "results")
MAX_PROFILES_PER_DAY = 50
DAILY_COUNT_FILE = os.path.join(BRIDGE_DIR, "config", "daily_count.json")

# LinkedIn profile extraction schema
PROFILE_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "headline": {"type": "string"},
        "location": {"type": "string"},
        "about": {"type": "string"},
        "connections": {"type": "integer"},
        "followers": {"type": "integer"},
        "current_company": {"type": "string"},
        "experience": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "company": {"type": "string"},
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "duration": {"type": "string"},
                    "location": {"type": "string"},
                    "description": {"type": "string"},
                    "employment_type": {"type": "string"},
                }
            }
        },
        "education": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "school": {"type": "string"},
                    "degree": {"type": "string"},
                    "field_of_study": {"type": "string"},
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "activities": {"type": "string"},
                }
            }
        },
        "skills": {"type": "array", "items": {"type": "string"}},
        "honors": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "date": {"type": "string"},
                    "issuer": {"type": "string"},
                }
            }
        },
        "certifications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "issuer": {"type": "string"},
                    "date": {"type": "string"},
                }
            }
        },
        "languages": {"type": "array", "items": {"type": "string"}},
        "volunteering": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "role": {"type": "string"},
                    "organization": {"type": "string"},
                    "dates": {"type": "string"},
                }
            }
        },
        "projects": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "description": {"type": "string"},
                    "dates": {"type": "string"},
                }
            }
        },
        "publications": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "publisher": {"type": "string"},
                    "date": {"type": "string"},
                }
            }
        },
        "recommendations_received": {"type": "integer"},
        "contact_info": {
            "type": "object",
            "properties": {
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "website": {"type": "string"},
                "twitter": {"type": "string"},
            }
        },
    }
}

EXTRACT_PROMPT = """Extract ALL information from this LinkedIn profile. Include every detail visible:
- Full name, headline, location, about/summary
- All experience entries with job title, company, dates, location, description, employment type
- All education with school, degree, field of study, dates, activities
- All skills listed
- Honors & awards with title, date, issuer
- Certifications with name, issuer, date
- Languages
- Volunteer experience
- Projects
- Publications
- Number of connections and followers
- Contact info (email, phone, website, twitter) if visible
Be thorough — capture everything on the page."""


def get_client():
    return Hyperbrowser(api_key=HB_API_KEY)


def get_session_options():
    return CreateSessionParams(
        use_stealth=True,
        solve_captchas=True,
        accept_cookies=True,
        profile=CreateSessionProfile(id=HB_PROFILE_ID),
    )


# ============================================================
# RATE LIMITING
# ============================================================

def check_daily_limit():
    today = datetime.now().strftime("%Y-%m-%d")
    count_data = {"date": today, "count": 0}
    if os.path.exists(DAILY_COUNT_FILE):
        with open(DAILY_COUNT_FILE, "r") as f:
            count_data = json.load(f)
    if count_data.get("date") != today:
        count_data = {"date": today, "count": 0}
    return count_data["count"] < MAX_PROFILES_PER_DAY, count_data


def increment_daily_count():
    allowed, count_data = check_daily_limit()
    count_data["count"] = count_data.get("count", 0) + 1
    os.makedirs(os.path.dirname(DAILY_COUNT_FILE), exist_ok=True)
    with open(DAILY_COUNT_FILE, "w") as f:
        json.dump(count_data, f)


# ============================================================
# CORE: Scrape a LinkedIn profile via Hyperbrowser Extract
# ============================================================

def scrape_linkedin_profile(url: str) -> dict:
    """Scrape a LinkedIn profile using pure Hyperbrowser Extract API."""
    allowed, count_data = check_daily_limit()
    if not allowed:
        return {"error": f"Daily limit ({MAX_PROFILES_PER_DAY}) reached.", "url": url}

    hb = get_client()
    print(f"[*] Extracting: {url}")

    try:
        result = hb.extract.start_and_wait(StartExtractJobParams(
            urls=[url],
            prompt=EXTRACT_PROMPT,
            schema_=PROFILE_SCHEMA,
            session_options=get_session_options(),
            wait_for=5000,
        ))

        profile = result.data if result.data else {}
        profile["url"] = url
        profile["scraped_at"] = datetime.now(timezone.utc).isoformat()
        profile["source"] = "sales_navigator" if "/sales/" in url else "regular"

        # Set current_company from first experience if not set
        if not profile.get("current_company") and profile.get("experience"):
            profile["current_company"] = profile["experience"][0].get("company", "")

        increment_daily_count()
        print(f"[+] Got: {profile.get('name', '?')} — {profile.get('headline', '?')}")
        return profile

    except Exception as e:
        print(f"[!] Extract error: {e}")
        return {"error": str(e), "url": url, "scraped_at": datetime.now(timezone.utc).isoformat()}


def scrape_batch(urls: list) -> list:
    """Scrape multiple profiles with delays between them."""
    results = []
    for i, url in enumerate(urls):
        allowed, _ = check_daily_limit()
        if not allowed:
            print(f"[!] Daily limit hit at profile {i+1}/{len(urls)}")
            break

        if i > 0:
            delay = random.uniform(8, 20)
            print(f"[*] Waiting {delay:.0f}s between profiles...")
            time.sleep(delay)

        profile = scrape_linkedin_profile(url)
        results.append(profile)

    return results


# ============================================================
# NEON DB
# ============================================================

def save_to_neon(profile: dict):
    if not NEON_CONN:
        print("[!] No NEON_DATABASE_URL set, skipping DB save")
        return
    if profile.get("error"):
        print(f"[!] Skipping DB save for errored profile")
        return

    try:
        import psycopg2
        conn = psycopg2.connect(NEON_CONN)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE,
                source TEXT DEFAULT 'regular',
                name TEXT,
                headline TEXT,
                location TEXT,
                about TEXT,
                current_company TEXT,
                experience JSONB DEFAULT '[]',
                education JSONB DEFAULT '[]',
                skills JSONB DEFAULT '[]',
                certifications JSONB DEFAULT '[]',
                honors JSONB DEFAULT '[]',
                volunteering JSONB DEFAULT '[]',
                languages JSONB DEFAULT '[]',
                projects JSONB DEFAULT '[]',
                publications JSONB DEFAULT '[]',
                contact_info JSONB DEFAULT '{}',
                connections INTEGER DEFAULT 0,
                followers INTEGER DEFAULT 0,
                recommendations_received INTEGER DEFAULT 0,
                scraped_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            INSERT INTO profiles (url, source, name, headline, location, about, current_company,
                experience, education, skills, certifications, honors, volunteering, languages,
                projects, publications, contact_info, connections, followers, recommendations_received, scraped_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (url) DO UPDATE SET
                source=EXCLUDED.source, name=EXCLUDED.name, headline=EXCLUDED.headline,
                location=EXCLUDED.location, about=EXCLUDED.about, current_company=EXCLUDED.current_company,
                experience=EXCLUDED.experience, education=EXCLUDED.education, skills=EXCLUDED.skills,
                certifications=EXCLUDED.certifications, honors=EXCLUDED.honors,
                volunteering=EXCLUDED.volunteering, languages=EXCLUDED.languages,
                projects=EXCLUDED.projects, publications=EXCLUDED.publications,
                contact_info=EXCLUDED.contact_info, connections=EXCLUDED.connections,
                followers=EXCLUDED.followers, recommendations_received=EXCLUDED.recommendations_received,
                scraped_at=EXCLUDED.scraped_at, updated_at=NOW()
        """, (
            profile["url"], profile.get("source", "regular"),
            profile.get("name", ""), profile.get("headline", ""),
            profile.get("location", ""), profile.get("about", ""),
            profile.get("current_company", ""),
            json.dumps(profile.get("experience", [])),
            json.dumps(profile.get("education", [])),
            json.dumps(profile.get("skills", [])),
            json.dumps(profile.get("certifications", [])),
            json.dumps(profile.get("honors", [])),
            json.dumps(profile.get("volunteering", [])),
            json.dumps(profile.get("languages", [])),
            json.dumps(profile.get("projects", [])),
            json.dumps(profile.get("publications", [])),
            json.dumps(profile.get("contact_info", {})),
            profile.get("connections", 0) or 0,
            profile.get("followers", 0) or 0,
            profile.get("recommendations_received", 0) or 0,
            profile.get("scraped_at"),
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"[+] Saved to Neon: {profile.get('name', '?')}")
    except Exception as e:
        print(f"[!] DB error: {e}")


# ============================================================
# REQUEST PROCESSING + GIT
# ============================================================

def process_requests():
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
        print(f"[*] Processing: {fname}")

        profile = scrape_linkedin_profile(url)
        save_to_neon(profile)

        with open(result_file, "w") as f:
            json.dump(profile, f, indent=2, default=str)

        processed += 1
        time.sleep(random.uniform(5, 15))

    return processed


def git_sync():
    try:
        subprocess.run(["git", "pull", "--rebase"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
    except Exception:
        pass


def git_push_results():
    try:
        subprocess.run(["git", "add", "results/"], cwd=BRIDGE_DIR, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", f"results: scraped {datetime.now().strftime('%Y%m%d_%H%M%S')}"],
            cwd=BRIDGE_DIR, capture_output=True)
        subprocess.run(["git", "push"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
        print("[+] Pushed results")
    except Exception as e:
        print(f"[!] Push error: {e}")


def run_once():
    print(f"\n[*] Run: {datetime.now().isoformat()}")
    git_sync()
    count = process_requests()
    if count > 0:
        git_push_results()
    print(f"[*] Done: {count} profiles")
    return count


def run_watcher(interval=30):
    print(f"[*] Watcher started (every {interval}s)")
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
        print(json.dumps(profile, indent=2, default=str))
    elif len(sys.argv) > 1 and sys.argv[1] == "--batch":
        urls = sys.argv[2:]
        results = scrape_batch(urls)
        for r in results:
            save_to_neon(r)
        print(json.dumps(results, indent=2, default=str))
    else:
        run_once()
