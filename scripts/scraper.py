"""
Pakistan-side scraper: picks up profile requests from GitHub, scrapes via Hyperbrowser + Playwright,
writes results back to GitHub and Neon DB.
"""
import os
import sys
import json
import time
import glob
import re
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

            print("[*] Waiting for page to render...")
            page.wait_for_timeout(6000)

            # Extract using DOM selectors for accuracy
            profile = extract_profile_data(page, url)

            # Fallback: also grab raw text
            raw_text = page.evaluate("document.body.innerText")
            profile["raw_text"] = raw_text[:8000]

            # If selectors missed stuff, fill from raw text
            if not profile["name"] or not profile["experience"]:
                fallback = parse_raw_text(raw_text, url)
                for key in fallback:
                    if key != "raw_text" and not profile.get(key):
                        profile[key] = fallback[key]

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


def extract_profile_data(page, url: str) -> dict:
    """Extract profile data using Playwright DOM selectors."""
    profile = {
        "url": url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "name": "",
        "headline": "",
        "location": "",
        "about": "",
        "current_company": "",
        "experience": [],
        "education": [],
        "skills": [],
        "certifications": [],
        "honors": [],
        "connections": "",
        "followers": "",
        "profile_photo": "",
        "raw_text": "",
    }

    # Name - try multiple selectors
    for sel in ["h1", ".text-heading-xlarge", "[data-anonymize='person-name']"]:
        try:
            el = page.query_selector(sel)
            if el:
                text = el.inner_text().strip()
                if text and len(text) < 80 and text.lower() not in ("linkedin", ""):
                    profile["name"] = text
                    break
        except Exception:
            pass

    # Headline
    for sel in [".text-body-medium.break-words", "[data-anonymize='headline']"]:
        try:
            el = page.query_selector(sel)
            if el:
                profile["headline"] = el.inner_text().strip()
                break
        except Exception:
            pass

    # Location
    for sel in [".text-body-small.inline.t-black--light.break-words", "span.text-body-small"]:
        try:
            els = page.query_selector_all(sel)
            for el in els:
                text = el.inner_text().strip()
                if any(geo in text.lower() for geo in ["united states", "india", "uk", "canada", "york", "francisco", "london", "area", "city", "state", ","]):
                    profile["location"] = text
                    break
            if profile["location"]:
                break
        except Exception:
            pass

    # Profile photo
    try:
        img = page.query_selector("img.pv-top-card-profile-picture__image")
        if img:
            profile["profile_photo"] = img.get_attribute("src") or ""
    except Exception:
        pass

    # About section
    try:
        about_section = page.query_selector("#about")
        if about_section:
            about_parent = about_section.evaluate_handle("el => el.closest('section')")
            if about_parent:
                spans = about_parent.query_selector_all("span[aria-hidden='true']")
                about_texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                profile["about"] = "\n".join(about_texts)
    except Exception:
        pass

    # Experience section
    try:
        exp_section = page.query_selector("#experience")
        if exp_section:
            section = exp_section.evaluate_handle("el => el.closest('section')")
            if section:
                items = section.query_selector_all("li.artdeco-list__item")
                for item in items:
                    entry = {}
                    spans = item.query_selector_all("span[aria-hidden='true']")
                    texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                    if texts:
                        entry["title"] = texts[0] if len(texts) > 0 else ""
                        entry["company"] = texts[1] if len(texts) > 1 else ""
                        entry["duration"] = texts[2] if len(texts) > 2 else ""
                        entry["location"] = texts[3] if len(texts) > 3 else ""
                        if len(texts) > 4:
                            entry["description"] = " ".join(texts[4:])
                        profile["experience"].append(entry)
                if profile["experience"]:
                    profile["current_company"] = profile["experience"][0].get("company", "")
    except Exception:
        pass

    # Education section
    try:
        edu_section = page.query_selector("#education")
        if edu_section:
            section = edu_section.evaluate_handle("el => el.closest('section')")
            if section:
                items = section.query_selector_all("li.artdeco-list__item")
                for item in items:
                    spans = item.query_selector_all("span[aria-hidden='true']")
                    texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                    entry = {}
                    if texts:
                        entry["school"] = texts[0] if len(texts) > 0 else ""
                        entry["degree"] = texts[1] if len(texts) > 1 else ""
                        entry["dates"] = texts[2] if len(texts) > 2 else ""
                        if len(texts) > 3:
                            entry["details"] = " ".join(texts[3:])
                        profile["education"].append(entry)
    except Exception:
        pass

    # Skills section
    try:
        skills_section = page.query_selector("#skills")
        if skills_section:
            section = skills_section.evaluate_handle("el => el.closest('section')")
            if section:
                items = section.query_selector_all("li.artdeco-list__item")
                for item in items:
                    spans = item.query_selector_all("span[aria-hidden='true']")
                    for s in spans:
                        text = s.inner_text().strip()
                        if text and text.lower() not in ("show all", "endorsement", "endorsements") and "endorsement" not in text.lower() and len(text) < 80:
                            profile["skills"].append(text)
                            break
    except Exception:
        pass

    # Honors & Awards
    try:
        honors_section = page.query_selector("#honors_and_awards")
        if honors_section:
            section = honors_section.evaluate_handle("el => el.closest('section')")
            if section:
                items = section.query_selector_all("li.artdeco-list__item")
                for item in items:
                    spans = item.query_selector_all("span[aria-hidden='true']")
                    texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                    if texts:
                        profile["honors"].append({"title": texts[0], "details": " ".join(texts[1:])})
    except Exception:
        pass

    # Certifications
    try:
        cert_section = page.query_selector("#licenses_and_certifications")
        if cert_section:
            section = cert_section.evaluate_handle("el => el.closest('section')")
            if section:
                items = section.query_selector_all("li.artdeco-list__item")
                for item in items:
                    spans = item.query_selector_all("span[aria-hidden='true']")
                    texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                    if texts:
                        profile["certifications"].append({"name": texts[0], "issuer": texts[1] if len(texts) > 1 else ""})
    except Exception:
        pass

    # Connections / Followers from raw text
    try:
        connection_el = page.query_selector("span.t-bold")
        if connection_el:
            text = connection_el.inner_text().strip()
            if "500" in text or "connection" in text.lower():
                profile["connections"] = text
    except Exception:
        pass

    return profile


def parse_raw_text(text: str, url: str) -> dict:
    """Fallback parser: extract from raw innerText when selectors fail."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    profile = {
        "url": url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "name": "",
        "headline": "",
        "location": "",
        "about": "",
        "current_company": "",
        "experience": [],
        "education": [],
        "skills": [],
        "certifications": [],
        "honors": [],
        "connections": "",
        "followers": "",
    }

    # Find the name — it appears after nav items, as "Firstname Lastname" or "Firstname L."
    nav_items = {"home", "my network", "jobs", "messaging", "notifications", "me", "for business", "sales nav", "skip to main content"}
    found_nav_end = False
    for i, line in enumerate(lines):
        lower = line.lower().strip()
        if lower.isdigit():
            continue
        if lower in nav_items:
            found_nav_end = True
            continue
        if found_nav_end and len(line) < 60 and not line.startswith("http"):
            profile["name"] = line
            # Next line is headline
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if next_line.lower() not in nav_items and not next_line.startswith("Save") and not next_line.startswith("Message"):
                    profile["headline"] = next_line
            break

    # Extract sections by known headers
    section_map = {}
    current_section = None
    current_lines = []
    section_headers = {"experience", "education", "skills", "about", "honors & awards", "licenses & certifications", "certifications", "activity", "interests", "more profiles for you", "people you may know", "you might like"}

    for line in lines:
        lower = line.lower().strip()
        if lower in section_headers:
            if current_section:
                section_map[current_section] = current_lines
            current_section = lower
            current_lines = []
        elif current_section:
            # Stop at next section
            if lower in section_headers:
                section_map[current_section] = current_lines
                current_section = lower
                current_lines = []
            else:
                current_lines.append(line)

    if current_section:
        section_map[current_section] = current_lines

    # Parse experience
    if "experience" in section_map:
        exp_lines = section_map["experience"]
        i = 0
        while i < len(exp_lines):
            line = exp_lines[i]
            if line.lower() in ("show all",) or line.startswith("http"):
                i += 1
                continue
            # Check if next line has a date pattern
            has_date_nearby = False
            for j in range(i, min(i + 4, len(exp_lines))):
                if re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}', exp_lines[j]) or "Present" in exp_lines[j]:
                    has_date_nearby = True
                    break

            if has_date_nearby and len(line) < 80:
                entry = {"title": line}
                if i + 1 < len(exp_lines):
                    entry["company"] = exp_lines[i + 1].replace(" · Internship", "").replace(" · Full-time", "").strip()
                if i + 2 < len(exp_lines):
                    entry["duration"] = exp_lines[i + 2]
                if i + 3 < len(exp_lines):
                    loc = exp_lines[i + 3]
                    if any(c in loc for c in [",", "United States", "India", "Remote"]):
                        entry["location"] = loc
                        i += 4
                    else:
                        i += 3
                else:
                    i += 3
                profile["experience"].append(entry)
            else:
                i += 1

        if profile["experience"]:
            profile["current_company"] = profile["experience"][0].get("company", "")

    # Parse education
    if "education" in section_map:
        edu_lines = section_map["education"]
        i = 0
        while i < len(edu_lines):
            line = edu_lines[i]
            if line.lower() in ("show all",):
                i += 1
                continue
            if len(line) < 100 and not line.startswith("http"):
                entry = {"school": line}
                if i + 1 < len(edu_lines):
                    entry["degree"] = edu_lines[i + 1]
                if i + 2 < len(edu_lines) and re.search(r'\d{4}', edu_lines[i + 2]):
                    entry["dates"] = edu_lines[i + 2]
                    i += 3
                else:
                    i += 2
                profile["education"].append(entry)
            else:
                i += 1

    # Parse skills
    if "skills" in section_map:
        for line in section_map["skills"]:
            clean = line.strip()
            if clean and clean.lower() not in ("show all", "see all") and "endorsement" not in clean.lower() and len(clean) < 80:
                profile["skills"].append(clean)

    # Parse honors
    if "honors & awards" in section_map:
        honors_lines = section_map["honors & awards"]
        i = 0
        while i < len(honors_lines):
            line = honors_lines[i]
            if len(line) > 5 and line.lower() not in ("show all",):
                honor = {"title": line}
                if i + 1 < len(honors_lines):
                    honor["date"] = honors_lines[i + 1]
                profile["honors"].append(honor)
                i += 2
            else:
                i += 1

    # Location
    for line in lines:
        if any(geo in line for geo in ["United States", "India", "United Kingdom", "Canada", "Germany", "Australia"]):
            if len(line) < 80 and "agree" not in line.lower():
                profile["location"] = line
                break

    # Connections / Followers
    for line in lines:
        if "connections" in line.lower() and len(line) < 30:
            profile["connections"] = line
        if "followers" in line.lower() and len(line) < 30:
            profile["followers"] = line

    return profile


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
                certifications JSONB DEFAULT '[]',
                honors JSONB DEFAULT '[]',
                connections TEXT DEFAULT '',
                followers TEXT DEFAULT '',
                raw_text TEXT,
                scraped_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            INSERT INTO profiles (url, name, headline, location, about, current_company, experience, education, skills, certifications, honors, connections, followers, raw_text, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                name = EXCLUDED.name,
                headline = EXCLUDED.headline,
                location = EXCLUDED.location,
                about = EXCLUDED.about,
                current_company = EXCLUDED.current_company,
                experience = EXCLUDED.experience,
                education = EXCLUDED.education,
                skills = EXCLUDED.skills,
                certifications = EXCLUDED.certifications,
                honors = EXCLUDED.honors,
                connections = EXCLUDED.connections,
                followers = EXCLUDED.followers,
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
            json.dumps(profile.get("certifications", [])),
            json.dumps(profile.get("honors", [])),
            profile.get("connections", ""),
            profile.get("followers", ""),
            profile.get("raw_text", "")[:8000],
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
        print(json.dumps(profile, indent=2, default=str))
    else:
        run_once()
