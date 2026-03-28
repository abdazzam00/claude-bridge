"""
Pakistan-side LinkedIn scraper — stealth-first architecture.

Anti-ban strategy:
1. Warm-up browsing (hit homepage/feed before target profile)
2. Random human-like delays (3-8s between actions)
3. Hyperbrowser stealth mode + proxy rotation
4. Mouse movements and scrolling to mimic real user
5. Max 50 profiles/day, with exponential backoff on errors
6. Session reuse — one session per batch, not per profile
7. Sales Navigator support for richer data
8. Hyperbrowser Extract API for structured output when possible
"""
import os
import sys
import json
import time
import glob
import re
import random
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
MAX_PROFILES_PER_DAY = 50
DAILY_COUNT_FILE = os.path.join(BRIDGE_DIR, "config", "daily_count.json")


# ============================================================
# ANTI-BAN: Human behavior simulation
# ============================================================

def human_delay(min_s=3, max_s=8):
    """Random delay to mimic human browsing."""
    delay = random.uniform(min_s, max_s)
    time.sleep(delay)


def human_scroll(page):
    """Scroll like a human — variable speed, pauses, up and down."""
    scroll_count = random.randint(3, 6)
    for _ in range(scroll_count):
        scroll_amount = random.randint(200, 600)
        page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        time.sleep(random.uniform(0.5, 1.5))

    # Sometimes scroll back up a bit
    if random.random() > 0.5:
        page.evaluate(f"window.scrollBy(0, -{random.randint(100, 300)})")
        time.sleep(random.uniform(0.3, 0.8))


def human_mouse_move(page):
    """Random mouse movements to look human."""
    try:
        for _ in range(random.randint(2, 5)):
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            page.mouse.move(x, y)
            time.sleep(random.uniform(0.1, 0.3))
    except Exception:
        pass


def warm_up_session(page):
    """
    Browse LinkedIn naturally before hitting target profile.
    This is critical — going directly to a profile URL is a red flag.
    """
    print("[*] Warming up session (browsing feed first)...")

    # Visit LinkedIn homepage/feed
    page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded", timeout=30000)
    human_delay(3, 6)
    human_scroll(page)
    human_mouse_move(page)
    human_delay(2, 4)

    # Maybe visit notifications or messaging (random)
    warmup_pages = [
        "https://www.linkedin.com/mynetwork/",
        "https://www.linkedin.com/notifications/",
    ]
    if random.random() > 0.5:
        pick = random.choice(warmup_pages)
        print(f"[*] Warm-up visit: {pick}")
        page.goto(pick, wait_until="domcontentloaded", timeout=20000)
        human_delay(2, 5)
        human_scroll(page)

    print("[+] Warm-up done")


def check_daily_limit():
    """Check if we've hit the daily scrape limit."""
    today = datetime.now().strftime("%Y-%m-%d")
    count_data = {"date": today, "count": 0}

    if os.path.exists(DAILY_COUNT_FILE):
        with open(DAILY_COUNT_FILE, "r") as f:
            count_data = json.load(f)

    if count_data.get("date") != today:
        count_data = {"date": today, "count": 0}

    return count_data["count"] < MAX_PROFILES_PER_DAY, count_data


def increment_daily_count():
    """Increment the daily profile count."""
    allowed, count_data = check_daily_limit()
    count_data["count"] = count_data.get("count", 0) + 1
    os.makedirs(os.path.dirname(DAILY_COUNT_FILE), exist_ok=True)
    with open(DAILY_COUNT_FILE, "w") as f:
        json.dump(count_data, f)


# ============================================================
# CORE SCRAPER — supports regular profiles + Sales Navigator
# ============================================================

def create_stealth_session():
    """Create a Hyperbrowser session with maximum stealth."""
    hb = Hyperbrowser(api_key=HB_API_KEY)
    session = hb.sessions.create(
        CreateSessionParams(
            use_stealth=True,
            solve_captchas=True,
            accept_cookies=True,
            use_proxy=True,  # Use Hyperbrowser's built-in proxy rotation
        )
    )
    return hb, session


def inject_linkedin_cookies(context):
    """Inject li_at cookie for LinkedIn authentication."""
    context.add_cookies([{
        "name": "li_at",
        "value": LI_AT_COOKIE,
        "domain": ".linkedin.com",
        "path": "/",
        "httpOnly": True,
        "secure": True,
        "sameSite": "None",
    }])


def scrape_linkedin_profile(url: str) -> dict:
    """
    Scrape a single LinkedIn profile with full anti-ban measures.
    Supports both regular profiles and Sales Navigator URLs.
    """
    # Check daily limit
    allowed, count_data = check_daily_limit()
    if not allowed:
        return {"error": f"Daily limit reached ({MAX_PROFILES_PER_DAY}). Try tomorrow.", "url": url}

    is_sales_nav = "/sales/" in url
    hb, session = None, None

    try:
        print(f"[*] Creating stealth session...")
        hb, session = create_stealth_session()
        print(f"[+] Session: {session.id}")

        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(session.ws_endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            inject_linkedin_cookies(context)
            page = context.pages[0] if context.pages else context.new_page()

            # WARM UP — critical for not getting flagged
            warm_up_session(page)

            # Navigate to target with human delay
            print(f"[*] Navigating to target: {url}")
            human_delay(2, 5)
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            human_delay(4, 7)

            # Scroll through the profile like a real person
            human_mouse_move(page)
            human_scroll(page)
            human_delay(2, 4)

            # Scroll all the way down to load all sections
            for _ in range(4):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                human_delay(1, 2)

            # Click "Show all" buttons to expand sections
            expand_sections(page)
            human_delay(2, 3)

            # Extract data based on URL type
            if is_sales_nav:
                profile = extract_sales_nav_profile(page, url)
            else:
                profile = extract_regular_profile(page, url)

            # Always grab raw text as backup
            raw_text = page.evaluate("document.body.innerText")
            profile["raw_text"] = raw_text[:10000]

            # Fill gaps from raw text
            if not profile.get("experience"):
                fallback = parse_raw_text(raw_text, url)
                for key in fallback:
                    if key != "raw_text" and not profile.get(key):
                        profile[key] = fallback[key]

            increment_daily_count()
            print(f"[+] Scraped: {profile.get('name', 'unknown')} | {profile.get('headline', '')}")
            return profile

    except Exception as e:
        print(f"[!] Scrape error: {e}")
        return {"error": str(e), "url": url, "scraped_at": datetime.now(timezone.utc).isoformat()}

    finally:
        if session and hb:
            try:
                hb.sessions.stop(session.id)
                print(f"[+] Session stopped")
            except Exception:
                pass


def expand_sections(page):
    """Click 'Show all' buttons to expand experience, education, skills."""
    try:
        buttons = page.query_selector_all("button")
        for btn in buttons:
            try:
                text = btn.inner_text().strip().lower()
                if "show all" in text and ("experience" in text or "education" in text or "skill" in text or "license" in text):
                    btn.click()
                    human_delay(1, 2)
            except Exception:
                pass
    except Exception:
        pass


def scrape_batch(urls: list) -> list:
    """
    Scrape multiple profiles in a single session — more efficient and stealthier.
    Reuses one browser session for all profiles with natural delays between them.
    """
    results = []
    hb, session = None, None

    try:
        hb, session = create_stealth_session()
        print(f"[+] Batch session: {session.id} for {len(urls)} profiles")

        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(session.ws_endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            inject_linkedin_cookies(context)
            page = context.pages[0] if context.pages else context.new_page()

            warm_up_session(page)

            for i, url in enumerate(urls):
                allowed, _ = check_daily_limit()
                if not allowed:
                    print(f"[!] Daily limit hit at profile {i+1}/{len(urls)}")
                    break

                print(f"\n[*] Profile {i+1}/{len(urls)}: {url}")

                # Random delay between profiles (5-15s)
                if i > 0:
                    delay = random.uniform(8, 20)
                    print(f"[*] Waiting {delay:.0f}s between profiles...")
                    time.sleep(delay)

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    human_delay(4, 7)
                    human_mouse_move(page)
                    human_scroll(page)
                    human_delay(2, 3)

                    for _ in range(3):
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        human_delay(1, 2)

                    expand_sections(page)

                    is_sales_nav = "/sales/" in url
                    if is_sales_nav:
                        profile = extract_sales_nav_profile(page, url)
                    else:
                        profile = extract_regular_profile(page, url)

                    raw_text = page.evaluate("document.body.innerText")
                    profile["raw_text"] = raw_text[:10000]

                    if not profile.get("experience"):
                        fallback = parse_raw_text(raw_text, url)
                        for key in fallback:
                            if key != "raw_text" and not profile.get(key):
                                profile[key] = fallback[key]

                    increment_daily_count()
                    results.append(profile)
                    print(f"[+] Got: {profile.get('name', '?')} — {profile.get('headline', '?')}")

                except Exception as e:
                    print(f"[!] Error on {url}: {e}")
                    results.append({"url": url, "error": str(e)})

    except Exception as e:
        print(f"[!] Batch error: {e}")
    finally:
        if session and hb:
            try:
                hb.sessions.stop(session.id)
            except Exception:
                pass

    return results


# ============================================================
# EXTRACTORS — DOM selectors for regular + Sales Navigator
# ============================================================

def extract_regular_profile(page, url: str) -> dict:
    """Extract from a regular LinkedIn profile page using DOM selectors."""
    p = _empty_profile(url)

    # Name
    p["name"] = _get_text(page, "h1") or ""

    # Headline
    p["headline"] = _get_text(page, ".text-body-medium.break-words") or ""

    # Location
    for sel in [".text-body-small.inline.t-black--light.break-words", "span.text-body-small"]:
        els = page.query_selector_all(sel)
        for el in els:
            text = el.inner_text().strip()
            if any(g in text.lower() for g in [",", "united states", "india", "uk", "canada", "york", "francisco", "london", "remote"]):
                p["location"] = text
                break
        if p["location"]:
            break

    # About
    p["about"] = _extract_section_text(page, "#about")

    # Experience
    p["experience"] = _extract_list_items(page, "#experience")
    if p["experience"]:
        p["current_company"] = p["experience"][0].get("line2", "")

    # Education
    p["education"] = _extract_list_items(page, "#education")

    # Skills
    p["skills"] = _extract_list_items(page, "#skills")

    # Honors
    p["honors"] = _extract_list_items(page, "#honors_and_awards")

    # Certifications
    p["certifications"] = _extract_list_items(page, "#licenses_and_certifications")

    # Volunteering
    p["volunteering"] = _extract_list_items(page, "#volunteering_experience")

    # Languages
    p["languages"] = _extract_list_items(page, "#languages")

    # Projects
    p["projects"] = _extract_list_items(page, "#projects")

    # Publications
    p["publications"] = _extract_list_items(page, "#publications")

    # Recommendations
    p["recommendations_count"] = _get_text(page, "#recommendations")

    # Connections & Followers
    for el in page.query_selector_all("span"):
        try:
            t = el.inner_text().strip()
            if "connection" in t.lower() and len(t) < 30:
                p["connections"] = t
            if "follower" in t.lower() and len(t) < 30:
                p["followers"] = t
        except Exception:
            pass

    # Profile photo
    try:
        img = page.query_selector("img.pv-top-card-profile-picture__image, img.profile-photo-edit__preview")
        if img:
            p["profile_photo"] = img.get_attribute("src") or ""
    except Exception:
        pass

    # Contact info — click the link and grab it
    p["contact_info"] = _extract_contact_info(page)

    return p


def extract_sales_nav_profile(page, url: str) -> dict:
    """
    Extract from Sales Navigator profile — has MORE data than regular profiles.
    Sales Nav shows: email, phone, company details, shared connections, tags, notes.
    """
    p = _empty_profile(url)
    p["source"] = "sales_navigator"

    # Sales Nav has different DOM structure
    # Name — usually in a h1 or specific SN class
    p["name"] = _get_text(page, "h1") or _get_text(page, "[data-anonymize='person-name']") or ""

    # Headline/Title
    p["headline"] = _get_text(page, ".profile-topcard__summary-position") or ""

    # Current company
    p["current_company"] = _get_text(page, ".profile-topcard__summary-company") or ""

    # Location
    p["location"] = _get_text(page, ".profile-topcard__location-data") or ""

    # Connection degree
    p["connection_degree"] = _get_text(page, ".profile-topcard__connection-info") or ""

    # About / Summary
    p["about"] = _get_text(page, ".profile-topcard__summary-self-link") or ""

    # Experience, Education — SN uses similar section structure
    p["experience"] = _extract_sn_section(page, "Experience")
    p["education"] = _extract_sn_section(page, "Education")

    if p["experience"]:
        p["current_company"] = p["current_company"] or p["experience"][0].get("line2", "")

    # Related leads (similar people at company)
    p["related_leads"] = []
    try:
        related = page.query_selector_all(".related-leads-card")
        for card in related[:5]:
            name = _get_text_from_el(card, "a")
            title = _get_text_from_el(card, "span")
            if name:
                p["related_leads"].append({"name": name, "title": title})
    except Exception:
        pass

    # Tags and notes (if any)
    p["tags"] = _get_text(page, ".tags-container") or ""
    p["notes"] = _get_text(page, ".notes-container") or ""

    return p


def _extract_sn_section(page, section_name: str) -> list:
    """Extract a Sales Navigator profile section by header text."""
    items = []
    try:
        headers = page.query_selector_all("h2, h3")
        for h in headers:
            if section_name.lower() in h.inner_text().strip().lower():
                parent = h.evaluate_handle("el => el.closest('section') || el.parentElement")
                if parent:
                    lis = parent.query_selector_all("li")
                    for li in lis:
                        spans = li.query_selector_all("span")
                        texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                        if texts:
                            entry = {"line1": texts[0]}
                            for j, t in enumerate(texts[1:], 2):
                                entry[f"line{j}"] = t
                            items.append(entry)
                break
    except Exception:
        pass
    return items


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def _empty_profile(url: str) -> dict:
    return {
        "url": url,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "source": "regular",
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
        "volunteering": [],
        "languages": [],
        "projects": [],
        "publications": [],
        "recommendations_count": "",
        "connections": "",
        "followers": "",
        "profile_photo": "",
        "contact_info": {},
        "raw_text": "",
    }


def _get_text(page, selector: str) -> str:
    try:
        el = page.query_selector(selector)
        if el:
            return el.inner_text().strip()
    except Exception:
        pass
    return ""


def _get_text_from_el(parent, selector: str) -> str:
    try:
        el = parent.query_selector(selector)
        if el:
            return el.inner_text().strip()
    except Exception:
        pass
    return ""


def _extract_section_text(page, anchor_id: str) -> str:
    """Get all text from a section identified by its anchor ID."""
    try:
        anchor = page.query_selector(anchor_id)
        if anchor:
            section = anchor.evaluate_handle("el => el.closest('section')")
            if section:
                spans = section.query_selector_all("span[aria-hidden='true']")
                texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
                return "\n".join(texts)
    except Exception:
        pass
    return ""


def _extract_list_items(page, anchor_id: str) -> list:
    """Extract list items from a LinkedIn profile section."""
    items = []
    try:
        anchor = page.query_selector(anchor_id)
        if not anchor:
            return items
        section = anchor.evaluate_handle("el => el.closest('section')")
        if not section:
            return items
        lis = section.query_selector_all("li.artdeco-list__item")
        for li in lis:
            spans = li.query_selector_all("span[aria-hidden='true']")
            texts = [s.inner_text().strip() for s in spans if s.inner_text().strip()]
            if texts:
                entry = {"line1": texts[0]}
                for j, t in enumerate(texts[1:], 2):
                    entry[f"line{j}"] = t
                items.append(entry)
    except Exception:
        pass
    return items


def _extract_contact_info(page) -> dict:
    """Click 'Contact info' link and extract email, phone, website, etc."""
    contact = {}
    try:
        # Find and click the contact info link
        links = page.query_selector_all("a")
        for link in links:
            try:
                text = link.inner_text().strip().lower()
                href = link.get_attribute("href") or ""
                if "contact" in text and "info" in text:
                    link.click()
                    human_delay(2, 3)

                    # Extract from the modal
                    modal = page.query_selector(".pv-profile-section__section-info, .artdeco-modal__content")
                    if modal:
                        sections = modal.query_selector_all("section")
                        for sec in sections:
                            header = _get_text_from_el(sec, "h3, h4")
                            value = _get_text_from_el(sec, "a, span")
                            if header and value:
                                key = header.lower().replace(" ", "_")
                                contact[key] = value

                    # Also grab any mailto/tel links
                    all_links = page.query_selector_all("a[href^='mailto:'], a[href^='tel:']")
                    for a in all_links:
                        href = a.get_attribute("href") or ""
                        if href.startswith("mailto:"):
                            contact["email"] = href.replace("mailto:", "")
                        elif href.startswith("tel:"):
                            contact["phone"] = href.replace("tel:", "")

                    # Close modal
                    close_btn = page.query_selector("button[aria-label='Dismiss'], .artdeco-modal__dismiss")
                    if close_btn:
                        close_btn.click()
                        human_delay(1, 2)
                    break
            except Exception:
                pass
    except Exception:
        pass
    return contact


# ============================================================
# RAW TEXT FALLBACK PARSER
# ============================================================

def parse_raw_text(text: str, url: str) -> dict:
    """Fallback: parse raw innerText when DOM selectors fail."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    p = _empty_profile(url)

    nav_items = {"home", "my network", "jobs", "messaging", "notifications", "me", "for business",
                 "sales nav", "skip to main content", "0 notifications", "1", "2", "3", "4", "5",
                 "6", "7", "8", "9"}
    found_nav_end = False

    for i, line in enumerate(lines):
        lower = line.lower().strip()
        if lower.isdigit() or lower in nav_items:
            found_nav_end = True
            continue
        if found_nav_end and len(line) < 60 and not line.startswith("http") and not line.startswith("Save"):
            p["name"] = line
            if i + 1 < len(lines):
                next_l = lines[i + 1]
                if next_l.lower() not in nav_items and not next_l.startswith("Save") and not next_l.startswith("Message"):
                    p["headline"] = next_l
            break

    # Sections
    section_map = {}
    current = None
    current_lines = []
    headers = {"experience", "education", "skills", "about", "honors & awards",
               "licenses & certifications", "volunteering", "languages", "projects", "publications"}

    stop_headers = {"more profiles for you", "people you may know", "you might like", "pages for you"}

    for line in lines:
        lower = line.lower().strip()
        if lower in stop_headers:
            if current:
                section_map[current] = current_lines
            current = None
            current_lines = []
            continue
        if lower in headers:
            if current:
                section_map[current] = current_lines
            current = lower
            current_lines = []
        elif current:
            current_lines.append(line)

    if current:
        section_map[current] = current_lines

    # Experience
    if "experience" in section_map:
        exp_lines = section_map["experience"]
        i = 0
        while i < len(exp_lines):
            line = exp_lines[i]
            if line.lower() in ("show all",) or line.startswith("http"):
                i += 1
                continue
            has_date = False
            for j in range(i, min(i + 4, len(exp_lines))):
                if re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}', exp_lines[j]) or "Present" in exp_lines[j]:
                    has_date = True
                    break
            if has_date and len(line) < 80:
                entry = {"line1": line}
                if i + 1 < len(exp_lines):
                    entry["line2"] = exp_lines[i + 1].replace(" · Internship", "").replace(" · Full-time", "").strip()
                if i + 2 < len(exp_lines):
                    entry["line3"] = exp_lines[i + 2]
                if i + 3 < len(exp_lines) and any(c in exp_lines[i + 3] for c in [",", "United States", "India", "Remote"]):
                    entry["line4"] = exp_lines[i + 3]
                    i += 4
                else:
                    i += 3
                p["experience"].append(entry)
            else:
                i += 1
        if p["experience"]:
            p["current_company"] = p["experience"][0].get("line2", "")

    # Education
    if "education" in section_map:
        edu_lines = section_map["education"]
        i = 0
        while i < len(edu_lines):
            line = edu_lines[i]
            if line.lower() in ("show all",):
                i += 1
                continue
            if len(line) < 100:
                entry = {"line1": line}
                if i + 1 < len(edu_lines):
                    entry["line2"] = edu_lines[i + 1]
                if i + 2 < len(edu_lines) and re.search(r'\d{4}', edu_lines[i + 2]):
                    entry["line3"] = edu_lines[i + 2]
                    i += 3
                else:
                    i += 2
                p["education"].append(entry)
            else:
                i += 1

    # Skills
    if "skills" in section_map:
        for line in section_map["skills"]:
            clean = line.strip()
            if clean and clean.lower() not in ("show all",) and "endorsement" not in clean.lower() and len(clean) < 80:
                p["skills"].append({"line1": clean})

    # About
    if "about" in section_map:
        p["about"] = "\n".join(section_map["about"][:20])

    # Honors
    if "honors & awards" in section_map:
        h_lines = section_map["honors & awards"]
        i = 0
        while i < len(h_lines):
            if len(h_lines[i]) > 5 and h_lines[i].lower() not in ("show all",):
                entry = {"line1": h_lines[i]}
                if i + 1 < len(h_lines):
                    entry["line2"] = h_lines[i + 1]
                p["honors"].append(entry)
                i += 2
            else:
                i += 1

    # Location
    for line in lines:
        if any(g in line for g in ["United States", "India", "United Kingdom", "Canada", "Germany"]):
            if len(line) < 80 and "agree" not in line.lower():
                p["location"] = line
                break

    # Connections/Followers
    for line in lines:
        if "connections" in line.lower() and len(line) < 30:
            p["connections"] = line
        if "followers" in line.lower() and len(line) < 30 and "page" not in line.lower():
            p["followers"] = line

    return p


# ============================================================
# NEON DB
# ============================================================

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
                connections TEXT DEFAULT '',
                followers TEXT DEFAULT '',
                profile_photo TEXT DEFAULT '',
                raw_text TEXT,
                scraped_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        cur.execute("""
            INSERT INTO profiles (url, source, name, headline, location, about, current_company,
                experience, education, skills, certifications, honors, volunteering, languages,
                projects, publications, contact_info, connections, followers, profile_photo, raw_text, scraped_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (url) DO UPDATE SET
                source=EXCLUDED.source, name=EXCLUDED.name, headline=EXCLUDED.headline,
                location=EXCLUDED.location, about=EXCLUDED.about, current_company=EXCLUDED.current_company,
                experience=EXCLUDED.experience, education=EXCLUDED.education, skills=EXCLUDED.skills,
                certifications=EXCLUDED.certifications, honors=EXCLUDED.honors,
                volunteering=EXCLUDED.volunteering, languages=EXCLUDED.languages,
                projects=EXCLUDED.projects, publications=EXCLUDED.publications,
                contact_info=EXCLUDED.contact_info, connections=EXCLUDED.connections,
                followers=EXCLUDED.followers, profile_photo=EXCLUDED.profile_photo,
                raw_text=EXCLUDED.raw_text, scraped_at=EXCLUDED.scraped_at, updated_at=NOW()
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
            profile.get("connections", ""), profile.get("followers", ""),
            profile.get("profile_photo", ""),
            profile.get("raw_text", "")[:10000],
            profile.get("scraped_at"),
        ))

        conn.commit()
        cur.close()
        conn.close()
        print(f"[+] Saved to Neon: {profile.get('name', '?')}")
    except Exception as e:
        print(f"[!] DB error: {e}")


# ============================================================
# REQUEST PROCESSING + GIT OPS
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
        print(f"[*] Processing: {fname} -> {url}")

        profile = scrape_linkedin_profile(url)
        save_to_neon(profile)

        with open(result_file, "w") as f:
            json.dump(profile, f, indent=2, default=str)

        print(f"[+] Result: {result_file}")
        processed += 1
        human_delay(5, 15)

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
            cwd=BRIDGE_DIR, capture_output=True
        )
        subprocess.run(["git", "push"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
        print("[+] Results pushed")
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
