"""
Pakistan-side Sales Navigator search: picks up search requests from GitHub,
runs Sales Nav searches via Hyperbrowser + Playwright with li_at cookie,
writes results back to GitHub and Neon DB.

Search requests go in searches/ dir, results in search_results/ dir.
"""
import os
import sys
import json
import time
import glob
import re
import subprocess
from datetime import datetime, timezone
from urllib.parse import quote

from hyperbrowser import Hyperbrowser
from hyperbrowser.models.session import CreateSessionParams
from playwright.sync_api import sync_playwright

# --- Config ---
HB_API_KEY = os.environ.get("HB_API_KEY", "hb_c954e7f6d25b0107fefcee51319b")
NEON_CONN = os.environ.get("NEON_DATABASE_URL", "")
LI_AT_COOKIE = os.environ.get("LI_AT_COOKIE", "AQEDAUnx49UA-xrTAAABnTB8w8sAAAGdVIlHy00AqxqgZC8WppDYrvdKQKoRSODpqQUnTIoDYs3e3VALS4SE0xGmz1vWLZW1_eNCdMBkU1KwWZINiWtKkLTk1b44XLugNEFyJgz7wV4C1mZZUzgEGZ9N")
BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEARCHES_DIR = os.path.join(BRIDGE_DIR, "searches")
SEARCH_RESULTS_DIR = os.path.join(BRIDGE_DIR, "search_results")


def run_sales_nav_search(query: str, filters: dict, max_results: int = 50) -> list[dict]:
    """
    Run a LinkedIn Sales Navigator search using Hyperbrowser.
    Returns list of {name, headline, location, linkedin_url, company}.
    """
    hb = Hyperbrowser(api_key=HB_API_KEY)
    session = None
    results = []

    try:
        print(f"[*] Creating Hyperbrowser session for Sales Nav search...")
        session = hb.sessions.create(
            CreateSessionParams(
                use_stealth=True,
                solve_captchas=True,
                accept_cookies=True,
            )
        )
        ws_endpoint = session.ws_endpoint

        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(ws_endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()

            # Inject li_at cookie
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

            # Build Sales Nav search URL
            search_url = build_sales_nav_url(query, filters)
            print(f"[*] Navigating to Sales Nav: {search_url[:100]}...")
            page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(8000)

            # Extract results from the page
            results = extract_search_results(page, max_results)
            print(f"[+] Found {len(results)} results from Sales Nav")

            # If we need more results, paginate
            pages_needed = (max_results // 25) + 1
            for page_num in range(2, min(pages_needed + 1, 5)):  # max 4 pages
                if len(results) >= max_results:
                    break
                try:
                    next_url = f"{search_url}&page={page_num}"
                    page.goto(next_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(6000)
                    new_results = extract_search_results(page, max_results - len(results))
                    results.extend(new_results)
                    print(f"  [Page {page_num}] Total: {len(results)} results")
                except Exception as e:
                    print(f"  [Page {page_num}] Failed: {e}")
                    break

    except Exception as e:
        print(f"[!] Sales Nav search error: {e}")
    finally:
        if session:
            try:
                hb.sessions.stop(session.id)
            except Exception:
                pass

    return results[:max_results]


def build_sales_nav_url(query: str, filters: dict) -> str:
    """Build a Sales Navigator search URL from query and filters."""
    base = "https://www.linkedin.com/sales/search/people"
    params = [f"query=(keywords:{quote(query)})"]

    # Add location filter (default: NYC metro)
    geo = filters.get("geo", "New York City Metropolitan Area")
    if geo:
        params.append(f"geoRegion={quote(geo)}")

    # Add company filters if provided
    companies = filters.get("past_companies", [])
    if companies:
        company_str = " OR ".join(companies)
        params.append(f"pastCompany={quote(company_str)}")

    current_companies = filters.get("current_companies", [])
    if current_companies:
        company_str = " OR ".join(current_companies)
        params.append(f"currentCompany={quote(company_str)}")

    # Seniority
    seniority = filters.get("seniority", [])
    if seniority:
        params.append(f"seniorityLevel={quote(','.join(seniority))}")

    # Company headcount
    headcount = filters.get("company_headcount", "")
    if headcount:
        params.append(f"companySize={quote(headcount)}")

    return f"{base}?{'&'.join(params)}"


def extract_search_results(page, max_results: int) -> list[dict]:
    """Extract people results from a Sales Nav search results page."""
    results = []

    # Try Sales Nav specific selectors
    try:
        # Sales Nav uses specific list items
        items = page.query_selector_all("li.artdeco-list__item")
        if not items:
            items = page.query_selector_all("[data-anonymize='person-name']")
        if not items:
            # Fallback: parse raw text
            raw = page.evaluate("document.body.innerText")
            return parse_search_raw_text(raw, max_results)

        for item in items[:max_results]:
            person = {}
            try:
                # Name
                name_el = item.query_selector("span[data-anonymize='person-name']") or item.query_selector("a span")
                if name_el:
                    person["name"] = name_el.inner_text().strip()

                # Headline/title
                headline_el = item.query_selector("span[data-anonymize='headline']") or item.query_selector(".artdeco-entity-lockup__subtitle")
                if headline_el:
                    person["headline"] = headline_el.inner_text().strip()

                # Company
                company_el = item.query_selector("span[data-anonymize='company-name']") or item.query_selector(".artdeco-entity-lockup__caption")
                if company_el:
                    person["company"] = company_el.inner_text().strip()

                # Location
                loc_el = item.query_selector("span[data-anonymize='location']")
                if loc_el:
                    person["location"] = loc_el.inner_text().strip()

                # LinkedIn URL
                link_el = item.query_selector("a[href*='/sales/lead/']") or item.query_selector("a[href*='/in/']")
                if link_el:
                    href = link_el.get_attribute("href") or ""
                    # Convert Sales Nav URL to regular LinkedIn
                    slug_match = re.search(r'/in/([^/?]+)', href)
                    if slug_match:
                        person["linkedin_url"] = f"https://www.linkedin.com/in/{slug_match.group(1)}/"
                    else:
                        person["linkedin_url"] = href

                if person.get("name"):
                    results.append(person)

            except Exception:
                continue

    except Exception as e:
        print(f"  [EXTRACT] Selector extraction failed: {e}")
        raw = page.evaluate("document.body.innerText")
        results = parse_search_raw_text(raw, max_results)

    return results


def parse_search_raw_text(text: str, max_results: int) -> list[dict]:
    """Fallback: parse search results from raw page text."""
    results = []
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Look for patterns like "Name\nTitle at Company\nLocation"
    i = 0
    while i < len(lines) - 1 and len(results) < max_results:
        line = lines[i]
        # Skip navigation, UI elements
        if len(line) < 3 or len(line) > 100 or any(skip in line.lower() for skip in ["search", "filter", "save", "show", "alert", "message", "connect"]):
            i += 1
            continue

        # Check if this looks like a person name (2-4 words, capitalized)
        words = line.split()
        if 2 <= len(words) <= 5 and words[0][0].isupper():
            person = {"name": line}
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                if " at " in next_line or " · " in next_line:
                    person["headline"] = next_line
                    parts = next_line.split(" at ")
                    if len(parts) == 2:
                        person["company"] = parts[1].strip()
            if i + 2 < len(lines):
                loc_line = lines[i + 2]
                if any(geo in loc_line for geo in [",", "Area", "City", "York", "Francisco"]):
                    person["location"] = loc_line

            if person.get("name") and len(person) > 1:
                results.append(person)
                i += 3
            else:
                i += 1
        else:
            i += 1

    return results


def save_search_results_to_db(search_id: str, results: list[dict]):
    """Save search results to Neon DB."""
    if not NEON_CONN:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(NEON_CONN)
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS search_results (
                id SERIAL PRIMARY KEY,
                search_id TEXT,
                name TEXT,
                headline TEXT,
                company TEXT,
                location TEXT,
                linkedin_url TEXT,
                searched_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_search_results_search_id ON search_results(search_id)")

        for r in results:
            cur.execute(
                "INSERT INTO search_results (search_id, name, headline, company, location, linkedin_url) VALUES (%s, %s, %s, %s, %s, %s)",
                (search_id, r.get("name", ""), r.get("headline", ""), r.get("company", ""), r.get("location", ""), r.get("linkedin_url", ""))
            )

        conn.commit()
        cur.close()
        conn.close()
        print(f"[+] Saved {len(results)} search results to Neon DB (search_id={search_id})")
    except Exception as e:
        print(f"[!] DB error saving search results: {e}")


def process_search_requests():
    """Process search requests from searches/ dir."""
    os.makedirs(SEARCHES_DIR, exist_ok=True)
    os.makedirs(SEARCH_RESULTS_DIR, exist_ok=True)

    request_files = sorted(glob.glob(os.path.join(SEARCHES_DIR, "*.json")))
    if not request_files:
        print("[*] No pending search requests")
        return 0

    processed = 0
    for req_file in request_files:
        fname = os.path.basename(req_file)
        result_file = os.path.join(SEARCH_RESULTS_DIR, fname)

        if os.path.exists(result_file):
            continue

        with open(req_file) as f:
            request = json.load(f)

        if request.get("status") != "pending":
            continue

        query = request.get("query", "")
        filters = request.get("filters", {})
        max_results = request.get("max_results", 50)
        search_id = request.get("search_id", fname.replace(".json", ""))

        print(f"\n{'='*60}")
        print(f"[*] Processing search: {fname}")
        print(f"[*] Query: {query}")
        print(f"[*] Filters: {json.dumps(filters)}")

        results = run_sales_nav_search(query, filters, max_results)
        save_search_results_to_db(search_id, results)

        output = {
            "search_id": search_id,
            "query": query,
            "filters": filters,
            "results": results,
            "count": len(results),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
        }

        with open(result_file, "w") as f:
            json.dump(output, f, indent=2)

        print(f"[+] Search result written: {result_file} ({len(results)} people)")
        processed += 1
        time.sleep(10)  # pace between searches

    return processed


def git_sync():
    subprocess.run(["git", "pull", "--rebase"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)


def git_push():
    subprocess.run(["git", "add", "search_results/"], cwd=BRIDGE_DIR, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", f"search_results: {datetime.now().strftime('%Y%m%d_%H%M%S')}"],
        cwd=BRIDGE_DIR, capture_output=True
    )
    subprocess.run(["git", "push"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)


def run_once():
    print(f"\n[*] Sales Nav search run at {datetime.now().isoformat()}")
    git_sync()
    count = process_search_requests()
    if count > 0:
        git_push()
    print(f"[*] Processed {count} search requests")
    return count


def run_watcher(interval=30):
    print(f"[*] Sales Nav search watcher started (polling every {interval}s)")
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[!] Error: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        run_watcher()
    else:
        run_once()
