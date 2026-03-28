"""
LinkedIn Sales Navigator Search — find profiles by criteria.
USA agent can request searches via chat or request files.

Search types:
1. Keyword search (title, company, location)
2. Sales Navigator filtered search
3. Regular LinkedIn search (fallback)

Usage:
  python search.py --keyword "Software Engineer" --company "Google" --location "San Francisco"
  python search.py --sales-nav-url "https://www.linkedin.com/sales/search/people?query=..."
  python search.py --search "AI engineers at startups in New York"
"""
import os
import sys
import json
import time
import random
import re
from datetime import datetime, timezone
from urllib.parse import quote

from hyperbrowser import Hyperbrowser
from hyperbrowser.models.session import CreateSessionParams
from playwright.sync_api import sync_playwright

# Import from scraper
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import (
    HB_API_KEY, LI_AT_COOKIE, NEON_CONN, BRIDGE_DIR,
    create_stealth_session, inject_linkedin_cookies, warm_up_session,
    human_delay, human_scroll, human_mouse_move, save_to_neon,
    scrape_linkedin_profile, scrape_batch
)

RESULTS_DIR = os.path.join(BRIDGE_DIR, "results")


def search_sales_navigator(
    keywords: str = "",
    title: str = "",
    company: str = "",
    location: str = "",
    industry: str = "",
    seniority: str = "",
    max_results: int = 25,
    sales_nav_url: str = "",
) -> list:
    """
    Search LinkedIn Sales Navigator for profiles matching criteria.
    Returns list of lead summaries with profile URLs.
    """
    hb, session = None, None

    try:
        hb, session = create_stealth_session()
        print(f"[+] Search session: {session.id}")

        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(session.ws_endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            inject_linkedin_cookies(context)
            page = context.pages[0] if context.pages else context.new_page()

            warm_up_session(page)
            human_delay(2, 4)

            # Build or use Sales Navigator search URL
            if sales_nav_url:
                search_url = sales_nav_url
            else:
                search_url = build_sales_nav_url(keywords, title, company, location)

            print(f"[*] Searching: {search_url}")
            page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
            human_delay(5, 8)
            human_scroll(page)
            human_delay(2, 4)

            # Extract search results
            leads = extract_search_results(page, max_results)

            if not leads:
                # Fallback to regular LinkedIn search
                print("[*] No Sales Nav results, trying regular LinkedIn search...")
                leads = search_regular_linkedin(page, keywords or title, max_results)

            print(f"[+] Found {len(leads)} leads")
            return leads

    except Exception as e:
        print(f"[!] Search error: {e}")
        # Fallback to regular search
        return search_regular_fallback(keywords or title, company, location, max_results)

    finally:
        if session and hb:
            try:
                hb.sessions.stop(session.id)
            except Exception:
                pass


def build_sales_nav_url(keywords: str, title: str, company: str, location: str) -> str:
    """
    Build a Sales Navigator search URL from filters.
    SN URL format: /sales/search/people?query=(filters)
    """
    # Start with Sales Nav search base
    base = "https://www.linkedin.com/sales/search/people?query="

    # Build query parts
    parts = []

    if keywords:
        parts.append(f"keywords:{quote(keywords)}")

    parts.append("spellCorrectionEnabled:true")

    # For title/company/location, we use the UI-based approach
    # since SN query syntax is complex and undocumented
    # We'll combine into keywords if specific filters aren't available via URL
    combined_keywords = []
    if title:
        combined_keywords.append(title)
    if company:
        combined_keywords.append(company)
    if location:
        combined_keywords.append(location)

    if combined_keywords and not keywords:
        parts[0:0] = [f"keywords:{quote(' '.join(combined_keywords))}"]

    query_string = ",".join(parts)
    return f"{base}({query_string})"


def extract_search_results(page, max_results: int = 25) -> list:
    """Extract lead results from Sales Navigator search page."""
    leads = []

    try:
        # Wait for results to load
        page.wait_for_timeout(3000)

        # SN results are in list items — try multiple selectors
        selectors = [
            "li.artdeco-list__item",
            "[data-anonymize='person-name']",
            ".search-results__result-item",
            "ol.search-results__result-list li",
            "div[data-x--search-result]",
        ]

        result_items = []
        for sel in selectors:
            result_items = page.query_selector_all(sel)
            if result_items:
                break

        if not result_items:
            # Fallback: parse the page text
            return _parse_search_text(page)

        for item in result_items[:max_results]:
            lead = {}
            try:
                # Name
                name_el = item.query_selector("a span, [data-anonymize='person-name']")
                if name_el:
                    lead["name"] = name_el.inner_text().strip()

                # Profile link
                link_el = item.query_selector("a[href*='/sales/lead/'], a[href*='/in/']")
                if link_el:
                    href = link_el.get_attribute("href") or ""
                    if href.startswith("/"):
                        href = "https://www.linkedin.com" + href
                    lead["profile_url"] = href
                    # Also extract the regular LinkedIn URL
                    lead["linkedin_url"] = _sales_nav_to_regular_url(href)

                # Title/Headline
                title_el = item.query_selector(".result-lockup__highlight-keyword, .artdeco-entity-lockup__subtitle")
                if title_el:
                    lead["headline"] = title_el.inner_text().strip()

                # Company
                company_el = item.query_selector(".result-lockup__position-company, .artdeco-entity-lockup__caption")
                if company_el:
                    lead["company"] = company_el.inner_text().strip()

                # Location
                location_el = item.query_selector(".result-lockup__misc-item")
                if location_el:
                    lead["location"] = location_el.inner_text().strip()

                if lead.get("name"):
                    leads.append(lead)

            except Exception:
                pass

    except Exception as e:
        print(f"[!] Extract error: {e}")

    return leads


def _parse_search_text(page) -> list:
    """Fallback: parse search results from raw page text."""
    leads = []
    raw_text = page.evaluate("document.body.innerText")
    lines = [l.strip() for l in raw_text.split("\n") if l.strip()]

    # Look for patterns like "Name\nTitle at Company\nLocation"
    i = 0
    while i < len(lines) - 1:
        line = lines[i]
        # Skip nav/UI text
        if line.lower() in ("message", "save", "connect", "show all", "follow", "more") or len(line) > 120:
            i += 1
            continue

        # Check if this looks like a person name (short, no special chars)
        if len(line) < 50 and not any(c in line for c in ["@", "http", "|", "·"]):
            next_line = lines[i + 1] if i + 1 < len(lines) else ""
            # Next line should be a headline/title
            if next_line and (" at " in next_line or " @ " in next_line or len(next_line) < 100):
                lead = {"name": line, "headline": next_line}
                # Check for location on next line
                if i + 2 < len(lines):
                    loc = lines[i + 2]
                    if any(g in loc for g in [",", "United States", "India", "UK", "Canada", "Area"]):
                        lead["location"] = loc
                leads.append(lead)
                i += 3
                continue
        i += 1

    return leads[:25]


def _sales_nav_to_regular_url(sn_url: str) -> str:
    """Convert Sales Navigator URL to regular LinkedIn URL."""
    # SN format: /sales/lead/ACwAAB... or /sales/people/ACwAAB...
    # We can't directly convert without looking at the page
    # Return the SN URL for now — the scraper handles both
    return sn_url


def search_regular_linkedin(page, query: str, max_results: int = 25) -> list:
    """Search using regular LinkedIn search as fallback."""
    leads = []
    search_url = f"https://www.linkedin.com/search/results/people/?keywords={quote(query)}"

    try:
        page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
        human_delay(4, 6)
        human_scroll(page)
        human_delay(2, 3)

        # Extract results
        items = page.query_selector_all("li.reusable-search__result-container")
        for item in items[:max_results]:
            lead = {}
            try:
                name_el = item.query_selector("span.entity-result__title-text a span span")
                if name_el:
                    lead["name"] = name_el.inner_text().strip()

                link_el = item.query_selector("a.app-aware-link[href*='/in/']")
                if link_el:
                    href = link_el.get_attribute("href") or ""
                    lead["linkedin_url"] = href.split("?")[0]

                subtitle = item.query_selector(".entity-result__primary-subtitle")
                if subtitle:
                    lead["headline"] = subtitle.inner_text().strip()

                secondary = item.query_selector(".entity-result__secondary-subtitle")
                if secondary:
                    lead["location"] = secondary.inner_text().strip()

                if lead.get("name"):
                    leads.append(lead)
            except Exception:
                pass

    except Exception as e:
        print(f"[!] Regular search error: {e}")

    return leads


def search_regular_fallback(query: str, company: str, location: str, max_results: int) -> list:
    """Complete fallback: new session, regular LinkedIn search."""
    hb, session = None, None
    try:
        hb, session = create_stealth_session()
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(session.ws_endpoint)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            inject_linkedin_cookies(context)
            page = context.pages[0] if context.pages else context.new_page()
            warm_up_session(page)
            human_delay(2, 4)

            search_terms = " ".join(filter(None, [query, company, location]))
            return search_regular_linkedin(page, search_terms, max_results)
    except Exception as e:
        print(f"[!] Fallback search error: {e}")
        return []
    finally:
        if session and hb:
            try:
                hb.sessions.stop(session.id)
            except Exception:
                pass


def search_and_scrape(
    keywords: str = "",
    title: str = "",
    company: str = "",
    location: str = "",
    max_results: int = 10,
    scrape_profiles: bool = True,
    sales_nav_url: str = "",
) -> dict:
    """
    Full pipeline: search for leads, then optionally scrape their full profiles.
    Returns search results + optionally full profile data.
    """
    print(f"\n{'='*60}")
    print(f"[*] Search & Scrape")
    print(f"    Keywords: {keywords}")
    print(f"    Title: {title}")
    print(f"    Company: {company}")
    print(f"    Location: {location}")
    print(f"    Max results: {max_results}")
    print(f"    Scrape profiles: {scrape_profiles}")

    # Step 1: Search
    leads = search_sales_navigator(
        keywords=keywords, title=title, company=company,
        location=location, max_results=max_results,
        sales_nav_url=sales_nav_url,
    )

    result = {
        "search_query": {
            "keywords": keywords, "title": title,
            "company": company, "location": location,
        },
        "searched_at": datetime.now(timezone.utc).isoformat(),
        "leads_found": len(leads),
        "leads": leads,
        "profiles": [],
    }

    # Step 2: Scrape full profiles if requested
    if scrape_profiles and leads:
        urls = []
        for lead in leads:
            url = lead.get("linkedin_url") or lead.get("profile_url")
            if url:
                urls.append(url)

        if urls:
            print(f"\n[*] Scraping {len(urls)} profiles...")
            profiles = scrape_batch(urls[:max_results])
            for p in profiles:
                save_to_neon(p)
            result["profiles"] = profiles

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = (keywords or title or company or "search").replace(" ", "_")[:30]
    result_file = os.path.join(RESULTS_DIR, f"search_{timestamp}_{slug}.json")
    with open(result_file, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"[+] Results saved: {result_file}")

    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LinkedIn Sales Navigator Search")
    parser.add_argument("--keyword", "-k", default="", help="Search keywords")
    parser.add_argument("--title", "-t", default="", help="Job title filter")
    parser.add_argument("--company", "-c", default="", help="Company filter")
    parser.add_argument("--location", "-l", default="", help="Location filter")
    parser.add_argument("--max", "-m", type=int, default=10, help="Max results")
    parser.add_argument("--scrape", action="store_true", help="Also scrape full profiles")
    parser.add_argument("--sales-nav-url", default="", help="Direct Sales Nav search URL")
    parser.add_argument("--search", "-s", default="", help="Natural language search query")

    args = parser.parse_args()

    # Natural language search — just use as keywords
    keywords = args.keyword or args.search

    result = search_and_scrape(
        keywords=keywords,
        title=args.title,
        company=args.company,
        location=args.location,
        max_results=args.max,
        scrape_profiles=args.scrape,
        sales_nav_url=args.sales_nav_url,
    )

    # Print summary
    print(f"\n{'='*60}")
    print(f"[*] Found {result['leads_found']} leads")
    for i, lead in enumerate(result["leads"], 1):
        print(f"  {i}. {lead.get('name', '?')} — {lead.get('headline', '?')}")
    if result["profiles"]:
        print(f"\n[*] Scraped {len(result['profiles'])} full profiles")
