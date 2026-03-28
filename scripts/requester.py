"""
USA-side requester: creates profile scrape requests and pushes to GitHub.
The USA Claude Code instance runs this.
"""
import os
import sys
import json
import subprocess
from datetime import datetime, timezone

BRIDGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REQUESTS_DIR = os.path.join(BRIDGE_DIR, "requests")
RESULTS_DIR = os.path.join(BRIDGE_DIR, "results")


def create_request(url: str, priority: str = "normal"):
    """Create a scrape request JSON file."""
    slug = url.rstrip("/").split("/")[-1]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{slug}.json"

    request = {
        "url": url,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "priority": priority,
        "status": "pending",
    }

    filepath = os.path.join(REQUESTS_DIR, filename)
    with open(filepath, "w") as f:
        json.dump(request, f, indent=2)

    print(f"[+] Request created: {filename}")
    return filepath


def push_requests():
    """Commit and push new requests."""
    subprocess.run(["git", "add", "requests/"], cwd=BRIDGE_DIR, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", f"request: new profiles {datetime.now().strftime('%Y%m%d_%H%M%S')}"],
        cwd=BRIDGE_DIR, capture_output=True
    )
    subprocess.run(["git", "push"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
    print("[+] Requests pushed to GitHub")


def check_results():
    """Pull and check for completed results."""
    subprocess.run(["git", "pull", "--rebase"], cwd=BRIDGE_DIR, capture_output=True, timeout=30)
    results = []
    for f in os.listdir(RESULTS_DIR):
        if f.endswith(".json"):
            with open(os.path.join(RESULTS_DIR, f)) as fh:
                results.append(json.load(fh))
    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python requester.py <linkedin_url> [<linkedin_url> ...]")
        sys.exit(1)

    for url in sys.argv[1:]:
        create_request(url)

    push_requests()
    print(f"\n[*] {len(sys.argv)-1} request(s) submitted. Pakistan side will process them.")
