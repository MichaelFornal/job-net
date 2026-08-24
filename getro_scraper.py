#!/usr/bin/env python3
"""
getro_scraper.py
Pull jobs from Getro-powered boards and keep the ones that fit your background.

How it works
------------
Every Getro board is a Next.js app that embeds its full jobs payload in a
<script id="__NEXT_DATA__"> tag as JSON. We fetch the page, parse that JSON,
recursively locate the job records, filter by your keyword lists, de-dupe
across boards, and write a CSV. Same shape works on getro.com subdomains and
on custom domains (e.g. jobs.everywhere.vc).

Usage
-----
    python getro_scraper.py                      # scrape BOARDS below -> getro_results.csv
    python getro_scraper.py --boards boards.csv  # scrape a url-per-line file instead
    python getro_scraper.py --validate URL       # is URL a live Getro board? how many jobs?
    python getro_scraper.py --no-filter          # keep every job, skip keyword filtering

Notes
-----
- These are public listings; this is for your own job search. Be a good citizen:
  the default 1.5s delay and sequential requests are deliberate. Do not parallelize
  into a hammer. Check each site's robots.txt / terms if you scale this up.
- Board JSON structure can shift over time. If a board returns 0 jobs but you know
  it has some, run --validate on it and inspect the printed keys, then adjust
  looks_like_job() if needed.

Requires: requests   (pip install requests)
"""

import argparse
import csv
import json
import re
import sys
import time
from html import unescape
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run:  pip install requests")

# ----------------------------------------------------------------- CONFIG
# Seed boards. The aggregators give the widest coverage; add standalone
# fund boards as you confirm them with --validate or discovery.
BOARDS = [
    "https://community.getro.com/jobs",     # Getro Community - broadest
    "https://jobsinvc.getro.com/jobs",      # roles AT venture funds
    "https://oifvc.getro.com/jobs",         # OIF Ventures
    "https://gsv.getro.com/jobs",           # GSV Ventures
    "https://jobs.everywhere.vc/jobs",      # Everywhere Ventures (custom domain)
]

# Kept if ANY include term appears in title or company, and NO exclude term does.
INCLUDE = [
    # core AI roles
    "ai engineer", "applied ai", "machine learning engineer", "ml engineer",
    "applied scientist", "ai product", "product manager", "product owner",
    "technical product", "solutions architect", "forward deployed",
    "founding engineer", "founding ai",
    # ai / llm tech
    "llm", "large language model", "generative ai", "genai", "agent", "agentic",
    "multi-agent", "rag", "retrieval augmented", "prompt", "mcp",
    "model context protocol", "claude", "gpt", "langchain", "langgraph",
    "embeddings", "vector", "fine-tune", "fine tuning", "evals",
    # domain edge
    "fintech", "capital markets", "blockchain", "tokenization", "digital assets",
    "crypto", "web3", "trading", "quant", "fund finance", "nav lending",
    "payments", "data scientist", "data science",
    # founder track
    "co-founder", "cofounder", "founding", "cto",
]

EXCLUDE = [
    "intern", "internship", "unpaid", "volunteer", "apprentice",
    "registered nurse", "clinical", "phlebotomist", "driver", "warehouse",
    "barista", "cashier", "custodian", "welder",
    # uncomment if you want individual-contributor roles only:
    # "vp ", "vice president", "head of", "director",
]

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
DELAY_SECONDS = 1.5
TIMEOUT = 25
RETRIES = 2
OUTPUT = "getro_results.csv"

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)


# ----------------------------------------------------------------- FETCH
def fetch(url):
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html"}
    last = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                if urlparse(r.url).netloc != urlparse(url).netloc:
                    print(f"    ! {url} redirected off-board to {r.url}")
                return r.text
            last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = str(e)
        time.sleep(DELAY_SECONDS * (attempt + 1))
    print(f"    ! fetch failed for {url}: {last}")
    return None


def extract_next_data(html):
    m = NEXT_DATA_RE.search(html or "")
    if not m:
        return None
    try:
        return json.loads(unescape(m.group(1)))
    except json.JSONDecodeError:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            return None


# --------------------------------------------------- FIND JOBS IN THE JSON
def looks_like_job(d):
    """A job record has a title plus some company reference and a link/id."""
    if not isinstance(d, dict):
        return False
    if "title" not in d:
        return False
    has_company = any(k in d for k in
                      ("organization", "company", "company_name", "org"))
    has_ref = any(k in d for k in ("url", "slug", "id", "application_url"))
    return has_company and has_ref


def walk_for_jobs(obj, found):
    """Recursively collect the largest list of job-like dicts in the payload."""
    if isinstance(obj, list):
        job_items = [x for x in obj if looks_like_job(x)]
        if len(job_items) >= 1 and len(job_items) >= len(obj) / 2:
            found.append(job_items)
        for x in obj:
            walk_for_jobs(x, found)
    elif isinstance(obj, dict):
        for v in obj.values():
            walk_for_jobs(v, found)


def text_of(v):
    if isinstance(v, dict):
        return v.get("name") or v.get("title") or v.get("label") or ""
    return v or ""


def normalize(job, board_root):
    title = str(job.get("title", "")).strip()

    company = ""
    for k in ("organization", "company", "org"):
        if k in job:
            company = text_of(job[k])
            break
    if not company:
        company = str(job.get("company_name", "")).strip()

    loc = ""
    if "locations" in job and isinstance(job["locations"], list):
        loc = ", ".join(text_of(x) for x in job["locations"] if text_of(x))
    elif "location" in job:
        loc = text_of(job["location"])

    url = job.get("url") or job.get("application_url") or ""
    if not url:
        slug = job.get("slug")
        jid = job.get("id")
        if slug:
            url = board_root.rstrip("/") + "/jobs/" + str(slug)
        elif jid:
            url = board_root.rstrip("/") + "/jobs/" + str(jid)
    if url and url.startswith("/"):
        url = board_root.rstrip("/") + url

    posted = (job.get("created_at") or job.get("posted_at")
              or job.get("timestamp") or job.get("updated_at") or "")

    return {
        "company": str(company).strip(),
        "title": title,
        "location": str(loc).strip(),
        "posted": str(posted).strip(),
        "url": str(url).strip(),
    }


def board_root_of(board_url):
    p = urlparse(board_url)
    return f"{p.scheme}://{p.netloc}"


def scrape_board(board_url):
    root = board_root_of(board_url)
    html = fetch(board_url)
    data = extract_next_data(html)
    if data is None and html is not None and "__NEXT_DATA__" not in html:
        print("    ! page has no __NEXT_DATA__ - not a live Getro board")
        return []
    if data is None:
        # try the root as a fallback
        html = fetch(root)
        data = extract_next_data(html)
    if data is None:
        return []
    buckets = []
    walk_for_jobs(data, buckets)
    if not buckets:
        return []
    best = max(buckets, key=len)
    return [normalize(j, root) for j in best]


# ----------------------------------------------------------------- FILTER
def keep(job):
    hay = f"{job['title']} {job['company']}".lower()
    if any(x in hay for x in EXCLUDE):
        return False
    return any(x in hay for x in INCLUDE)


# ----------------------------------------------------------------- MODES
def run_scrape(boards, do_filter=True):
    seen = set()
    rows = []
    for i, b in enumerate(boards, 1):
        print(f"[{i}/{len(boards)}] {b}")
        jobs = scrape_board(b)
        print(f"    found {len(jobs)} jobs")
        for j in jobs:
            if do_filter and not keep(j):
                continue
            key = (j["company"].lower(), j["title"].lower(), j["url"])
            if key in seen:
                continue
            seen.add(key)
            j["board"] = board_root_of(b)
            rows.append(j)
        time.sleep(DELAY_SECONDS)

    rows.sort(key=lambda x: x["posted"], reverse=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f, fieldnames=["company", "title", "location", "posted", "url", "board"])
        w.writeheader()
        w.writerows(rows)
    print(f"\nWrote {len(rows)} rows to {OUTPUT}"
          + ("" if do_filter else "  (unfiltered)"))


def run_validate(url):
    root = board_root_of(url)
    html = fetch(url if "/jobs" in url else root + "/jobs")
    if html is None:
        print("NOT a reachable page.")
        return
    is_getro = ("__NEXT_DATA__" in html) and ("getro" in html.lower())
    jobs = scrape_board(url if "/jobs" in url else root + "/jobs")
    print(f"URL:        {url}")
    print(f"Getro board: {'yes' if is_getro else 'unclear - inspect manually'}")
    print(f"Jobs seen:   {len(jobs)}")
    if jobs[:3]:
        print("Sample:")
        for j in jobs[:3]:
            print(f"  - {j['title']}  @ {j['company']}  ({j['location']})")


def load_boards(path):
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                out.append(line.split(",")[0].strip())
    return out


def main():
    ap = argparse.ArgumentParser(description="Scrape Getro job boards.")
    ap.add_argument("--boards", help="file with one board URL per line")
    ap.add_argument("--validate", help="check a single URL and exit")
    ap.add_argument("--no-filter", action="store_true",
                    help="keep all jobs, skip keyword filtering")
    args = ap.parse_args()

    if args.validate:
        run_validate(args.validate)
        return
    boards = load_boards(args.boards) if args.boards else BOARDS
    run_scrape(boards, do_filter=not args.no_filter)


if __name__ == "__main__":
    main()
