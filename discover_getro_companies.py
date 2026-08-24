#!/usr/bin/env python3
"""
discover_getro_companies.py
Enumerate every company / fund inside a Getro network.

Point it at an aggregator's companies page (or any Getro board root) and it
parses the embedded __NEXT_DATA__ payload for the network's company list, then
writes companies.csv with each name, website, and any careers link. Use it to
grow your boards.csv beyond a hand-typed list: discover companies, spot the
ones that run their own Getro board, add those to the scraper.

Usage
-----
    python discover_getro_companies.py https://community.getro.com/companies
    python discover_getro_companies.py https://jobsinvc.getro.com/companies

Requires: requests
"""

import csv
import json
import re
import sys
from html import unescape

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run:  pip install requests")

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)


def extract_next_data(html):
    m = NEXT_DATA_RE.search(html or "")
    if not m:
        return None
    for candidate in (unescape(m.group(1)), m.group(1)):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def looks_like_company(d):
    if not isinstance(d, dict):
        return False
    if "name" not in d:
        return False
    # companies carry a domain/website/slug and usually NOT a job title field
    has_site = any(k in d for k in ("domain", "website", "slug", "url", "id"))
    is_job = "title" in d and ("locations" in d or "organization" in d)
    return has_site and not is_job


def walk_for_companies(obj, found):
    if isinstance(obj, list):
        items = [x for x in obj if looks_like_company(x)]
        if items and len(items) >= len(obj) / 2:
            found.append(items)
        for x in obj:
            walk_for_companies(x, found)
    elif isinstance(obj, dict):
        for v in obj.values():
            walk_for_companies(v, found)


def website_of(c):
    for k in ("website", "domain", "url"):
        v = c.get(k)
        if v:
            v = str(v)
            if v and not v.startswith("http"):
                v = "https://" + v
            return v
    return ""


def main():
    if len(sys.argv) < 2:
        sys.exit("Give a companies URL, e.g. "
                 "https://community.getro.com/companies")
    url = sys.argv[1]
    root = url.split("/companies")[0].rstrip("/")
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=25)
    data = extract_next_data(r.text)
    if data is None:
        sys.exit("Could not find __NEXT_DATA__ on that page.")

    buckets = []
    walk_for_companies(data, buckets)
    if not buckets:
        sys.exit("No company list found in the payload.")
    companies = max(buckets, key=len)

    rows = []
    seen = set()
    for c in companies:
        name = str(c.get("name", "")).strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        slug = c.get("slug", "")
        careers = root + "/companies/" + str(slug) if slug else ""
        rows.append({
            "name": name,
            "website": website_of(c),
            "careers_on_board": careers,
        })

    rows.sort(key=lambda x: x["name"].lower())
    with open("companies.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "website", "careers_on_board"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} companies to companies.csv")
    print("Next: skim for firms running their own Getro board, add those "
          "board URLs to boards.csv, then run getro_scraper.py --boards boards.csv")


if __name__ == "__main__":
    main()
