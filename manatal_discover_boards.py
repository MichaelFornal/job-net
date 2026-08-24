#!/usr/bin/env python3
"""
manatal_discover_boards.py — enumerate careers-page.com (Manatal) tenant slugs
beyond the ones already visible in all_jobs_master.csv.

WHY A SEPARATE PASS
-------------------
tenants/manatal.csv is mined from the master, so it only holds tenants some
aggregator already indexed: 595 slugs.  Wayback's CDX index answers for this
domain (unlike zohorecruit, where it does not), and it is not close:

    http://web.archive.org/cdx/search/cdx?url=careers-page.com
        &matchType=domain&fl=original&collapse=urlkey&page={n}

The first 5,000 rows alone — urlkeys from "-black-pen-recruitment" through
"1r..." , i.e. the very start of the alphabet — carry 244 distinct slugs, 223 of
which the master has never seen.  Both host generations appear in the index and
both are collected:

    https://www.careers-page.com/{slug}[/job/{HASH}]      -> shape v1
    https://{slug}.careers-page.com/[jobs/{uuid}]         -> shape v2

WHAT CDX RETURNS THAT IS NOT A TENANT
-------------------------------------
The path space is shared with the product's own routes and with junk the
crawler recorded from CSS/analytics, so a raw first-segment extraction is not a
slug list.  Measured in the same 5,000 rows: `api`, `static`, `media`,
`.well-known`, `accounts`, and percent-encoded colour triples that arrived as
paths ("0,%2098,%20177").  RESERVED plus _SLUG_OK below drop those; anything
that survives is only a CANDIDATE — nothing here claims a board exists.

Liveness is deliberately NOT decided here.  manatal_fetch.py already owns the
dead/empty/blocked discrimination (a retired slug answers the v1 API with
HTTP 404 {"detail":"No ClientPortalSettings matches the given query."}, while a
Cloudflare interstitial off the v2 host must stay pending), and duplicating
that logic in two places is how the two copies drift.  With --probe this script
will run the cheap half of it — one v1 API call per candidate — purely to write
a `shape` hint and to report a yield number; without it, every candidate is
handed over unprobed and the fetcher spends two requests proving each corpse.

OUTPUT: manatal_candidates.csv  (tenant,n_rows,example_url,shape,found_by)
        manatal_fetch.py reads tenants/manatal.csv first and then this file,
        skipping slugs already present, so the mined list stays authoritative
        and this one only ever adds.

ENV / FLAGS
    --pages N     stop after N CDX pages (default: until a page comes back
                  empty; the domain was ~6-15 pages when this was written)
    --probe       probe each new candidate's v1 API once and record the shape
    --rps R       probe rate against www.careers-page.com (default 12)
    --out PATH    override the output CSV
"""
import argparse
import concurrent.futures as cf
import csv
import json
import os
import re
import sys
import threading
import time

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "manatal.csv")
OUT = os.path.join(BASE, "manatal_candidates.csv")

CDX = ("http://web.archive.org/cdx/search/cdx?url=careers-page.com"
       "&matchType=domain&fl=original&collapse=urlkey&page=%d")
WWW = "https://www.careers-page.com"
API = WWW + "/api/v1.0/c/%s/jobs/"

V1 = re.compile(r"^https?://(?:www\.)?careers-page\.com/([^/?#]+)", re.I)
V2 = re.compile(r"^https?://([a-z0-9][a-z0-9-]*)\.careers-page\.com/", re.I)
V1_JOB = re.compile(
    r"^https?://(?:www\.)?careers-page\.com/([^/?#]+)/job/([A-Za-z0-9]+)", re.I)
V2_JOB = re.compile(
    r"^https?://([a-z0-9][a-z0-9-]*)\.careers-page\.com/jobs/([0-9a-f-]{36})",
    re.I)

# Product routes and crawler debris that share the first path segment with
# tenant slugs.  Everything here was observed in the live CDX output.
RESERVED = {
    "api", "static", "media", "admin", "accounts", "account", "login",
    "logout", "signup", "register", "password", "favicon.ico", "robots.txt",
    "sitemap.xml", ".well-known", "www", "assets", "img", "images", "css",
    "js", "fonts", "health", "healthz", "status", "search", "job", "jobs",
    "apply", "refer", "careers", "career", "privacy", "terms", "cdn-cgi",
}
# A real slug is a Django slug: lowercase alnum + hyphen/underscore, 2..80.
# The percent signs, commas and spaces in "0,%2098,%20177" fail this outright.
_SLUG_OK = re.compile(r"^[a-z0-9][a-z0-9_-]{1,79}$")


def is_slug(s):
    s = (s or "").strip().lower()
    return bool(s) and s not in RESERVED and bool(_SLUG_OK.match(s))


def cdx_pages(max_pages=None, timeout=180):
    """Yield raw CDX lines page by page, stopping at the first empty page."""
    page = 0
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA})
    while max_pages is None or page < max_pages:
        url = CDX % page
        try:
            r = s.get(url, timeout=timeout)
        except requests.RequestException as e:
            print("  cdx page %d: %s — stopping" % (page, type(e).__name__),
                  flush=True)
            return
        if r.status_code != 200:
            print("  cdx page %d: HTTP %d — stopping" % (page, r.status_code),
                  flush=True)
            return
        lines = [l.strip() for l in r.text.splitlines() if l.strip()]
        print("  cdx page %d: %d urls" % (page, len(lines)), flush=True)
        if not lines:
            return
        for l in lines:
            yield l
        page += 1


def harvest(max_pages=None):
    """-> {slug: {"shape": v1|v2|"", "example_url": url}}"""
    found = {}
    for u in cdx_pages(max_pages=max_pages):
        m = V1_JOB.match(u)
        if m and is_slug(m.group(1)):
            found.setdefault(m.group(1).lower(),
                             {"shape": "v1", "example_url": u.split("?")[0]})
            continue
        m = V2_JOB.match(u)
        if m and is_slug(m.group(1)):
            found.setdefault(m.group(1).lower(),
                             {"shape": "v2", "example_url": u.split("?")[0]})
            continue
        m = V1.match(u)
        if m and is_slug(m.group(1)):
            found.setdefault(m.group(1).lower(),
                             {"shape": "", "example_url": "%s/%s"
                              % (WWW, m.group(1).lower())})
            continue
        m = V2.match(u)
        if m and is_slug(m.group(1)) and m.group(1).lower() != "www":
            found.setdefault(m.group(1).lower(),
                             {"shape": "", "example_url":
                              "https://%s.careers-page.com/" % m.group(1).lower()})
    return found


_rate_lock = threading.Lock()
_next_t = [0.0]


def probe(slug, rps):
    """One v1 API call.  -> ("v1", count) | ("", 0) | (None, 0) on a transient.

    Deliberately the SAME payload-shape test the fetcher uses: a 200 only
    counts if the body is a dict carrying both "count" and "results".  A 404
    here does not mean the tenant is dead — 9 of the 35 v2 portals in the
    corpus are absent from this API and alive on their own subdomain — so it
    resolves to "shape unknown", not to a verdict.
    """
    interval = 1.0 / rps
    while True:
        with _rate_lock:
            now = time.time()
            wait = _next_t[0] - now
            if wait <= 0:
                _next_t[0] = max(_next_t[0] + interval, now)
                break
        time.sleep(min(wait, 2))
    try:
        r = requests.get(API % slug, timeout=25,
                         headers={"User-Agent": cc.UA,
                                  "Accept": "application/json"})
    except requests.RequestException:
        return None, 0
    if r.status_code == 404:
        return "", 0
    if r.status_code != 200:
        return None, 0
    try:
        d = r.json()
    except ValueError:
        return None, 0
    if isinstance(d, dict) and "count" in d and "results" in d:
        return "v1", int(d.get("count") or 0)
    return None, 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, default=0,
                    help="stop after N CDX pages (0 = until empty)")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--rps", type=float, default=12.0)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args()

    known = set()
    if os.path.exists(TENANTS):
        with open(TENANTS, newline="", encoding="utf-8") as f:
            known = {(r.get("tenant") or "").strip().lower()
                     for r in csv.DictReader(f)}
        known.discard("")
    print("manatal discover: %d slugs already in tenants/manatal.csv"
          % len(known), flush=True)

    found = harvest(max_pages=a.pages or None)
    new = {k: v for k, v in found.items() if k not in known}
    print("cdx: %d slugs seen, %d not already known" % (len(found), len(new)),
          flush=True)

    counts = {}
    if a.probe and new:
        slugs = sorted(new)
        live = 0
        with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
            for slug, (shape, n) in zip(
                    slugs, ex.map(lambda s: probe(s, a.rps), slugs)):
                if shape == "v1":
                    new[slug]["shape"] = "v1"
                    counts[slug] = n
                    live += 1
                elif shape == "":
                    # No v1 portal.  Leave the shape hint alone: the fetcher
                    # still has to try the v2 host before calling it dead.
                    pass
        print("probe: %d/%d answer the v1 API, %d jobs on them"
              % (live, len(slugs), sum(counts.values())), flush=True)

    with open(a.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tenant", "n_rows", "example_url",
                                          "shape", "found_by"])
        w.writeheader()
        for slug in sorted(new):
            v = new[slug]
            w.writerow({"tenant": slug, "n_rows": counts.get(slug, ""),
                        "example_url": v["example_url"],
                        "shape": v["shape"], "found_by": "wayback-cdx"})
    print("wrote %s (%d candidates)" % (a.out, len(new)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
