#!/usr/bin/env python3
"""
dayforce_discover_boards.py — enumerate Ceridian Dayforce career-site boards.

WHAT A DAYFORCE "BOARD" IS
--------------------------
Not one segment, TWO.  Every board is a {clientNamespace}/{careerSiteXRefCode}
pair, exactly like Workday's tenant+site:

    https://jobs.dayforcehcm.com/en-US/lightwavedental/CarolinasDentist
                                       ^client        ^site

and the locale segment is OPTIONAL.  Both of these are live and both are in the
corpus:

    https://jobs.dayforcehcm.com/en-US/lightwavedental/CarolinasDentist/jobs/7252
    https://jobs.dayforcehcm.com/crisis24/CANDIDATEPORTAL/jobs/23501

so a parser that indexes path segments blind reads "crisis24" as a locale and
loses every bare-form URL.  parse_pair() below locates the locale by SHAPE
(`xx-YY`) and strips it only when it matches, which is why both forms land on
the same pair.  The same rule is why the Next.js data-route form
`/_next/data/{buildId}/en-US/{client}/{site}/jobs/{id}.json` — which the Wayback
index is full of — has to have its three-segment prefix removed before the
locale test, not after.

TWO SOURCES, DELIBERATELY BOTH
------------------------------
  corpus  all_jobs_master.csv apply_urls.  139 pairs, all of them provably
          real (an aggregator saw a job on them), but only the ones some
          aggregator happened to index.
  cdx     web.archive.org.  4,563 pairs / 1,894 clients, measured 2026-08-20.
          Historical, so a large share are retired tenants — that is fine and
          expected: dayforce_fetch.py's sitecontext probe is the liveness
          gate, and it records a dead board with the reason that proved it.

The CDX call uses `collapse=urlkey:60`, not plain `collapse=urlkey`.  Plain
collapse returns one row per distinct URL, which for this domain means >1M rows
and a 504 from the CDX gateway (measured twice at limit=50000: the alphabet
stopped at "b").  Collapsing on the first 60 characters of the SURT key folds
every job id under a board into a handful of rows and returns the whole
alphabet, A to Z, in 20 seconds / 6.2 MB.  The prefix length is chosen to sit
past "com,dayforcehcm,jobs)/en-us/{client}/{site}" for typical names.

OUTPUT
------
  tenants/dayforce.csv        house format: tenant,n_rows,example_url
                              tenant is the *pair* "client/site".
  dayforce_candidates.csv     the same rows plus where each came from and the
                              per-source evidence counts.

ENV
---
  DAYFORCE_MASTER      path to all_jobs_master.csv (default: alongside this file)
  DAYFORCE_NO_CDX=1    corpus only — no network at all
  DAYFORCE_NO_CORPUS=1 CDX only (useful when the master is mid-rebuild)
  DAYFORCE_CDX_FILE    read the CDX dump from this file instead of fetching,
                       and write it there after a fetch.  Optional cache; the
                       script is complete without it.
  DAYFORCE_CDX_LIMIT   CDX row cap (default 200000)
"""
import collections
import csv
import os
import re
import sys

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
MASTER = os.environ.get("DAYFORCE_MASTER") or os.path.join(
    BASE, "all_jobs_master.csv")
TENANTS_DIR = os.path.join(BASE, "tenants")
TENANTS = os.path.join(TENANTS_DIR, "dayforce.csv")
CANDIDATES = os.path.join(BASE, "dayforce_candidates.csv")

HOST = "https://jobs.dayforcehcm.com"
CDX = ("http://web.archive.org/cdx/search/cdx?url=jobs.dayforcehcm.com"
       "&matchType=domain&output=text&fl=original&collapse=urlkey:60"
       "&filter=statuscode:200&limit=%d")
CDX_LIMIT = int(os.environ.get("DAYFORCE_CDX_LIMIT") or 200000)
CDX_FILE = os.environ.get("DAYFORCE_CDX_FILE") or ""

# `xx-YY` / `xx-YYY`: en-US, fr-CA, es-MX, en-GB, zh-Hans ... The Dayforce site
# advertises isoCultureCodes like ["en-US","fr-CA","es-MX"].
LOCALE_RX = re.compile(r"^[a-z]{2}-[A-Za-z]{2,4}$")
URL_RX = re.compile(r"^https?://(?:www\.)?jobs\.dayforcehcm\.com/(.*)$", re.I)

# First segments that are app plumbing, never a clientNamespace.
NOT_CLIENT = {"api", "_next", "static", "images", "img", "languages", "fonts",
              "icons", "locales", "assets", "auth", "favicon.ico",
              "robots.txt", "sitemap.xml", "sw.js", "_error", "404", "500"}
# Second segments that are a PAGE under a client, not a careerSiteXRefCode.
NOT_SITE = {"jobs", "privacypolicy", "accessibility", "savedjobs",
            "savedsearches", "recommendedjobs", "appliedjobs", "profile",
            "signin", "signup", "login", "logout", "search"}


def parse_pair(url):
    """('client', 'site') from any jobs.dayforcehcm.com URL, or None.

    Handles, in this order: the _next/data/{buildId}/... rewrite, an OPTIONAL
    locale segment matched by shape, then client/site.  Nothing is indexed
    blind — that is the whole point of the function.
    """
    m = URL_RX.match((url or "").strip())
    if not m:
        return None
    path = m.group(1).split("?")[0].split("#")[0]
    parts = [p for p in path.split("/") if p]
    if parts and parts[0] == "_next":
        # /_next/data/{buildId}/{locale}/{client}/{site}/...
        if len(parts) >= 4 and parts[1] == "data":
            parts = parts[3:]
        else:
            return None
    if parts and LOCALE_RX.match(parts[0]):
        parts = parts[1:]
    if len(parts) < 2:
        return None
    client, site = parts[0].lower(), parts[1].lower()
    if client in NOT_CLIENT or "." in client or site in NOT_SITE:
        return None
    if not client or not site:
        return None
    return client, site


def from_corpus():
    """{(client, site): (n_rows, example_url)} mined from the master."""
    out = collections.defaultdict(lambda: [0, ""])
    if not os.path.exists(MASTER):
        print("  (no master at %s — skipping corpus mine)" % MASTER)
        return {}
    csv.field_size_limit(10 ** 9)
    with open(MASTER, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            for col in ("apply_url", "job_url"):
                u = (r.get(col) or "").strip()
                if "jobs.dayforcehcm.com" not in u:
                    continue
                pair = parse_pair(u)
                if not pair:
                    continue
                rec = out[pair]
                rec[0] += 1
                if not rec[1] and "/jobs/" in u:
                    rec[1] = u
                break
    return {k: tuple(v) for k, v in out.items()}


def cdx_text():
    if CDX_FILE and os.path.exists(CDX_FILE):
        print("  cdx: reading cached %s" % CDX_FILE)
        with open(CDX_FILE, encoding="utf-8", errors="replace") as f:
            return f.read()
    url = CDX % CDX_LIMIT
    print("  cdx: fetching (limit=%d)" % CDX_LIMIT)
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA})
    for attempt in range(3):
        try:
            r = s.get(url, timeout=600)
        except requests.RequestException as e:
            print("  cdx: %s (attempt %d)" % (type(e).__name__, attempt + 1))
            continue
        if r.status_code == 200 and "dayforcehcm" in r.text:
            if CDX_FILE:
                with open(CDX_FILE, "w", encoding="utf-8") as f:
                    f.write(r.text)
            return r.text
        # A 504 here is the gateway giving up on too large a result set, not a
        # missing domain.  Say so rather than reporting "no boards found".
        print("  cdx: HTTP %d (attempt %d)" % (r.status_code, attempt + 1))
    print("  cdx: FAILED — continuing with the corpus pairs only")
    return ""


def from_cdx():
    out = collections.defaultdict(lambda: [0, ""])
    for line in cdx_text().splitlines():
        u = line.strip()
        if not u:
            continue
        pair = parse_pair(u)
        if not pair:
            continue
        rec = out[pair]
        rec[0] += 1
        if not rec[1] and "/jobs/" in u:
            rec[1] = u
    return {k: tuple(v) for k, v in out.items()}


def main():
    corpus = {} if os.environ.get("DAYFORCE_NO_CORPUS") == "1" else from_corpus()
    print("corpus: %d pairs" % len(corpus))
    cdx = {} if os.environ.get("DAYFORCE_NO_CDX") == "1" else from_cdx()
    print("cdx:    %d pairs" % len(cdx))

    pairs = sorted(set(corpus) | set(cdx))
    rows = []
    for client, site in pairs:
        cn, cu = corpus.get((client, site), (0, ""))
        xn, xu = cdx.get((client, site), (0, ""))
        found = "+".join(x for x in (("corpus" if cn else ""),
                                     ("cdx" if xn else "")) if x)
        example = cu or xu or "%s/en-US/%s/%s" % (HOST, client, site)
        rows.append({
            "tenant": "%s/%s" % (client, site),
            "client": client,
            "site": site,
            # n_rows RANKS the queue (the fetcher sorts by it), so it must be
            # the corpus count where there is one — CDX counts are snapshots of
            # an archive crawl, not job counts, and mixing them would put a
            # heavily-archived dead tenant ahead of a live 500-job board.
            "n_rows": cn,
            "cdx_urls": xn,
            "found_in": found,
            "example_url": example,
        })
    rows.sort(key=lambda r: (-r["n_rows"], -r["cdx_urls"], r["tenant"]))

    os.makedirs(TENANTS_DIR, exist_ok=True)
    with open(TENANTS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tenant", "n_rows", "example_url"])
        for r in rows:
            w.writerow([r["tenant"], r["n_rows"], r["example_url"]])
    with open(CANDIDATES, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["tenant", "client", "site", "n_rows",
                                          "cdx_urls", "found_in",
                                          "example_url"])
        w.writeheader()
        w.writerows(rows)

    both = sum(1 for r in rows if r["found_in"] == "corpus+cdx")
    print("wrote %s and %s: %d pairs, %d clients (%d in both sources)"
          % (TENANTS, CANDIDATES, len(rows),
             len({r["client"] for r in rows}), both))
    return 0


if __name__ == "__main__":
    sys.exit(main())
