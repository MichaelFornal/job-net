#!/usr/bin/env python3
"""
radancy_discover_boards.py — find every Radancy / TalentBrew career site in the
corpus and write tenants/radancy.csv.

WHY A DISCOVERY PASS EXISTS AT ALL
----------------------------------
Radancy has no multi-tenant hostname.  Every customer runs the platform on its
own vanity domain (jobs.walgreens.com, www.att.jobs, search.jobs.barclays,
www.commonspirit.careers ...), so there is no `*.radancy.com` to enumerate and
no directory endpoint.  The only place the tenant list exists is the corpus:
all_jobs_master.csv already carries these employers' apply URLs because
aggregators (getro, themuse, jobgether) republish them.

THE URL FINGERPRINT
    /job/{city-slug}/{title-slug}/{orgId}/{jobId}
    optionally behind a locale prefix: /en/job/..., /en-us/job/...
The two trailing numeric segments are the tell: {orgId} is the Radancy company
number (Walgreens 1242, AT&T 117, Citi 287, UnitedHealth 34088) and {jobId} is
an 11-digit posting id.  Measured 2026-08-20 over 3,218,751 master rows:
77 hosts, 65,557 rows carry it.

A NEAR MISS THAT IS NOT THIS FAMILY
    /{title-slug}/job/{32-HEX}         careers.dunkindonuts.com, stryker,
                                       pilotcompany, raisingcanes, marriott...
16 hosts / 2,287 rows.  These look Radancy-ish but every one of them answers
HTTP 404 on /search-jobs/results (measured, all 16).  They are a different
platform and are deliberately NOT written to tenants/radancy.csv — a fetcher
that assumed they were would mark 16 live boards dead on the first run.

WHAT COUNTS AS DISCOVERED (rule 1 — a 200 is not success)
    GET https://{host}{prefix}/search-jobs/results?<criteria>
A host is only recorded when the response is application/json, parses to a
dict, has a "results" key, and that HTML fragment contains
`data-total-results="N"`.  www.lockheedmartinjobs.com answers HTTP 200 for
both prefixes and is still dead — it is a parked redirect into
lockheedmartin.com/en-us/404.html.  Status code alone would have kept it.

Result of the 2026-08-20 discovery run: 76 of 77 hosts live, 105,430 postings.

Output: tenants/radancy.csv   columns: tenant,prefix,company,n_rows,example_url
  tenant       the vanity host, which IS the board id for this family
  prefix       "" or "/en" — the locale segment the site's own ajax URL uses
  company      employer name, read from the board's own RSS <channel><title>
  n_rows       rows the master already had from this host (ranking only)
  example_url  a real posting, kept for the next person who has to re-probe
"""
import argparse
import collections
import concurrent.futures as cf
import csv
import html
import json
import os
import re
import sys
from urllib.parse import urlsplit

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
MASTER = os.path.join(BASE, "all_jobs_master.csv")
OUT = os.path.join(BASE, "tenants", "radancy.csv")
CAND = os.path.join(BASE, "radancy_candidates.csv")

# /job/{city}/{title}/{orgId}/{jobId}, optional locale prefix.
JOB_RX = re.compile(
    r"^/(?:([a-z]{2}(?:-[a-z]{2})?)/)?job/[^/]+/[^/]+/(\d+)/(\d{6,})")

# Hosts that belong to ATS families this repo already pipes.  A Radancy job URL
# never lives on one of these, and letting them through would double-fetch.
KNOWN_ATS = re.compile(
    r"(icims\.com|myworkdayjobs\.com|greenhouse\.io|lever\.co|ashbyhq\.com|"
    r"smartrecruiters\.com|recruitee\.com|breezy\.hr|bamboohr\.com|"
    r"workable\.com|jobvite\.com|comeet\.com|taleo\.net|successfactors\.|"
    r"oraclecloud\.com|adp\.com|paylocity\.com|applytojob\.com|personio\.|"
    r"rippling\.com|ycombinator\.com|linkedin\.com|indeed\.com|glassdoor\.|"
    r"ziprecruiter\.|amazon\.jobs|getro\.com|notion\.site|zohorecruit|"
    r"paycomonline|ultipro\.com|brassring|avature\.net|phenompeople\.com|"
    r"eightfold\.ai)", re.I)

CRITERIA = (
    "ActiveFacetID=0&CurrentPage=1&RecordsPerPage=1&Distance=50"
    "&RadiusUnitType=0&Keywords=&Location=&ShowRadius=False&CustomFacetName="
    "&FacetTerm=&FacetType=0&SearchResultsModuleName=Search+Results"
    "&SortCriteria=0&SortDirection=0&SearchType=5")

TOTAL_RX = re.compile(r'data-total-results="(\d+)"')


def mine_master(path=MASTER, seed_only=False):
    """-> {host: {"n": rows, "prefix": "/en"|"", "example": url}} from the corpus."""
    if seed_only or not os.path.exists(path):
        return {}
    csv.field_size_limit(10 ** 9)
    hosts = collections.defaultdict(
        lambda: {"n": 0, "prefix": "", "example": ""})
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            u = row.get("apply_url") or ""
            if "/job/" not in u or not u.startswith("http"):
                continue
            try:
                sp = urlsplit(u)
            except ValueError:
                continue
            h = sp.netloc.lower()
            if not h or KNOWN_ATS.search(h):
                continue
            m = JOB_RX.match(sp.path)
            if not m:
                continue
            rec = hosts[h]
            rec["n"] += 1
            if m.group(1):
                rec["prefix"] = "/" + m.group(1)
            if not rec["example"]:
                rec["example"] = u.split("?")[0]
    return dict(hosts)


def _clean(s):
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip()


_COMPANY_PATS = [
    re.compile(r"^Working at (.+?)(?:\s*\||$)", re.I),
    re.compile(r"^Careers? at (.+?)(?:\s*\||$)", re.I),
    re.compile(r"^(?:Search |Explore )?Jobs? and Careers? at (.+?)(?:\s*\||$)",
               re.I),
    re.compile(r"^(.+?) Careers?(?:\s*[|:]|$)", re.I),
    re.compile(r"^(.+?)\s*\|", re.I),
]


def company_from_title(title, host):
    """Employer name out of the RSS channel title, host-derived as fallback.

    The corpus cannot supply this.  Aggregators attribute a Radancy apply URL
    to whatever portfolio company republished it, so all_jobs_master.csv says
    careers.moodys.com is "Passfort" and careers.munichre.com is "Next
    Insurance".  The board's own feed title says "Working at Moody's".
    """
    t = _clean(title)
    for rx in _COMPANY_PATS:
        m = rx.match(t)
        if m:
            v = _clean(m.group(1))
            v = re.sub(r"\s*[-–—:].*$", "", v).strip()
            if 1 < len(v) <= 60:
                return v
    if t and len(t) <= 60:
        return t
    stem = re.sub(r"^(jobs|careers|search|www|apply|jobsearch)\.", "", host)
    stem = re.sub(r"\.(com|org|net|jobs|careers|ca|co\.uk|au|io)$", "", stem)
    return stem.split(".")[0].replace("-", " ").title()


def probe(host, hint_prefix, timeout=30):
    """-> dict.  ok True only when the payload SHAPE is a TalentBrew result."""
    s = cc.session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json"})
    out = {"host": host, "codes": []}
    tried = []
    for pre in [hint_prefix, "", "/en"]:
        if pre in tried:
            continue
        tried.append(pre)
        url = "https://%s%s/search-jobs/results?%s" % (host, pre, CRITERIA)
        try:
            r = s.get(url, timeout=timeout)
        except requests.RequestException as e:
            out["codes"].append("%s:%s" % (pre or "/", type(e).__name__))
            continue
        ct = (r.headers.get("content-type") or "").split(";")[0]
        if r.status_code == 200 and "json" in ct:
            try:
                j = r.json()
            except ValueError:
                j = None
            if isinstance(j, dict) and isinstance(j.get("results"), str):
                m = TOTAL_RX.search(j["results"])
                if m:
                    out.update(ok=True, prefix=pre, total=int(m.group(1)))
                    break
        out["codes"].append("%s:%d:%s" % (pre or "/", r.status_code, ct))
    else:
        out["ok"] = False
    if out.get("ok"):
        out["company"] = _rss_company(s, host, out["prefix"])
    s.close()
    return out


def _rss_company(s, host, prefix):
    """First 300KB of /rss is enough for <channel><title>."""
    try:
        r = s.get("https://%s%s/rss" % (host, prefix), timeout=60, stream=True,
                  headers={"Accept": "application/xml"})
        if r.status_code != 200:
            return company_from_title("", host)
        buf = b""
        for chunk in r.iter_content(65536):
            buf += chunk
            if b"</title>" in buf or len(buf) > 300000:
                break
        r.close()
        t = buf.decode("utf-8", "replace")
        m = re.search(r"<channel>.*?<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?"
                      r"</title>", t, re.S)
        return company_from_title(m.group(1) if m else "", host)
    except requests.RequestException:
        return company_from_title("", host)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--no-probe", action="store_true",
                    help="write every mined host without verifying the route")
    ap.add_argument("--master", default=MASTER)
    a = ap.parse_args()

    mined = mine_master(a.master)
    print("mined %d candidate hosts from %s" % (len(mined), a.master),
          flush=True)
    if not mined:
        print("no candidates — is all_jobs_master.csv present?", flush=True)
        return 1

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(CAND, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["host", "n_rows", "prefix_hint", "example_url"])
        for h, r in sorted(mined.items(), key=lambda kv: -kv[1]["n"]):
            w.writerow([h, r["n"], r["prefix"], r["example"]])
    print("wrote %s" % CAND, flush=True)

    # Company names already in tenants/radancy.csv WIN over a freshly parsed
    # RSS title.  The parser gets ~80% of them right and the rest were fixed by
    # hand ("Join Spectrum" -> Spectrum, "Jobs with Purpose" -> Veolia,
    # "Explore new horizons with Boeing" -> Boeing); a re-run must not undo
    # that.  Delete the row, or the file, to re-derive one.
    kept = {}
    if os.path.exists(OUT):
        with open(OUT, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if (r.get("tenant") or "").strip():
                    kept[r["tenant"].strip()] = (r.get("company") or "").strip()

    rows = []
    if a.no_probe:
        for h, r in mined.items():
            rows.append((h, r["prefix"], kept.get(h) or company_from_title("", h),
                         r["n"], r["example"]))
    else:
        live = dead = 0
        with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
            futs = {ex.submit(probe, h, r["prefix"]): h
                    for h, r in mined.items()}
            for fut in cf.as_completed(futs):
                h = futs[fut]
                o = fut.result()
                if o.get("ok"):
                    live += 1
                    rows.append((h, o["prefix"], kept.get(h) or o["company"],
                                 mined[h]["n"], mined[h]["example"]))
                    print("  LIVE %-38s %-4s %7d  %s"
                          % (h, o["prefix"] or "-", o["total"], o["company"]),
                          flush=True)
                else:
                    dead += 1
                    print("  dead %-38s %s" % (h, o["codes"]), flush=True)
        print("live %d  dead %d" % (live, dead), flush=True)

    rows.sort(key=lambda r: -r[3])
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tenant", "prefix", "company", "n_rows", "example_url"])
        w.writerows(rows)
    print("wrote %s (%d boards)" % (OUT, len(rows)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
