#!/usr/bin/env python3
"""
workday_discover_boards.py — turn the cached Workday CDX sweep into probed
(tenant, pod, site, domain) boards and merge them into tenants/workday.csv.

  python3 workday_discover_boards.py            parse + probe + merge
  python3 workday_discover_boards.py --no-probe parse only (writes candidates)
  python3 workday_discover_boards.py --dry      report, write nothing
  python3 workday_discover_boards.py --limit N  probe at most N new boards

WHY THIS IS A SEPARATE SCRIPT.  Every other family in this repo is discovered
as a single slug, so slug_discover_boards can extract it generically.  A
Workday board is not a slug: the CXS endpoint is /wday/cxs/{tenant}/{site}/jobs
and the board lives at {tenant}.{pod}.myworkdayjobs.com/{site}, so identity is
a TRIPLE and one tenant routinely owns several sites (External, Campus,
Alumni...).  slug_common carries the two Workday apexes as raw_only=True: the
generic crawler fetches, caches and state-tracks their pages exactly like any
other apex, and this script owns the parse.

WHY THIS MATTERS MORE THAN THE OTHER FAMILIES.  Workday measured 328 rows per
board on 2026-08-19 (581,118 rows / 1,769 boards), the highest of any platform
here, and its tenant list came from ONE channel: ats_mine_tenants.py mining the
finished master.  That is a closed loop -- it can only ever contain employers
some other source already handed us -- and it is why the count sat at 1,786
against a platform with an order of magnitude more customers.

THREE TRAPS, all measured 2026-08-20:

  1. SANDBOX PODS.  SURT sorts "impl-" before "wd", so the early CDX pages are
     almost entirely Workday IMPLEMENTATION tenants (impl-wd10, impl-wd103,
     aade-wd106) plus -preview/-gold copies.  A page-0 sample reads the whole
     platform as ~19 tenants.  Only ^wd\\d+$ is production.

  2. CASE IS LOAD-BEARING.  {site} is case-sensitive
     ("SwisscomExternalCareers"), and a SURT urlkey is lowercased -- which is
     why the apexes ask for fl=original and this parser reads real URLs.

  3. A 200 IS NOT A BOARD.  Per workday_fetch's header, a retired tenant
     resolves (wildcard DNS per pod) and the edge 303-redirects rather than
     404ing, and a wrong tenant/pod pair answers HTTP 422.  Nothing here is
     admitted on a URL shape; every new board is probed through
     workday_fetch.post_jobs, which already knows those discriminators.
"""
import argparse
import concurrent.futures as cf
import csv
import os
import re
import sys
import threading
from collections import defaultdict
from urllib.parse import urlparse

import requests

import contract_common as cc
import slug_common as sc
import workday_fetch as wf

BASE = os.path.dirname(os.path.abspath(__file__))
RAW_TXT = os.path.join(BASE, "slug_wayback_urls.txt")
CAND_CSV = os.path.join(BASE, "workday_candidates.csv")
TENANTS_CSV = wf.TENANTS_CSV
TAGS = {"workday-jobs": wf.JOBS_DOMAIN, "workday-site": wf.SITE_DOMAIN}

# {tenant}.{pod}.myworkdayjobs.com/[{locale}/]{site}/...
JOBS_RE = re.compile(
    r"^https?://(?P<tenant>[a-z0-9][a-z0-9_-]*)\.(?P<pod>[a-z0-9-]+)"
    r"\.myworkdayjobs\.com/(?P<rest>.*)$", re.I)
# {pod}.myworkdaysite.com/recruiting/{tenant}/{site}/...
SITE_RE = re.compile(
    r"^https?://(?P<pod>[a-z0-9-]+)\.myworkdaysite\.com/recruiting/"
    r"(?P<tenant>[a-z0-9][a-z0-9_-]*)/(?P<rest>.*)$", re.I)

PROD_POD = re.compile(r"^wd\d+$", re.I)
LOCALE = re.compile(r"^[a-z]{2}([-_][A-Za-z]{2})?$")
ASSET = re.compile(r"\.(ico|png|jpe?g|css|js|svg|woff2?|gif|xml|txt|map)$", re.I)
# Path segments that are Workday chrome, never a career site.
NOT_A_SITE = {"wday", "assets", "static", "images", "img", "login", "logout",
              "favicon.ico", "robots.txt", "api", "d", "error", "errors",
              "signin", "signout", "auth", "public", "resources", "cdn"}
# Tenant suffixes that mark a non-production copy even on a production pod.
SANDBOX_TENANT = re.compile(
    r"(-|_)(preview|gold|sandbox|sbx|impl|dev|test|uat|stage|staging|train|"
    r"training|demo|dr|poc)\d*$", re.I)


def parse_raw():
    """(tenant, pod, site, domain) -> times seen, from the cached CDX rows."""
    found = defaultdict(int)
    skipped = defaultdict(int)
    if not os.path.exists(RAW_TXT):
        raise SystemExit(
            f"missing {os.path.basename(RAW_TXT)} — run "
            f"`python3 slug_discover_boards.py --apex workday-jobs,workday-site`"
            f" first")
    with open(RAW_TXT, encoding="utf-8", errors="replace") as f:
        for line in f:
            tag, _, url = line.partition("\t")
            dom = TAGS.get(tag)
            if dom is None:
                continue
            url = url.strip()
            m = (JOBS_RE if dom == wf.JOBS_DOMAIN else SITE_RE).match(url)
            if not m:
                skipped["unparsed"] += 1
                continue
            tenant = m.group("tenant").lower()
            pod = m.group("pod").lower()
            if not PROD_POD.match(pod):
                skipped["sandbox-pod"] += 1
                continue
            if SANDBOX_TENANT.search(tenant):
                skipped["sandbox-tenant"] += 1
                continue
            path = urlparse("https://x/" + m.group("rest")).path
            if ASSET.search(path):
                skipped["asset"] += 1
                continue
            segs = [x for x in path.split("/") if x]
            # A locale prefix shifts the site by one; strip it rather than
            # indexing blind, the same reason rippling's apex uses an anchor.
            if segs and LOCALE.match(segs[0]):
                segs = segs[1:]
            if not segs:
                skipped["no-site"] += 1
                continue
            site = segs[0]
            if site.lower() in NOT_A_SITE or len(site) > 64:
                skipped["not-a-site"] += 1
                continue
            found[(tenant, pod, site, dom)] += 1
    return found, skipped


def load_known():
    known = {}
    if not os.path.exists(TENANTS_CSV):
        return known
    with open(TENANTS_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not (r.get("tenant") and r.get("wd_host") and r.get("site")):
                continue
            dom = (r.get("wd_domain") or wf.JOBS_DOMAIN).strip() \
                or wf.JOBS_DOMAIN
            known[(r["tenant"].lower(), r["wd_host"].lower(),
                   r["site"], dom)] = r
    return known


def probe_one(board):
    """(board, n_jobs) if it is a live career site, else (board, None)."""
    tenant, pod, site, dom = board
    url = wf.wd_urls(tenant, pod, site, dom)[0]
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                      "Content-Type": "application/json"})
    try:
        with wf.gate(pod):
            d = wf.post_jobs(s, pod, url, {"appliedFacets": {}, "limit": 1,
                                           "offset": 0, "searchText": ""})
    except (wf.Dead, wf.Transient):
        return board, None
    except (requests.RequestException, ValueError):
        return board, None
    finally:
        s.close()
    if not isinstance(d, dict):
        return board, None
    total = d.get("total")
    if total is None and not d.get("jobPostings"):
        return board, None
    # total == 0 is a REAL board with nothing open today.  Keeping it is the
    # point: workday_fetch will find its postings on a later run, and dropping
    # it here would make the tenant undiscoverable until CDX re-archives it.
    return board, int(total or 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-probe", action="store_true")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--from-candidates", action="store_true",
                    help="merge boards already recorded live in "
                         "workday_candidates.csv instead of re-probing")
    ap.add_argument("--workers", type=int,
                    default=int(os.environ.get("WD_DISC_WORKERS") or 24))
    a = ap.parse_args()

    found, skipped = parse_raw()
    known = load_known()
    new = sorted(k for k in found if k not in known)
    print(f"cdx rows parsed -> {len(found):,} candidate boards "
          f"({len({k[0] for k in found}):,} tenants)")
    print(f"  skipped: {dict(skipped)}")
    print(f"  already in {os.path.relpath(TENANTS_CSV, BASE)}: "
          f"{len(known):,} boards")
    print(f"  NEW: {len(new):,} boards "
          f"({len({k[0] for k in new}):,} tenants)")

    if a.limit and len(new) > a.limit:
        print(f"  --limit {a.limit}: probing the {a.limit} most-seen")
        new = sorted(new, key=lambda k: -found[k])[:a.limit]

    live, probed = {}, set()
    if getattr(a, "from_candidates", False):
        # Replays a previous probe.  Exists because run_all's prelude
        # (ats_mine_tenants) rewrites tenants/workday.csv from the master, and
        # before src= was introduced that deleted every board only this sweep
        # knew about.  Re-probing 5,890 boards to recover them costs ~6
        # minutes; the verdicts are already on disk.
        if os.path.exists(CAND_CSV):
            with open(CAND_CSV, newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    k = (r["tenant"], r["wd_host"], r["site"],
                         r.get("wd_domain") or wf.JOBS_DOMAIN)
                    if r.get("status") == "dead":
                        probed.add(k)      # keep the old refusal on record
                    elif r.get("status") == "live" and k not in known:
                        live[k] = int(r.get("probe_jobs") or 0)
                        probed.add(k)
        print(f"replayed {len(live):,} live boards from "
              f"{os.path.relpath(CAND_CSV, BASE)}")
    elif new and not a.no_probe:
        print(f"probing {len(new):,} new boards at {a.workers} workers ...",
              flush=True)
        done = 0
        lk = threading.Lock()
        with cf.ThreadPoolExecutor(max_workers=a.workers) as ex:
            for board, n in ex.map(probe_one, new):
                with lk:
                    done += 1
                    probed.add(board)
                    if n is not None:
                        live[board] = n
                    if done % 250 == 0:
                        print(f"  {done:,}/{len(new):,} probed, "
                              f"{len(live):,} live", flush=True)
        print(f"probe done: {len(live):,} live of {len(new):,} "
              f"({100.0 * len(live) / max(1, len(new)):.1f}%)")

    if a.dry:
        print("dry run — nothing written")
        return

    with open(CAND_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tenant", "wd_host", "site", "wd_domain", "cdx_hits",
                    "probe_jobs", "status"])
        for k in sorted(found):
            # "dead" means PROBED AND REFUSED, never merely "not probed this
            # run".  Conflating them let a --no-probe pass followed by a
            # --from-candidates pass relabel 3,293 probed-live boards as dead
            # and destroy the very verdicts this file exists to remember.
            st = ("known" if k in known
                  else "live" if k in live
                  else "dead" if k in probed else "unprobed")
            w.writerow([k[0], k[1], k[2], k[3], found[k],
                        live.get(k, ""), st])
    print(f"wrote {os.path.relpath(CAND_CSV, BASE)} ({len(found):,} rows)")

    if not live:
        print("no new live boards — tenants/workday.csv unchanged")
        return

    # Merge, never rewrite from scratch.  ats_mine_tenants.py rebuilds this
    # file from the master on every run, so anything discovered here that the
    # master does not yet know about would be erased if this script were the
    # one to truncate it.  Appending preserves the live >= prev invariant the
    # ATS ledger relies on.
    rows = []
    if os.path.exists(TENANTS_CSV):
        with open(TENANTS_CSV, newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            cols = list(rd.fieldnames or [])
            rows = list(rd)
    else:
        cols = []
    for c in ("tenant", "wd_host", "site", "n_rows", "example_url",
              "wd_domain", "src"):
        if c not in cols:
            cols.append(c)
    for (tenant, pod, site, dom), n in sorted(live.items()):
        rows.append({"tenant": tenant, "wd_host": pod, "site": site,
                     "n_rows": n, "wd_domain": dom,
                     # src marks the CHANNEL.  ats_mine_tenants rewrites this
                     # file from the master on every run and would otherwise
                     # delete every board only this sweep knows about.
                     "src": "cdx",
                     "example_url": wf.wd_urls(tenant, pod, site, dom)[1]})
    tmp = TENANTS_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            r.setdefault("wd_domain", wf.JOBS_DOMAIN)
            w.writerow(r)
    os.replace(tmp, TENANTS_CSV)
    print(f"merged {len(live):,} new boards -> "
          f"{os.path.relpath(TENANTS_CSV, BASE)} ({len(rows):,} rows total)")


if __name__ == "__main__":
    main()
