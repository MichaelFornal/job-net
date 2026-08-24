#!/usr/bin/env python3
"""
yc_discover_boards.py — the slug list for Y Combinator's Work at a Startup
boards.

  https://yc-oss.github.io/api/companies/hiring.json   companies hiring now
  https://yc-oss.github.io/api/companies/all.json      every YC company
                                                       (YC_ALL_SLUGS=1)

NO WAYBACK CDX HERE, deliberately, unlike every other discover script in this
repo.  Measured: of the distinct YC company slugs appearing anywhere in
all_jobs_master.csv, all but one are already in hiring.json.  A CDX crawl
would cost 10-20 minutes of flaky paging to add roughly one slug per 156.  The
yc-oss API is the authoritative list; a grep of the local corpus covers
anything that started hiring since the last yc-oss build.

Guard: GitHub Pages has served truncated stubs for these files before, and
silently overwriting the candidate list with a stub would look like "YC has
almost no jobs" rather than like a failure.  Hence the hard floor below.

Outputs
  yc_candidates.csv   slug,company,batch,stage,team_size,industry,status,sources
  yc_companies.json   verbatim records, so the build step gets company
                      metadata without a second fetch
  yc_discovery_state.json
"""
import csv
import json
import os
import random
import re
import subprocess
import sys
import time

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""
ALL_SLUGS = os.environ.get("YC_ALL_SLUGS") == "1"

OUT_CSV = os.path.join(BASE, PFX + "yc_candidates.csv")
COMPANIES_JSON = os.path.join(BASE, PFX + "yc_companies.json")
STATE_JSON = os.path.join(BASE, PFX + "yc_discovery_state.json")
ALLJOBS_CSV = os.path.join(BASE, "all_jobs_master.csv")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")

API = "https://yc-oss.github.io/api/companies/%s.json"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TRIES = 6
TIMEOUT = 120
# Stub guard: real files hold ~1,494 (hiring) and ~6,085 (all).
FLOOR = {"hiring": 1000, "all": 5000}
SLUG_OK = re.compile(r"^[a-z0-9][a-z0-9._-]{0,79}$")


def get_json(url):
    last = None
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    try:
        for attempt in range(TRIES):
            try:
                r = s.get(url, timeout=TIMEOUT)
                if r.status_code == 200:
                    try:
                        return r.json()
                    except ValueError:
                        last = "bad-json"
                else:
                    last = "HTTP %d" % r.status_code
            except requests.RequestException as e:
                last = type(e).__name__
            time.sleep(min(5 * (2 ** attempt), 120) + random.uniform(0, 3))
    finally:
        s.close()
    raise SystemExit("yc-oss %s failed after %d tries: %s"
                     % (url, TRIES, last))


def grep_slugs(path, tag, add):
    if not os.path.exists(path):
        print("  (%s: %s missing, skipped)"
              % (tag, os.path.basename(path)), flush=True)
        return
    pat = r"ycombinator\.com/companies/[A-Za-z0-9][A-Za-z0-9_.-]{0,60}"
    with open(path, "rb") as fh:
        out = subprocess.run(["grep", "-aoE", pat], stdin=fh,
                             capture_output=True, text=True).stdout
    n = 0
    for hit in set(out.splitlines()):
        slug = hit.rsplit("/", 1)[-1].strip().lower()
        if slug and slug not in ("companies", "founders") \
                and SLUG_OK.match(slug):
            add(slug, tag)
            n += 1
    print("  %s: %d slugs" % (tag, n), flush=True)


def main(argv=None):
    argv = list(argv if argv is not None else sys.argv[1:])
    do_grep = "--no-grep" not in argv and not SMOKE

    which = "all" if ALL_SLUGS else "hiring"
    recs = get_json(API % which)
    if not isinstance(recs, list):
        raise SystemExit("yc-oss %s.json was not a list" % which)
    if len(recs) < FLOOR[which]:
        raise SystemExit(
            "yc-oss %s.json returned only %d companies (floor %d) — refusing "
            "to overwrite %s with a stub"
            % (which, len(recs), FLOOR[which], OUT_CSV))
    print("yc-oss %s.json: %d companies" % (which, len(recs)), flush=True)

    cands = {}          # slug -> dict
    srcs = {}           # slug -> set(source tags)

    def add(slug, tag):
        srcs.setdefault(slug, set()).add(tag)
        cands.setdefault(slug, {})

    kept = 0
    for c in recs:
        slug = (c.get("slug") or "").strip().lower()
        if not slug or not SLUG_OK.match(slug):
            continue
        if (c.get("status") or "").strip() and c.get("status") != "Active":
            continue
        add(slug, which)
        cands[slug] = {
            "company": c.get("name") or "",
            "batch": c.get("batch") or "",
            "stage": c.get("stage") or "",
            "team_size": c.get("team_size") or "",
            "industry": c.get("industry") or "",
            "subindustry": c.get("subindustry") or "",
            "regions": "; ".join(c.get("regions") or [])
                       if isinstance(c.get("regions"), list) else "",
            "website": c.get("website") or "",
            "status": c.get("status") or "",
        }
        kept += 1
    print("  active with a usable slug: %d" % kept, flush=True)

    if do_grep:
        grep_slugs(ALLJOBS_CSV, "alljobs", add)
        grep_slugs(GETRO_JSONL, "getro", add)

    rows = sorted(cands)
    cols = ["slug", "company", "batch", "stage", "team_size", "industry",
            "subindustry", "regions", "website", "status", "sources"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for slug in rows:
            d = cands[slug]
            w.writerow([slug] + [d.get(c, "") for c in cols[1:-1]]
                       + ["|".join(sorted(srcs.get(slug, ())))])

    tmp = COMPANIES_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({s: cands[s] for s in rows}, f)
    os.replace(tmp, COMPANIES_JSON)

    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"source": which, "records": len(recs), "slugs": len(rows),
                   "grepped": bool(do_grep)}, f)
    os.replace(tmp, STATE_JSON)

    print("%d raw company records -> %d candidate slugs -> %s"
          % (len(recs), len(rows), OUT_CSV), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
