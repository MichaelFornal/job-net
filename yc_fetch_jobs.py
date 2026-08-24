#!/usr/bin/env python3
"""
yc_fetch_jobs.py — pull Work at a Startup postings from the public YC company
pages.  Fetch doubles as the probe.

  GET https://www.ycombinator.com/companies/{slug}/jobs
      Accept: text/html          <-- LOAD-BEARING

`Accept: application/json` returns HTTP 404 {"message":"Not found"} on the
exact same URL.  With text/html the page carries the complete job list inline
in an Inertia.js data-page attribute; no login (props.signedIn is false).

WHY NO SEPARATE PROBE STAGE (following ashby_fetch_jobs.py): the page that
answers "is this board alive" is the same 114-240 KB page that carries the
jobs, so probing separately would download ~170 MB twice for no new
information.

Statuses:
  live    props.jobPostings present (possibly empty)
  dead    404 — normal for a slug that is not a WaaS company
  nodata  200, page parsed, but no props.jobPostings.  Distinct from live/dead
          on purpose: it is the signature of an Inertia schema change, and it
          is re-tried next run.  If it exceeds NODATA_TRIP of the 200s the run
          fails loudly rather than quietly writing zero jobs everywhere.

POSTED DATES — the subtle part.  `createdAt` is a Rails
`time_ago_in_words` string ("5 days", "about 2 months", "over 1 year"), i.e.
relative to fetch time, not a date.  Converting it on every run would make the
same posting drift a month older each month; all_jobs_build.merge() keeps the
EARLIEST posted, so rows would march backwards until they fell off the
POSTED_SINCE cutoff and vanished while still live.  So the computed date is
frozen in yc_posted_ledger.json the first time a posting id is seen and reused
verbatim forever after.
"""
import csv
import html
import json
import os
import re
import shutil
import sys
import threading
import time
import concurrent.futures as cf
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

CAND_CSV = os.path.join(BASE, PFX + "yc_candidates.csv")
OUT_JSONL = os.path.join(BASE, PFX + "yc_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, PFX + "yc_fetch_state.json")
LEDGER_JSON = os.path.join(BASE, PFX + "yc_posted_ledger.json")
BOARDS_CSV = os.path.join(BASE, PFX + "yc_job_boards.csv")

URL = "https://www.ycombinator.com/companies/%s/jobs"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

WORKERS = 8
RPS = 4.0
TIMEOUT = 90
TRIES = 6
MIN_FREE_GB = 5
NODATA_TRIP = 0.20
# Multi-KB prose and a signed S3 URL that changes every fetch; nothing
# downstream reads them and they would bloat the JSONL.
STRIP_JOB = ("companyLogoUrl", "hiringManager", "skills", "minSchoolYear",
             "askUs", "isIncomplete", "companyOneLiner")
KEEP_COMPANY = ("id", "slug", "name", "batch_name", "one_liner", "website",
                "team_size", "location", "country", "tags")

TODAY = datetime.now(timezone.utc)
TODAY_S = TODAY.strftime("%Y-%m-%d")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_counts = {"live": 0, "dead": 0, "nodata": 0, "failed": 0, "r429": 0}
_state = {}
_ledger = {}

DATA_PAGE_RE = re.compile(r'data-page="(.*?)"', re.S)
# Rails distance_of_time_in_words output.
AGO_RE = re.compile(r"^(?:(about|over|almost|less than)\s+)?"
                    r"(a|an|\d+)\s+"
                    r"(second|minute|hour|day|week|month|year)s?$")


def ago_days(text):
    """Rails relative phrase -> whole days, or None.

    Bucket midpoints, because Rails already rounded: 'about 1 month' covers
    30-45 days, 'about 2 months' covers 45-60, and so on.
    """
    if not text:
        return None
    t = str(text).strip().lower()
    m = AGO_RE.match(t)
    if not m:
        return None
    qual, num, unit = m.group(1), m.group(2), m.group(3)
    n = 1 if num in ("a", "an") else int(num)
    if unit in ("second", "minute", "hour"):
        return 0
    if unit == "day":
        return n
    if unit == "week":
        return n * 7
    if unit == "month":
        if qual == "about":
            return 37 if n == 1 else 52 if n == 2 else int(round(n * 30.44))
        return int(round(n * 30.44))
    # years
    if n == 1:
        if qual == "over":
            return 548
        if qual == "almost":
            return 305
        return 410                      # "about 1 year" / "1 year"
    if qual == "almost":
        return n * 365 - 60
    if qual == "over":
        return n * 365 + 180
    return n * 365


def posted_from(created_raw):
    d = ago_days(created_raw)
    if d is None:
        return "", "first_seen"
    return (TODAY - timedelta(days=d)).strftime("%Y-%m-%d"), "relative"


class _PageAttr(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self, convert_charrefs=True)
        self.raw = []

    def handle_starttag(self, tag, attrs):
        for k, v in attrs:
            if k == "data-page" and v:
                self.raw.append(v)


def extract_props(text):
    """-> the Inertia props dict, or None.

    Regex is safe here and the invariant is checkable: Inertia serialises the
    payload into an HTML attribute, so every quote inside the JSON is &quot;
    and a non-greedy match cannot terminate early.  Every data-page attribute
    is tried rather than assuming the first one is the Inertia root.
    """
    for m in DATA_PAGE_RE.finditer(text):
        try:
            d = json.loads(html.unescape(m.group(1)))
        except ValueError:
            continue
        if isinstance(d, dict) and isinstance(d.get("props"), dict):
            return d["props"]
    p = _PageAttr()                      # lenient fallback
    try:
        p.feed(text)
    except Exception:                    # noqa: BLE001 — malformed markup
        return None
    for raw in p.raw:
        try:
            d = json.loads(raw)
        except ValueError:
            continue
        if isinstance(d, dict) and isinstance(d.get("props"), dict):
            return d["props"]
    return None


def throttle():
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[0]:
                wait = _pause_until[0] - now
            else:
                wait = _next_t[0] - now
                if wait <= 0:
                    _next_t[0] = max(_next_t[0] + _interval[0], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return default
    return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def fetch_board(slug):
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "text/html"})
    last = ""
    try:
        for attempt in range(TRIES):
            throttle()
            try:
                r = s.get(URL % slug, timeout=TIMEOUT)
                if r.status_code == 404:
                    return "dead", [], {}
                if r.status_code in (429, 418, 403):
                    with _rate_lock:
                        _counts["r429"] += 1
                        _pause_until[0] = time.time() + 20
                        _interval[0] = min(_interval[0] * 1.5, 0.5)
                    last = "HTTP %d" % r.status_code
                elif r.status_code == 200:
                    props = extract_props(r.text)
                    if props is None or "jobPostings" not in props:
                        return "nodata", [], {}
                    jobs = props.get("jobPostings") or []
                    comp = props.get("company") or {}
                    comp = {k: comp.get(k) for k in KEEP_COMPANY
                            if k in comp}
                    out = []
                    for j in jobs:
                        if isinstance(j, dict):
                            out.append({k: v for k, v in j.items()
                                        if k not in STRIP_JOB})
                    return "live", out, comp
                else:
                    last = "HTTP %d" % r.status_code
            except requests.RequestException as e:
                last = type(e).__name__
            time.sleep(2 * (attempt + 1))
    finally:
        s.close()
    raise RuntimeError("%s failed after %d tries: %s" % (slug, TRIES, last))


def main():
    if not os.path.exists(CAND_CSV):
        raise SystemExit("missing %s — run yc_discover_boards.py" % CAND_CSV)
    free_gb = shutil.disk_usage(BASE).free / 1e9
    if free_gb < MIN_FREE_GB:
        raise SystemExit("only %.1f GB free — refusing to start" % free_gb)

    global _state, _ledger
    _state = load_json(STATE_JSON, {})
    _ledger = load_json(LEDGER_JSON, {})

    with open(CAND_CSV, newline="", encoding="utf-8") as f:
        cands = list(csv.DictReader(f))
    todo = [c for c in cands
            if (_state.get(c["slug"]) or {}).get("status")
            not in ("live", "dead")]
    already = len(cands) - len(todo)
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d unsettled boards, artifacts prefixed '%s'"
              % (len(todo), len(cands) - already, PFX), flush=True)
    print("%d candidates, %d settled, %d to fetch"
          % (len(cands), already, len(todo)), flush=True)

    total = ndone = 0
    t0 = time.time()
    with open(OUT_JSONL, "a", encoding="utf-8") as out:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(fetch_board, c["slug"]): c for c in todo}
            for fut in cf.as_completed(futs):
                c = futs[fut]
                slug = c["slug"]
                try:
                    status, jobs, comp = fut.result()
                except Exception as e:              # noqa: BLE001
                    with _lock:
                        _state[slug] = {"status": "failed",
                                        "error": str(e)[:120]}
                        _counts["failed"] += 1
                        ndone += 1
                        save_json(STATE_JSON, _state)
                    continue
                with _lock:
                    _counts[status] += 1
                    for j in jobs:
                        jid = str(j.get("id") or "")
                        if jid and jid not in _ledger:
                            posted, prec = posted_from(j.get("createdAt"))
                            _ledger[jid] = {
                                "posted": posted or TODAY_S,
                                "precision": prec,
                                "created_raw": j.get("createdAt") or "",
                                "first_seen": TODAY_S,
                            }
                        out.write(json.dumps({
                            "slug": slug,
                            "company": c.get("company") or slug,
                            "board_url": URL % slug,
                            "yc": comp,
                            "job": j,
                        }, ensure_ascii=False) + "\n")
                    out.flush()
                    total += len(jobs)
                    _state[slug] = {"status": status, "fetched": len(jobs),
                                    "fetched_at": TODAY_S}
                    ndone += 1
                    save_json(STATE_JSON, _state)
                    if ndone % 200 == 0:
                        save_json(LEDGER_JSON, _ledger)
                        el = time.time() - t0
                        eta = el / ndone * (len(todo) - ndone) / 60
                        print("  %d/%d boards | %d jobs | eta %.0fm"
                              % (ndone, len(todo), total, eta), flush=True)
    save_json(LEDGER_JSON, _ledger)

    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "board_url", "n_jobs", "status"])
        for s, v in sorted(_state.items()):
            if v.get("status") == "live":
                w.writerow([s, URL % s, v.get("fetched", 0), v["status"]])

    print("\nlive %d  dead %d  nodata %d  failed %d  jobs %d"
          % (_counts["live"], _counts["dead"], _counts["nodata"],
             _counts["failed"], total), flush=True)
    print("Done. %d job rows in %s; %d boards failed%s"
          % (total, OUT_JSONL, _counts["failed"],
             " (re-run to retry)" if _counts["failed"] else ""), flush=True)

    seen200 = _counts["live"] + _counts["nodata"]
    if seen200 and _counts["nodata"] / seen200 > NODATA_TRIP:
        raise SystemExit(
            "YC payload schema changed — %d of %d HTTP 200 responses had no "
            "props.jobPostings" % (_counts["nodata"], seen200))
    raise SystemExit(1 if _counts["failed"] else 0)


if __name__ == "__main__":
    sys.exit(main())
