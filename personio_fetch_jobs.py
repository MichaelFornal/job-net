#!/usr/bin/env python3
"""
personio_fetch_jobs.py — pull every posting from the Personio tenant boards in
personio_candidates.csv.  Fetch doubles as the probe.

  GET https://{slug}.jobs.personio.de/search.json   -> JSON array, whole board
  GET https://{slug}.jobs.personio.com/search.json  -> .com tenants

WHY THERE IS NO SEPARATE PROBE STAGE (a deliberate deviation from the
greenhouse/lever four-script shape, following the ashby_fetch_jobs.py
precedent): search.json IS the only public endpoint, so a probe would be a
byte-identical request.  That matters more here than anywhere else in the repo
because Personio rate-limits aggressively — a 20-slug sample returned 11x HTTP
429 against 9x 200.  Probing separately would double the load on the one host
that is already refusing more than half of what we send it.

Payload shape (sampled 567 postings across 8 tenants, 2026-07-27):
  id, name, employment_type, seniority, keywords, description, office,
  offices[], schedule, category, department, subcompany
Note what is absent: any posting date.  As with Rippling, undated rows survive
all_jobs_build.py's cutoff forever, so first-seen dates are frozen in
personio_posted_ledger.json.

Everything is German ("Festanstellung", "Berufserfahren", "Voll- oder
Teilzeit"), which is why personio_build_sheet.py maps the vocabulary and
defaults country to DE — see the note there.

`description` can be multi-KB HTML and nothing downstream reads it, so it is
stripped before the row is written (this volume is 6k+ boards on a disk that
is 93% full).
"""
import csv
import json
import os
import shutil
import sys
import threading
import time
import concurrent.futures as cf
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

CAND_CSV = os.path.join(BASE, PFX + "personio_candidates.csv")
OUT_JSONL = os.path.join(BASE, PFX + "personio_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, PFX + "personio_fetch_state.json")
LEDGER_JSON = os.path.join(BASE, PFX + "personio_posted_ledger.json")
BOARDS_CSV = os.path.join(BASE, PFX + "personio_job_boards.csv")

HOSTS = ("de", "com")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# The original note here read "measured 55% HTTP 429 at an unthrottled
# 20-request burst".  Re-measured 2026-07-28 with the redirect check in
# fetch_board(): 78 requests, 24 concurrent, no throttle at all, 30.8 rps ->
# 54x HTTP 200 and 24x HTTP 429, and EVERY ONE of the 429s came from a dead
# tenant redirected to personio.com's marketing site.  Zero live tenants
# refused.  The old figure was measuring dead slugs, not Personio throttling
# us, and it cost the fetcher a 15x slowdown for nothing.  Kept well under the
# measured ceiling anyway; override with PERSONIO_WORKERS / PERSONIO_RPS.
WORKERS = int(os.environ.get("PERSONIO_WORKERS") or 16)
RPS = float(os.environ.get("PERSONIO_RPS") or 12.0)
TIMEOUT = 45
TRIES = 6
# Wall-clock budget, added 2026-07-28.  The comment above sized this for the
# ~6,000 boards the old candidate list held; the Wayback slug sweep raised it
# to 8,197, and the overwhelming majority of those are retired tenants whose
# subdomain no longer resolves.  A dead slug is not cheap here: it costs 2
# hosts x TRIES retries with escalating sleeps, so the measured pace was 42
# boards in 3 hours — ~34h for the full list, sitting directly in front of
# run_all's build tail.  Boards not reached stay ABSENT from the state file
# and are retried by the next run.  0 disables the limit.
BUDGET_MIN = int(os.environ.get("PERSONIO_BUDGET_MIN") or 15)
MIN_FREE_GB = 5
STRIP = ("description", "keywords")

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_counts = {"live": 0, "empty": 0, "dead": 0, "failed": 0, "r429": 0}
_state = {}
_ledger = {}


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


def slow_down():
    with _rate_lock:
        _counts["r429"] += 1
        _pause_until[0] = time.time() + 30
        _interval[0] = min(_interval[0] * 1.5, 1.0)


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
    """-> (status, tld, jobs).  Tries .de then .com; the first host that
    answers 200 wins and is remembered in the state so later runs skip the
    other one."""
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    known = (_state.get(slug) or {}).get("tld")
    order = (known,) if known else HOSTS
    last = ""
    try:
        for tld in order:
            host = "%s.jobs.personio.%s" % (slug, tld)
            url = "https://%s/search.json" % host
            for attempt in range(TRIES):
                throttle()
                try:
                    r = s.get(url, timeout=TIMEOUT)
                    # A RETIRED tenant does not 404.  *.jobs.personio.{de,com}
                    # is a wildcard DNS record, so the name always resolves and
                    # the edge answers 307 -> https://personio.com — the
                    # marketing site, which rate-limits bots and returns 429.
                    # Read naively that looks exactly like "Personio is
                    # throttling us": the old code called slow_down() (a 30s
                    # GLOBAL pause plus a permanent 1.5x interval penalty) six
                    # times per host, twice per slug, then gave up with
                    # "failed" so the slug was retried forever on later runs.
                    # Measured cost: 45 boards in 13 minutes, i.e. ~68h for the
                    # 8.2k list, nearly all of it self-inflicted.  The
                    # redirect OFF the tenant host is the real death signal, so
                    # it is checked before any status code.
                    if r.history and urlparse(r.url).hostname != host:
                        last = "redirected off tenant host"
                        break                      # try the next tld
                    if r.status_code in (404, 410):
                        last = "404"
                        break                      # try the next tld
                    if r.status_code in (429, 418, 403):
                        slow_down()
                        last = "HTTP %d" % r.status_code
                    elif r.status_code == 200:
                        ctype = (r.headers.get("content-type") or "").lower()
                        if "json" not in ctype:
                            last = "non-json ctype"
                            break
                        try:
                            arr = r.json()
                        except ValueError:
                            last = "bad-json"
                        else:
                            if not isinstance(arr, list):
                                last = "not-a-list"
                                break
                            out = []
                            for j in arr:
                                if not isinstance(j, dict):
                                    continue
                                out.append({k: v for k, v in j.items()
                                            if k not in STRIP})
                            return ("live" if out else "empty"), tld, out
                    else:
                        last = "HTTP %d" % r.status_code
                except requests.RequestException as e:
                    last = type(e).__name__
                time.sleep(2 * (attempt + 1))
    finally:
        s.close()
    if last in ("404", "non-json ctype", "not-a-list",
                "redirected off tenant host"):
        return "dead", "", []
    raise RuntimeError("%s failed: %s" % (slug, last))


def main():
    if not os.path.exists(CAND_CSV):
        raise SystemExit("missing %s — run personio_discover_boards.py"
                         % CAND_CSV)
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
            not in ("live", "empty", "dead")]
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
    stopped_early = False
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    with open(OUT_JSONL, "a", encoding="utf-8") as out:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(fetch_board, c["slug"]): c for c in todo}
            for fut in cf.as_completed(futs):
                if deadline and time.time() > deadline:
                    stopped_early = True
                    for f in futs:
                        f.cancel()
                    break
                c = futs[fut]
                slug = c["slug"]
                try:
                    status, tld, jobs = fut.result()
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
                            _ledger[jid] = {"first_seen": TODAY}
                        out.write(json.dumps({
                            "slug": slug, "tld": tld,
                            "board_url": "https://%s.jobs.personio.%s"
                                         % (slug, tld),
                            "job": j,
                        }, ensure_ascii=False) + "\n")
                    out.flush()
                    total += len(jobs)
                    _state[slug] = {"status": status, "tld": tld,
                                    "fetched": len(jobs), "fetched_at": TODAY}
                    ndone += 1
                    save_json(STATE_JSON, _state)
                    if ndone % 100 == 0:
                        save_json(LEDGER_JSON, _ledger)
                        el = time.time() - t0
                        eta = el / ndone * (len(todo) - ndone) / 60
                        print("  %d/%d | live %d empty %d dead %d fail %d "
                              "| %d jobs | eta %.0fm"
                              % (ndone, len(todo), _counts["live"],
                                 _counts["empty"], _counts["dead"],
                                 _counts["failed"], total, eta), flush=True)
    save_json(LEDGER_JSON, _ledger)

    boards = [(s, v) for s, v in _state.items()
              if v.get("status") in ("live", "empty")]
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "board_url", "n_jobs", "status"])
        for s, v in sorted(boards):
            w.writerow([s, "https://%s.jobs.personio.%s" % (s, v.get("tld")
                                                            or "de"),
                        v.get("fetched", 0), v.get("status")])
    if _counts["r429"]:
        print("  rate-limit backoffs: %d" % _counts["r429"], flush=True)
    print("\nDone. %d job rows in %s; %d boards failed%s"
          % (total, OUT_JSONL, _counts["failed"],
             " (re-run to retry)" if _counts["failed"] else ""), flush=True)
    if stopped_early:
        # Exit 0 so run_all's supervisor proceeds to the build rather than
        # relaunching into the same budget 40 times over.
        print("BUDGET REACHED after %dmin: %d/%d boards settled, %d still "
              "pending for the next run.  Raise or remove with "
              "PERSONIO_BUDGET_MIN=<n> (0 = no limit)."
              % (BUDGET_MIN, ndone, len(todo), len(todo) - ndone), flush=True)
        raise SystemExit(0)
    raise SystemExit(1 if _counts["failed"] else 0)


if __name__ == "__main__":
    sys.exit(main())
