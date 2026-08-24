#!/usr/bin/env python3
"""
greenhouse_probe_boards.py
Probe every candidate token from greenhouse_candidates.csv against the public
Greenhouse Job Board API and classify live / dead (mirrors
lever_probe_boards.py).

API (learned 2026-07-21):
    GET https://boards-api.greenhouse.io/v1/boards/{token}
    200 {"name": "Stripe", "content": "..."}  -> live, name = company display
    404 {"status":404,"error":"Job not found"} -> dead
    Tokens are CASE-INSENSITIVE (unlike Lever). The single US API host serves
    EU-hosted boards too (boards-api.eu.greenhouse.io does not resolve), so
    one probe per token — no per-region probing.

Resumable: rows append to greenhouse_probe_results.csv as they finish; on
restart, tokens already classified (non-error) are skipped, error rows
retried. Final output: greenhouse_job_boards.csv (live boards only, sorted).
Exits non-zero while error rows remain, so a supervisor loop retries them.
"""

import csv
import html
import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
CAND_CSV = os.path.join(BASE, "greenhouse_candidates.csv")
RESULTS_CSV = os.path.join(BASE, "greenhouse_probe_results.csv")
BOARDS_CSV = os.path.join(BASE, "greenhouse_job_boards.csv")

WORKERS = 16
RPS = 10.0
TIMEOUT = 25
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

API = "https://boards-api.greenhouse.io/v1/boards/"
SITE = "https://job-boards.greenhouse.io/"

RES_COLS = ["token", "status", "company", "board_url", "api_url",
            "eu_hint", "sources", "detail"]

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_counts = {"live": 0, "dead": 0, "error": 0, "req": 0, "r429": 0}

TAG_RE = re.compile(r"<[^>]+>")


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
        _pause_until[0] = time.time() + 20
        _interval[0] = min(_interval[0] * 1.5, 0.5)


def api_probe(session, token):
    """-> ("live", name) | ("dead", "") | raises after retries."""
    url = API + urllib.parse.quote(token, safe="")
    last = None
    for attempt in range(6):
        throttle()
        _counts["req"] += 1
        try:
            r = session.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    return "dead", "non-json-200"
                if isinstance(data, dict) and "name" in data:
                    name = html.unescape(
                        TAG_RE.sub("", str(data.get("name") or ""))).strip()
                    return "live", re.sub(r"\s+", " ", name)[:120]
                return "dead", "no-name-field"
            if r.status_code == 404:
                return "dead", ""
            last = f"HTTP {r.status_code}"
            if r.status_code in (429, 418, 403):
                slow_down()
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last or "unknown")


def probe_token(cand):
    token, eu_hint, sources = cand["token"], cand["eu_hint"], cand["sources"]
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    try:
        status, detail = api_probe(session, token)
    except RuntimeError as e:
        return [token, "error", "", "", "", eu_hint, sources, str(e)]
    if status == "live":
        return [token, "live", detail or token, SITE + token,
                API + token + "/jobs", eu_hint, sources, ""]
    return [token, "dead", "", "", "", eu_hint, sources, detail]


def main():
    with open(CAND_CSV, encoding="utf-8") as f:
        cands = list(csv.DictReader(f))

    done = set()
    keep_rows = []
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["status"] in ("live", "dead"):
                    done.add(row["token"])
                    keep_rows.append([row.get(c, "") for c in RES_COLS])
    todo = [c for c in cands if c["token"] not in done]
    print(f"{len(cands)} candidates, {len(done)} already classified, "
          f"{len(todo)} to probe", flush=True)

    # rewrite results file with only the kept (non-error) rows
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(RES_COLS)
        w.writerows(keep_rows)

    out = open(RESULTS_CSV, "a", newline="", encoding="utf-8")
    w = csv.writer(out)
    t0 = time.time()
    ndone = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(probe_token, c): c["token"] for c in todo}
        for fut in as_completed(futs):
            token = futs[fut]
            try:
                row = fut.result()
            except Exception as e:  # unexpected — record as error row
                row = [token, "error", "", "", "", "", "",
                       f"unexpected {type(e).__name__}: {e}"]
            with _lock:
                w.writerow(row)
                _counts[row[1]] = _counts.get(row[1], 0) + 1
                out.flush()
                ndone += 1
                if ndone % 500 == 0:
                    el = time.time() - t0
                    eta = el / ndone * (len(todo) - ndone) / 60
                    print(f"  {ndone}/{len(todo)}  live {_counts['live']} "
                          f"dead {_counts['dead']} err {_counts['error']} "
                          f"| {_counts['req'] / el:.1f} rps "
                          f"(429s: {_counts['r429']}) | eta {eta:.0f}m",
                          flush=True)
    out.close()

    # final live-boards CSV
    live = {}
    with open(RESULTS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] == "live":
                live[row["token"]] = row
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["token", "company", "board_url", "api_url", "eu_hint",
                    "sources"])
        for token in sorted(live):
            r = live[token]
            w.writerow([token, r["company"], r["board_url"], r["api_url"],
                        r["eu_hint"], r["sources"]])
    print(f"\nDone: {len(live)} live boards -> {BOARDS_CSV} "
          f"({_counts['error']} errors — re-run to retry)", flush=True)
    raise SystemExit(1 if _counts["error"] else 0)


if __name__ == "__main__":
    main()
