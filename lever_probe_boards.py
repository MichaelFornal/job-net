#!/usr/bin/env python3
"""
lever_probe_boards.py
Probe every candidate slug from lever_candidates.csv against the public
Lever postings API and classify live / dead.

API (learned 2026-07-19):
    GET https://api.lever.co/v0/postings/{slug}?mode=json&limit=1
    GET https://api.eu.lever.co/v0/postings/{slug}?mode=json&limit=1
    200 + JSON array  -> live board ([] = live, zero postings)
    404 {"ok":false,"error":"Document not found"} -> no board in that region
    Slugs are CASE-SENSITIVE. Spaces can occur in slugs (%20...) — quote them.

For live boards the hosted page <title> is the company display name
(e.g. jobs.lever.co/palantir -> "Palantir Technologies").

eu_hint=1 candidates (seen on an EU host) probe EU first AND always probe
both regions; others probe US first, EU only on 404.

Resumable: rows append to lever_probe_results.csv as they finish; on restart,
slugs already classified (non-error) are skipped, error rows retried.
Final output: lever_job_boards.csv (live boards only, sorted).
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
CAND_CSV = os.path.join(BASE, "lever_candidates.csv")
RESULTS_CSV = os.path.join(BASE, "lever_probe_results.csv")
BOARDS_CSV = os.path.join(BASE, "lever_job_boards.csv")

WORKERS = 16
RPS = 14.0
TIMEOUT = 25
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

API = {"us": "https://api.lever.co/v0/postings/",
       "eu": "https://api.eu.lever.co/v0/postings/"}
SITE = {"us": "https://jobs.lever.co/",
        "eu": "https://jobs.eu.lever.co/"}

RES_COLS = ["slug", "region", "status", "company", "board_url", "api_url",
            "eu_hint", "sources", "detail"]

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_counts = {"live": 0, "dead": 0, "error": 0, "req": 0, "r429": 0}

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)
TITLE_STRIP = [
    r"\s*[-–—|·]\s*current openings\s*$", r"\s*[-–—|·]\s*careers?\s*$",
    r"\s*[-–—|·]\s*jobs?\s*$", r"\s*[-–—|·]\s*job board\s*$",
    r"^\s*careers?\s+at\s+", r"^\s*jobs?\s+at\s+",
    r"^\s*current openings\s*[-–—|·]\s*", r"\s+careers?\s*$",
]


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


def api_probe(session, region, slug):
    """-> ("live", n_sample) | ("dead", "") | raises after retries."""
    url = API[region] + urllib.parse.quote(slug, safe="")
    last = None
    for attempt in range(6):
        throttle()
        _counts["req"] += 1
        try:
            r = session.get(url, params={"mode": "json", "limit": 1},
                            timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    return "dead", "non-json-200"
                return ("live", len(data)) if isinstance(data, list) \
                    else ("dead", "non-array")
            if r.status_code == 404:
                return "dead", ""
            last = f"HTTP {r.status_code}"
            if r.status_code in (429, 418, 403):
                slow_down()
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last or "unknown")


def board_title(session, region, slug):
    try:
        throttle()
        r = session.get(SITE[region] + urllib.parse.quote(slug, safe=""),
                        timeout=TIMEOUT, stream=True)
        chunk = next(r.iter_content(65536, decode_unicode=False), b"") or b""
        r.close()
        m = TITLE_RE.search(chunk.decode("utf-8", "replace"))
        if not m:
            return ""
        t = html.unescape(m.group(1)).strip()
        for pat in TITLE_STRIP:
            t = re.sub(pat, "", t, flags=re.I).strip()
        if t.lower() in ("lever", "jobs", "careers", "current openings", ""):
            return ""
        if re.search(r"not found|404|\berror\b|just a moment|attention "
                     r"required|access denied|page unavailable", t, re.I):
            return ""  # live API but hidden/unlisted hosted page
        return re.sub(r"\s+", " ", t)[:120]
    except requests.RequestException:
        return ""


def probe_slug(cand):
    slug, eu_hint, sources = cand["slug"], cand["eu_hint"], cand["sources"]
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    order = ["eu", "us"] if eu_hint == "1" else ["us", "eu"]
    both = eu_hint == "1"
    rows = []
    err = None
    for i, region in enumerate(order):
        if i == 1 and rows and not both:
            break  # first region live, no need for fallback
        try:
            status, detail = api_probe(session, region, slug)
        except RuntimeError as e:
            err = str(e)
            continue
        if status == "live":
            name = board_title(session, region, slug) or slug
            rows.append([slug, region, "live", name,
                         SITE[region] + urllib.parse.quote(slug, safe=""),
                         API[region] + urllib.parse.quote(slug, safe="")
                         + "?mode=json", eu_hint, sources, detail])
    if not rows:
        rows.append([slug, "", "error" if err else "dead", "", "", "",
                     eu_hint, sources, err or ""])
    return rows


def main():
    with open(CAND_CSV, encoding="utf-8") as f:
        cands = list(csv.DictReader(f))

    done = set()
    keep_rows = []
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["status"] in ("live", "dead"):
                    done.add(row["slug"])
                    keep_rows.append([row.get(c, "") for c in RES_COLS])
    todo = [c for c in cands if c["slug"] not in done]
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
        futs = {ex.submit(probe_slug, c): c["slug"] for c in todo}
        for fut in as_completed(futs):
            slug = futs[fut]
            try:
                rows = fut.result()
            except Exception as e:  # unexpected — record as error row
                rows = [[slug, "", "error", "", "", "", "", "",
                         f"unexpected {type(e).__name__}: {e}"]]
            with _lock:
                for r in rows:
                    w.writerow(r)
                    _counts[r[2]] = _counts.get(r[2], 0) + 1
                out.flush()
                ndone += 1
                if ndone % 200 == 0:
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
                live[(row["slug"], row["region"])] = row
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "region", "company", "board_url", "api_url",
                    "sources"])
        for (slug, region) in sorted(live, key=lambda k: (k[0].lower(), k[1])):
            r = live[(slug, region)]
            w.writerow([slug, region, r["company"], r["board_url"],
                        r["api_url"], r["sources"]])
    print(f"\nDone: {len(live)} live boards -> {BOARDS_CSV} "
          f"({_counts['error']} errors — re-run to retry)", flush=True)
    # Matches greenhouse_probe_boards.py.  Without this the script always
    # exited 0, so run_all.py's 40-attempt supervisor never retried a board
    # that errored — the "re-run to retry" in the line above was a lie.
    raise SystemExit(1 if _counts["error"] else 0)


if __name__ == "__main__":
    main()
