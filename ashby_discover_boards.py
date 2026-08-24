#!/usr/bin/env python3
"""
ashby_discover_boards.py
Discover every Ashby job-board slug we can find (mirrors
greenhouse_discover_boards.py). Source families:

  1. Wayback Machine CDX (paged, resumable) over three host patterns:
       jobs.ashbyhq.com/*
       api.ashbyhq.com/posting-api/job-board/*
       *.ashbyhq.com/*                (catch-all: embeds, other subdomains)
     Per-host collapse length: urlkey prefix must cover
     "com,ashbyhq,<host>)/<fixed path>" plus ~18 chars of slug.
  2. getro_ats_slugs.json["ashby"] (slug -> job count, pre-extracted).
  3. grep of getro_all_jobs.jsonl for ashby URLs.
  4. grep of all_jobs_master.csv for ashby URLs.
  5. grep of contract_norm/ats_all_ashby.jsonl (the older contract-only pull).

Learned 2026-07-23 (probed against the live API):
  - Slugs are CASE-INSENSITIVE (COSTANOAVC == costanoavc) -> lowercase.
  - A dead slug returns a bare 404 "Not Found" (not JSON).
  - The posting API returns ALL jobs in one response; no pagination.
  - ?includeCompensation=true adds structured salary (min/max/currency/
    interval) — Ashby is the only one of our ATS pipes with real pay data.

Resumable: per-(host,page) state in ashby_discovery_state.json; raw CDX rows
append to ashby_wayback_urls.txt. Output: ashby_candidates.csv
(slug, sources). Exits non-zero while CDX pages are still missing.
"""

import csv
import json
import os
import random
import re
import subprocess
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import slug_common as sc          # validate(): CDX 200-with-HTML discriminator

BASE = os.path.dirname(os.path.abspath(__file__))
STATE_JSON = os.path.join(BASE, "ashby_discovery_state.json")
RAW_TXT = os.path.join(BASE, "ashby_wayback_urls.txt")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")
GETRO_SLUGS = os.path.join(BASE, "getro_ats_slugs.json")
ALLJOBS_CSV = os.path.join(BASE, "all_jobs_master.csv")
ATS_JSONL = os.path.join(BASE, "contract_norm", "ats_all_ashby.jsonl")
OUT_CSV = os.path.join(BASE, "ashby_candidates.csv")

CDX = "https://web.archive.org/cdx/search/cdx"
HOSTS = [  # (tag, url pattern, collapse)
    ("jobs", "jobs.ashbyhq.com/*", "urlkey:36"),
    ("api", "api.ashbyhq.com/posting-api/job-board/*", "urlkey:57"),
    ("any", "*.ashbyhq.com/*", "urlkey:50"),
]
WORKERS = 6
TRIES = 6
TIMEOUT = 150
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Path segments that are never a board slug.
JUNK = {"", "api", "posting-api", "job-board", "embed", "favicon.ico",
        "robots.txt", "sitemap.xml", "assets", "static", "img", "images",
        "css", "js", "fonts", "_next", "undefined", "null", "error",
        "application", "jobs", "job", "login", "signup", "dashboard",
        "settings", "admin", "auth", "oauth", "logout", "index.html"}

_lock = threading.Lock()
_state = {}


def load_state():
    global _state
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, encoding="utf-8") as f:
            _state = json.load(f)
    _state.setdefault("pages_done", {})
    _state.setdefault("numpages", {})


def save_state():
    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_state, f)
    os.replace(tmp, STATE_JSON)


def cdx_get(session, params, expect="urls"):
    # The body is validated, not just the status code.  web.archive.org serves
    # nginx 503/504 error pages WITH HTTP 200; returning that text made the
    # caller write HTML into the URL dump and mark the page permanently done,
    # which is why published slug baselines never reproduced.  slug_common
    # already owns the marker list, so reuse it rather than fork it.
    last = None
    for attempt in range(TRIES):
        try:
            r = session.get(CDX, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                ok, why = sc.validate(r.text, expect)
                if ok:
                    return r.text
                last = "bad-body: " + why
            else:
                last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = f"{type(e).__name__}"
        time.sleep(min(15 * (2 ** attempt), 240) + random.uniform(0, 5))
    raise RuntimeError(f"CDX failed after {TRIES} tries: {last}")


def numpages(session, pattern):
    if pattern in _state["numpages"]:
        return _state["numpages"][pattern]
    n = int(cdx_get(session, {"url": pattern, "showNumPages": "true"},
                    expect="int").strip() or 0)
    with _lock:
        _state["numpages"][pattern] = n
        save_state()
    return n


def fetch_page(tag, pattern, collapse, page):
    session = requests.Session()
    session.headers["User-Agent"] = UA
    text = cdx_get(session, {
        "url": pattern, "fl": "original", "collapse": collapse,
        "page": str(page),
    })
    lines = [ln for ln in text.splitlines() if ln.strip()]
    with _lock:
        with open(RAW_TXT, "a", encoding="utf-8") as f:
            for ln in lines:
                f.write(f"{tag}\t{ln}\n")
        _state["pages_done"][f"{tag}:{page}"] = len(lines)
        save_state()
    return len(lines)


def crawl_wayback():
    session = requests.Session()
    session.headers["User-Agent"] = UA
    jobs = []
    for tag, pattern, collapse in HOSTS:
        n = numpages(session, pattern)
        print(f"{tag}: {n} pages", flush=True)
        for p in range(n):
            if f"{tag}:{p}" not in _state["pages_done"]:
                jobs.append((tag, pattern, collapse, p))
    random.shuffle(jobs)  # spread hosts across workers
    print(f"{len(jobs)} CDX pages to fetch", flush=True)
    if not jobs:
        return
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_page, t, pat, col, p): (t, p)
                for t, pat, col, p in jobs}
        for fut in as_completed(futs):
            tag, p = futs[fut]
            try:
                fut.result()
            except Exception as e:  # noqa: BLE001 — page retried on re-run
                print(f"  PAGE FAIL {tag}:{p}: {e}", flush=True)
                continue
            done += 1
            if done % 10 == 0:
                rate = (time.time() - t0) / done
                eta = rate * (len(jobs) - done) / 60
                print(f"  {done}/{len(jobs)} pages, eta {eta:.0f}m",
                      flush=True)
    missing = sum(1 for t, _pat, _c, p in jobs
                  if f"{t}:{p}" not in _state["pages_done"])
    if missing:
        raise SystemExit(f"{missing} CDX pages still missing — re-run")


# jobs.ashbyhq.com/<slug>[/<uuid>[/application]]
PATH_RE = re.compile(r"^https?://jobs\.ashbyhq\.com/([^/?#]+)", re.I)
# api.ashbyhq.com/posting-api/job-board/<slug>
API_RE = re.compile(
    r"^https?://api\.ashbyhq\.com/posting-api/job-board/([^/?#]+)", re.I)
# embeds sometimes carry ?jobBoardName=<slug>
NAME_RE = re.compile(r"[?&]jobBoardName=([^&#]+)", re.I)
SLUG_OK = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,79}$")
# A bare UUID is a posting id, not a board slug.
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")


def clean(seg):
    seg = urllib.parse.unquote(urllib.parse.unquote(seg)).strip().lower()
    if seg in JUNK or UUID_RE.match(seg) or not SLUG_OK.match(seg):
        return None
    return seg


def slugs_of(url):
    """Yield every board slug found in one URL."""
    url = url.strip()
    for rx in (PATH_RE, API_RE):
        m = rx.match(url)
        if m:
            s = clean(m.group(1))
            if s:
                yield s
    if "ashbyhq" in url.lower():
        for m in NAME_RE.finditer(url):
            s = clean(m.group(1))
            if s:
                yield s


GREP_PAT = (r"jobs\.ashbyhq\.com/[^\"'[:space:]\\]+"
            r"|api\.ashbyhq\.com/posting-api/job-board/[A-Za-z0-9_%.-]+")


def grep_file(path, src, add):
    if not os.path.exists(path):
        print(f"  ({src}: {os.path.basename(path)} missing, skipped)",
              flush=True)
        return
    with open(path, "rb") as fh:
        out = subprocess.run(["grep", "-aoE", GREP_PAT], stdin=fh,
                             capture_output=True, text=True).stdout
    hits = set(out.splitlines())
    for u in hits:
        for s in slugs_of("https://" + u if not u.startswith("http") else u):
            add(s, src)
    print(f"  {src}: {len(hits)} url hits", flush=True)


def main():
    load_state()
    crawl_wayback()

    cand = {}  # slug -> set(sources)

    def add(slug, src):
        cand.setdefault(slug, set()).add(src)

    n_raw = 0
    if os.path.exists(RAW_TXT):
        with open(RAW_TXT, encoding="utf-8") as f:
            for line in f:
                n_raw += 1
                _tag, _, url = line.partition("\t")
                for s in slugs_of(url):
                    add(s, "wayback")

    # pre-extracted ashby slugs from the getro build
    if os.path.exists(GETRO_SLUGS):
        try:
            with open(GETRO_SLUGS, encoding="utf-8") as f:
                slugs = json.load(f).get("ashby", {})
            for s in slugs:
                t = clean(str(s))
                if t:
                    add(t, "getro")
            print(f"  getro-slugs: {len(slugs)} slugs", flush=True)
        except (ValueError, OSError) as e:
            print(f"  getro-slugs failed: {e}", flush=True)

    grep_file(GETRO_JSONL, "getro", add)
    grep_file(ALLJOBS_CSV, "alljobs", add)
    grep_file(ATS_JSONL, "ats", add)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "sources"])
        for slug in sorted(cand):
            w.writerow([slug, "|".join(sorted(cand[slug]))])

    print(f"{n_raw} raw wayback rows -> {len(cand)} candidate slugs "
          f"-> {OUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
