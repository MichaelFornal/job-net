#!/usr/bin/env python3
"""
lever_discover_boards.py
Discover every Lever job-board slug we can find, from two source families:

  1. Wayback Machine CDX (paged, resumable) over four hosts:
       jobs.lever.co/*  jobs.eu.lever.co/*
       api.lever.co/v0/postings/*  api.eu.lever.co/v0/postings/*
     Learned 2026-07-19:
       - regex `filter=` must be sent URL-encoded (--data-urlencode style)
       - `collapse=urlkey:40` crushes per-posting rows to ~1 row per slug
         (urlkey prefix "co,lever,jobs)/<slug>/..." — first 40 chars) while
         still surfacing slugs whose board ROOT page was never archived
       - ~25s per page regardless of rows; ~380 pages total -> thread pool
       - Common Crawl is useless here (only robots.txt rows; CCBot blocked)
  2. Our own getro_all_jobs.jsonl: apply URLs pointing at lever.co
     (pre-extracted by grep to lever_from_getro_raw.txt in the scratchpad,
      regenerated here directly if that file is missing).

IMPORTANT: Lever slugs are CASE-SENSITIVE (InfrastructureandCapitalProjects
is live, its lowercase 404s) — original casing is preserved everywhere.

Resumable: per-(host,page) state in lever_discovery_state.json; raw CDX rows
append to lever_wayback_urls.txt. Output: lever_candidates.csv
(slug, eu_hint, sources).
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

import requests

import slug_common as sc          # validate(): CDX 200-with-HTML discriminator

BASE = os.path.dirname(os.path.abspath(__file__))
STATE_JSON = os.path.join(BASE, "lever_discovery_state.json")
RAW_TXT = os.path.join(BASE, "lever_wayback_urls.txt")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")
OUT_CSV = os.path.join(BASE, "lever_candidates.csv")

CDX = "https://web.archive.org/cdx/search/cdx"
HOSTS = [  # (tag, url pattern, eu_hint)
    ("us-jobs", "jobs.lever.co/*", False),
    ("eu-jobs", "jobs.eu.lever.co/*", True),
    ("us-api", "api.lever.co/v0/postings/*", False),
    ("eu-api", "api.eu.lever.co/v0/postings/*", True),
]
WORKERS = 6
TRIES = 6
TIMEOUT = 150
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

JUNK = {"favicon.ico", "robots.txt", "sitemap.xml", "$company", "undefined",
        "null", "apply", "login", "assets", "img", "css", "js", "v0",
        "postings", "posting", "api", "jobs", ""}

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
    key = pattern
    if key in _state["numpages"]:
        return _state["numpages"][key]
    n = int(cdx_get(session, {"url": pattern, "showNumPages": "true"},
                    expect="int").strip() or 0)
    with _lock:
        _state["numpages"][key] = n
        save_state()
    return n


def fetch_page(tag, pattern, page):
    session = requests.Session()
    session.headers["User-Agent"] = UA
    text = cdx_get(session, {
        "url": pattern, "fl": "original", "collapse": "urlkey:40",
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
    for tag, pattern, _eu in HOSTS:
        n = numpages(session, pattern)
        print(f"{tag}: {n} pages", flush=True)
        for p in range(n):
            if f"{tag}:{p}" not in _state["pages_done"]:
                jobs.append((tag, pattern, p))
    random.shuffle(jobs)  # spread hosts across workers
    print(f"{len(jobs)} CDX pages to fetch", flush=True)
    if not jobs:
        return
    done = 0
    t0 = time.time()
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_page, t, pat, p): (t, p)
                for t, pat, p in jobs}
        for fut in as_completed(futs):
            tag, p = futs[fut]
            try:
                nrows = fut.result()
            except Exception as e:
                print(f"  PAGE FAIL {tag}:{p}: {e}", flush=True)
                continue
            done += 1
            if done % 20 == 0:
                rate = (time.time() - t0) / done
                eta = rate * (len(jobs) - done) / 60
                print(f"  {done}/{len(jobs)} pages, eta {eta:.0f}m",
                      flush=True)
    missing = sum(1 for t, _pat, p in jobs
                  if f"{t}:{p}" not in _state["pages_done"])
    if missing:
        raise SystemExit(f"{missing} CDX pages still missing — re-run")


SEG_RE = re.compile(r"^https?://(?:jobs|api)\.(eu\.)?lever\.co/(.*)$")


def slug_of(url):
    """(slug, eu) from an archived URL, or (None, False)."""
    m = SEG_RE.match(url.strip())
    if not m:
        return None, False
    eu = bool(m.group(1))
    path = m.group(2)
    if path.startswith("v0/postings/"):
        path = path[len("v0/postings/"):]
    seg = path.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    seg = urllib.parse.unquote(seg).strip()
    if (not seg or seg in JUNK or len(seg) > 80
            or seg.lower() in ("favicon.ico", "robots.txt")
            or any(c in seg for c in "/\\<>\"' \t")):
        return None, eu
    return seg, eu


def main():
    load_state()
    crawl_wayback()

    cand = {}  # slug (exact case) -> {"eu": bool-any, "sources": set}

    def add(slug, eu, src):
        e = cand.setdefault(slug, {"eu": False, "sources": set()})
        e["eu"] = e["eu"] or eu
        e["sources"].add(src)

    n_raw = 0
    with open(RAW_TXT, encoding="utf-8") as f:
        for line in f:
            n_raw += 1
            tag, _, url = line.partition("\t")
            slug, eu = slug_of(url)
            if slug:
                add(slug, eu or tag.startswith("eu"), "wayback")

    # Getro dump: use pre-grepped file if present, else grep now
    scratch = os.environ.get("LEVER_SCRATCH", "")
    pre = os.path.join(scratch, "lever_from_getro_raw.txt") if scratch else ""
    if pre and os.path.exists(pre):
        with open(pre, encoding="utf-8") as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2:
                    slug, eu = slug_of("https://" + parts[1])
                    if slug:
                        add(slug, eu, "getro")
    elif os.path.exists(GETRO_JSONL):
        out = subprocess.run(
            ["grep", "-aoE",
             r"(jobs|api)\.(eu\.)?lever\.co/(v0/postings/)?[A-Za-z0-9._-]+"],
            stdin=open(GETRO_JSONL), capture_output=True, text=True).stdout
        for u in set(out.splitlines()):
            slug, eu = slug_of("https://" + u)
            if slug:
                add(slug, eu, "getro")

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "eu_hint", "sources"])
        for slug in sorted(cand, key=str.lower):
            e = cand[slug]
            w.writerow([slug, int(e["eu"]), "|".join(sorted(e["sources"]))])

    n_ci = len({s.lower() for s in cand})
    print(f"{n_raw} raw wayback rows -> {len(cand)} exact-case candidates "
          f"({n_ci} case-insensitive) -> {OUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
