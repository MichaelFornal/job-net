#!/usr/bin/env python3
"""
greenhouse_discover_boards.py
Discover every Greenhouse job-board token we can find (mirrors
lever_discover_boards.py). Source families:

  1. Wayback Machine CDX (paged, resumable) over eight host patterns:
       boards.greenhouse.io/*        job-boards.greenhouse.io/*
       boards.eu.greenhouse.io/*     job-boards.eu.greenhouse.io/*
       boards-api.greenhouse.io/v1/boards/*
       boards-api.eu.greenhouse.io/v1/boards/*
       api.greenhouse.io/v1/boards/*
       boards.greenhouse.io/embed/*   (?for=<token> query URLs)
     Per-host collapse length: urlkey prefix must cover
     "io,greenhouse,<host>)/<fixed path>" plus ~18 chars of token.
  2. getro_ats_slugs.json["greenhouse"] (slug -> job count, pre-extracted).
  3. grep of getro_all_jobs.jsonl for greenhouse URLs.
  4. grep of all_jobs_master.csv for greenhouse URLs.

Learned 2026-07-21 (differs from Lever!):
  - Tokens are CASE-INSENSITIVE (STRIPE == stripe) -> lowercase everywhere.
  - boards-api.eu.greenhouse.io does NOT resolve; the single US API host
    boards-api.greenhouse.io serves EU-hosted boards too (abbyy test).
  - Jobs endpoint returns ALL jobs in one response (meta.total == len).

Resumable: per-(host,page) state in greenhouse_discovery_state.json; raw CDX
rows append to greenhouse_wayback_urls.txt. Output: greenhouse_candidates.csv
(token, eu_hint, sources). Exits non-zero while CDX pages are still missing.
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
STATE_JSON = os.path.join(BASE, "greenhouse_discovery_state.json")
RAW_TXT = os.path.join(BASE, "greenhouse_wayback_urls.txt")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")
GETRO_SLUGS = os.path.join(BASE, "getro_ats_slugs.json")
ALLJOBS_CSV = os.path.join(BASE, "all_jobs_master.csv")
OUT_CSV = os.path.join(BASE, "greenhouse_candidates.csv")

CDX = "https://web.archive.org/cdx/search/cdx"
HOSTS = [  # (tag, url pattern, eu_hint, collapse)
    ("us-boards", "boards.greenhouse.io/*", False, "urlkey:40"),
    ("us-jobboards", "job-boards.greenhouse.io/*", False, "urlkey:44"),
    ("eu-boards", "boards.eu.greenhouse.io/*", True, "urlkey:44"),
    ("eu-jobboards", "job-boards.eu.greenhouse.io/*", True, "urlkey:48"),
    ("us-api", "boards-api.greenhouse.io/v1/boards/*", False, "urlkey:54"),
    ("eu-api", "boards-api.eu.greenhouse.io/v1/boards/*", True, "urlkey:57"),
    ("us-oldapi", "api.greenhouse.io/v1/boards/*", False, "urlkey:47"),
    ("us-embed", "boards.greenhouse.io/embed/*", False, "urlkey:64"),
]
WORKERS = 6
TRIES = 6
TIMEOUT = 150
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

JUNK = {"embed", "favicon.ico", "robots.txt", "sitemap.xml", "assets",
        "static", "img", "images", "css", "js", "fonts", "api", "v1",
        "boards", "undefined", "null", "error", "$company", ""}

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
    for tag, pattern, _eu, collapse in HOSTS:
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
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_page, t, pat, col, p): (t, p)
                for t, pat, col, p in jobs}
        for fut in as_completed(futs):
            tag, p = futs[fut]
            try:
                fut.result()
            except Exception as e:
                print(f"  PAGE FAIL {tag}:{p}: {e}", flush=True)
                continue
            done += 1
            if done % 20 == 0:
                rate = (time.time() - t0) / done
                eta = rate * (len(jobs) - done) / 60
                print(f"  {done}/{len(jobs)} pages, eta {eta:.0f}m",
                      flush=True)
    missing = sum(1 for t, _pat, _c, p in jobs
                  if f"{t}:{p}" not in _state["pages_done"])
    if missing:
        raise SystemExit(f"{missing} CDX pages still missing — re-run")


PATH_RE = re.compile(
    r"^https?://(?:boards|job-boards)\.(eu\.)?greenhouse\.io/([^/?#]+)", re.I)
API_RE = re.compile(
    r"^https?://(?:boards-api|api)\.(eu\.)?greenhouse\.io/v1/boards/"
    r"([^/?#]+)", re.I)
FOR_RE = re.compile(r"[?&]for=([^&#]+)", re.I)
TOKEN_OK = re.compile(r"^[a-z0-9][a-z0-9_-]{0,79}$")


def clean(seg):
    seg = urllib.parse.unquote(urllib.parse.unquote(seg)).strip().lower()
    if seg in JUNK or not TOKEN_OK.match(seg):
        return None
    return seg


def tokens_of(url):
    """Yield (token, eu) pairs found in one URL."""
    url = url.strip()
    m = PATH_RE.match(url)
    if m:
        t = clean(m.group(2))
        if t:
            yield t, bool(m.group(1))
    m = API_RE.match(url)
    if m:
        t = clean(m.group(2))
        if t:
            yield t, bool(m.group(1))
    if "greenhouse" in url:
        for m in FOR_RE.finditer(url):
            t = clean(m.group(1))
            if t:
                yield t, ".eu.greenhouse" in url


def grep_file(path, src, add):
    if not os.path.exists(path):
        print(f"  ({src}: {os.path.basename(path)} missing, skipped)",
              flush=True)
        return
    pat = (r"(boards|job-boards)\.(eu\.)?greenhouse\.io/[^\"'[:space:]\\]+"
           r"|(boards-api|api)\.(eu\.)?greenhouse\.io/v1/boards/[A-Za-z0-9_%.-]+")
    with open(path, "rb") as fh:
        out = subprocess.run(["grep", "-aoE", pat], stdin=fh,
                             capture_output=True, text=True).stdout
    n = 0
    for u in set(out.splitlines()):
        for tok, eu in tokens_of("https://" + u if not
                                 u.startswith("http") else u):
            add(tok, eu, src)
            n += 1
    print(f"  {src}: {len(set(out.splitlines()))} url hits", flush=True)


def main():
    load_state()
    crawl_wayback()

    cand = {}  # token -> {"eu": bool-any, "sources": set}

    def add(token, eu, src):
        e = cand.setdefault(token, {"eu": False, "sources": set()})
        e["eu"] = e["eu"] or eu
        e["sources"].add(src)

    n_raw = 0
    with open(RAW_TXT, encoding="utf-8") as f:
        for line in f:
            n_raw += 1
            tag, _, url = line.partition("\t")
            for tok, eu in tokens_of(url):
                add(tok, eu or tag.startswith("eu"), "wayback")

    # pre-extracted greenhouse slugs from the getro build
    if os.path.exists(GETRO_SLUGS):
        try:
            with open(GETRO_SLUGS, encoding="utf-8") as f:
                slugs = json.load(f).get("greenhouse", {})
            for s in slugs:
                t = clean(str(s))
                if t:
                    add(t, False, "getro")
            print(f"  getro-slugs: {len(slugs)} tokens", flush=True)
        except (ValueError, OSError) as e:
            print(f"  getro-slugs failed: {e}", flush=True)

    grep_file(GETRO_JSONL, "getro", add)
    grep_file(ALLJOBS_CSV, "alljobs", add)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["token", "eu_hint", "sources"])
        for token in sorted(cand):
            e = cand[token]
            w.writerow([token, int(e["eu"]), "|".join(sorted(e["sources"]))])

    print(f"{n_raw} raw wayback rows -> {len(cand)} candidate tokens "
          f"-> {OUT_CSV}", flush=True)


if __name__ == "__main__":
    main()
