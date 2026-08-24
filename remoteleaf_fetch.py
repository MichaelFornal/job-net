#!/usr/bin/env python3
"""
remoteleaf_fetch.py — harvest remoteleaf.com into contract_norm/remoteleaf.jsonl.

WHY THIS SOURCE
  Remote Leaf is a paid curation service, but the JOB PAGES ARE NOT GATED and
  every one of them renders the employer's DIRECT outbound ATS link (greenhouse,
  workable, lever, ashby, company career sites carrying ?gh_jid=...).  That link
  is the whole point of the pipe: the aggregator's own /company/<c>/<slug>/ URL
  would be useless for the canonical-ATS dedupe, the outbound URL is not.

SHAPE
  1. GET /sitemap.xml            -> a <sitemapindex>.  The SEVEN job partitions
                                    are sitemap-jobs-{united-states,europe,
                                    united-kingdom,canada,australia,worldwide,
                                    other-locations}.xml.  57,967 <loc> measured
                                    2026-07-28, matching the advertised 57,969.
  2. GET each partition sitemap  -> one flat <urlset>, no depth ceiling, no
                                    nested index.  <lastmod> is a *sitemap*
                                    stamp (all "today"), NOT the posting date —
                                    do not use it as `posted`.
  3. GET each job page           -> HTML.  It DOES carry a full schema.org
                                    JobPosting JSON-LD (title, datePosted,
                                    hiringOrganization, employmentType,
                                    jobLocationType, applicantLocationRequire-
                                    ments, baseSalary, identifier) plus a
                                    BreadcrumbList.  The apply URL is NOT in the
                                    JSON-LD; it is the <a href> right after the
                                    "<!-- Apply Button -->" comment.

TRAPS MEASURED HERE
  * /jobs 301s, /feed and /wp-json answer 404 with a 21 KB HTML body, /jobs/feed
    answers 200 with a 399 KB HTML shell, and /sitemap_index.xml 404s.  There is
    no feed — /sitemap.xml plus HTML parsing is the only surface.
  * A dead/unknown job path answers a REAL 404 (verified: /company/stripe/
    no-such-job-xyz-123/ -> HTTP 404, 21 KB body).  So status alone separates
    dead from live here — but the body is still validated, because a 200 that
    carries no JobPosting JSON-LD is a marketing/paywall shell, not a posting,
    and must NOT be recorded as "this job is gone".
  * A transient failure (timeout, 5xx, 429, connection reset) leaves the URL
    OUT of the done-ledger so a re-run retries it.  Only a parsed row or a hard
    404 marks a URL done.
  * applicantLocationRequirements is frequently a 50-country list.  It is
    compacted, but "United States" is hoisted to the front when present so the
    downstream US/MA filters still see it.
  * THE SITEMAP COUNT IS NOT A JOB COUNT.  A posting open to several countries
    gets one URL per country (…-mexico/, …-argentina/, …-brazil/ …) and every
    one of those pages renders the SAME JobPosting — same identifier, same
    outbound apply link.  Measured on the first 8,987 rows: 5,815 distinct
    postings, i.e. the 57,967 sitemap <loc>s are worth roughly 37k real jobs.
    Rows are therefore deduped on the source's own identity, (company,
    identifier.value), which collapses the country clones without merging two
    different postings that happen to share a careers-page apply URL (Zipline:
    45 rows, 45 ids, 45 urls — all kept).

RESUMABILITY
  remoteleaf_done.txt      append-only, one finished job URL per line.
  remoteleaf_fetch_state.json  counters + partition progress for reporting.
  The output JSONL is APPEND-only and is never re-written, so a URL already in
  the done-ledger is never fetched or emitted twice.

  A full crawl is ~58k page fetches (~1.4 GB of HTML) and takes about an hour at
  a polite rate, so a run is bounded by RL_MAX_SECONDS (default 1200) and the
  work queue is a PROPORTIONAL interleave of the seven partitions — each
  partition is consumed at a rate proportional to its size, so a partial run is
  a representative sample of the whole board rather than "all of Australia".
  Re-run until "remaining: 0".

ENV
  RL_MAX_SECONDS  wall-clock budget for one run (default 1200)
  RL_LIMIT        max job pages this run (default 0 = unbounded)
  RL_WORKERS      concurrent fetchers (default 6)
  RL_RPS          request budget per second (default 12)
"""
import json
import os
import re
import sys
import threading
import time
import concurrent.futures as cf
from html import unescape

import requests
import defusedxml.ElementTree as ET  # hardened against XXE / billion-laughs

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "remoteleaf.jsonl")
DONE_TXT = os.path.join(BASE, "remoteleaf_done.txt")
STATE_JSON = os.path.join(BASE, "remoteleaf_fetch_state.json")
SITEMAP_DIR = os.path.join(BASE, "remoteleaf_sitemaps")

ROOT = "https://remoteleaf.com"
SITEMAP_INDEX = ROOT + "/sitemap.xml"
# The seven location partitions.  sitemap-jobs-by.xml in the same index is a
# FILTER sitemap ("jobs by skill/title/..."), not a job partition — including it
# would enqueue thousands of listing pages that carry no JobPosting.
PARTITIONS = ["united-states", "europe", "united-kingdom", "canada",
              "australia", "worldwide", "other-locations"]

MAX_SECONDS = int(os.environ.get("RL_MAX_SECONDS") or 1200)
LIMIT = int(os.environ.get("RL_LIMIT") or 0)
WORKERS = int(os.environ.get("RL_WORKERS") or 6)
RPS = float(os.environ.get("RL_RPS") or 12)
TIMEOUT = 30
TRIES = 3
SITEMAP_MAX_AGE = 6 * 3600      # re-download a partition sitemap after 6h

_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]

LD_RX = re.compile(r'<script type="application/ld\+json"[^>]*>(.*?)</script>',
                   re.S)
# The primary apply button.  The template emits an HTML comment immediately
# before it; the "related jobs" cards further down the page carry their OWN
# outbound apply links, so anchoring on the comment (and not on the first
# external href) is what keeps a job from inheriting a neighbour's URL.
APPLY_RX = re.compile(r'<!--\s*Apply Button\s*-->.*?<a\s+href="(https?://[^"]+)"',
                      re.S)
APPLY_FALLBACK_RX = re.compile(
    r'Apply directly on the company website.*?<a\s+href="(https?://[^"]+)"',
    re.S)


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


def backoff(seconds=20):
    with _rate_lock:
        _pause_until[0] = max(_pause_until[0], time.time() + seconds)
        _interval[0] = min(_interval[0] * 1.5, 1.0)


def get_text(s, url, allow_404=True):
    """Return (text, status).  status is 'ok' | '404' | 'transient'."""
    last = "unknown"
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(url, timeout=TIMEOUT)
            if r.status_code == 404 and allow_404:
                return "", "404"
            if r.status_code == 429 or r.status_code >= 500:
                backoff(20 if r.status_code == 429 else 5)
                last = "HTTP %d" % r.status_code
            elif r.status_code == 200:
                return r.text, "ok"
            else:
                last = "HTTP %d" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))
    return last, "transient"


def sitemap_locs(s, name):
    """<loc> list for one job partition, cached on disk for SITEMAP_MAX_AGE."""
    os.makedirs(SITEMAP_DIR, exist_ok=True)
    path = os.path.join(SITEMAP_DIR, "sitemap-jobs-%s.xml" % name)
    fresh = (os.path.exists(path)
             and time.time() - os.path.getmtime(path) < SITEMAP_MAX_AGE
             and os.path.getsize(path) > 500)
    if not fresh:
        txt, st = get_text(s, "%s/sitemap-jobs-%s.xml" % (ROOT, name))
        if st != "ok" or "<urlset" not in txt:
            print("    ! sitemap %s -> %s (keeping any cached copy)"
                  % (name, st), flush=True)
            if not os.path.exists(path):
                return []
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(txt)
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    root = ET.fromstring(raw.encode("utf-8"))
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    out = []
    for u in root.findall(ns + "url"):
        loc = (u.findtext(ns + "loc") or "").strip()
        # only real job pages: /company/<company>/<job-slug>/
        if loc.startswith(ROOT + "/company/") and loc.count("/") >= 6:
            out.append(loc)
    return out


def first_jobposting(html):
    """The JobPosting JSON-LD, or None.  This is the live/dead DISCRIMINATOR:
    remoteleaf answers 200 with a full HTML shell for several non-job paths, so
    'status 200' proves nothing — the presence of a parsed JobPosting does."""
    for blob in LD_RX.findall(html):
        if '"JobPosting"' not in blob:
            continue
        try:
            d = json.loads(blob)
        except ValueError:
            continue
        if isinstance(d, dict) and d.get("@type") == "JobPosting":
            return d
    return None


def loc_names(ld):
    """Compact applicantLocationRequirements; hoist United States to the front
    so the downstream US/remote filters keep seeing it in a 50-country list."""
    alr = ld.get("applicantLocationRequirements")
    names = []
    if isinstance(alr, dict):
        names = [alr.get("name") or ""]
    elif isinstance(alr, list):
        names = [x.get("name") or "" for x in alr if isinstance(x, dict)]
    names = [n for n in names if n]
    if "United States" in names:
        names.remove("United States")
        names.insert(0, "United States")
    return names


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
    except (TypeError, ValueError):
        return ""


def parse_job(url, html):
    ld = first_jobposting(html)
    if ld is None:
        return None
    title = (ld.get("title") or "").strip()
    org = ld.get("hiringOrganization") or {}
    company = (org.get("name") or "").strip() if isinstance(org, dict) else ""

    m = APPLY_RX.search(html) or APPLY_FALLBACK_RX.search(html)
    # unescape: hrefs come out of the raw HTML, so a query string arrives as
    # "?gh_jid=1&amp;src=x" and would carry the entity into the canonical-ATS
    # key the pipeline parses out of apply_url.
    apply_url = unescape(m.group(1)) if m else url   # else the RemoteLeaf page

    names = loc_names(ld)
    telecommute = (ld.get("jobLocationType") or "").upper() == "TELECOMMUTE"
    if names and names[0].lower() in ("anywhere", "worldwide"):
        location = "Anywhere"
        country = ""
    elif names:
        location = ", ".join(names[:6]) + (" +%d more" % (len(names) - 6)
                                           if len(names) > 6 else "")
        country = names[0] if len(names) == 1 else ""
    else:
        location = "Remote" if telecommute else ""
        country = ""

    city = state = ""
    jl = ld.get("jobLocation")
    if isinstance(jl, dict):
        addr = jl.get("address") or {}
        if isinstance(addr, dict):
            city = addr.get("addressLocality") or ""
            state = addr.get("addressRegion") or ""
            country = country or (addr.get("addressCountry") or "")

    smin = smax = cur = per = ""
    bs = ld.get("baseSalary")
    if isinstance(bs, dict):
        val = bs.get("value") or {}
        if isinstance(val, dict):
            smin = num(val.get("minValue"))
            smax = num(val.get("maxValue"))
            per = (val.get("unitText") or "").title()
        cur = (bs.get("currency") or "").upper()
        if not (smin or smax):
            cur = per = ""

    ident = ld.get("identifier")
    job_id = ""
    if isinstance(ident, dict):
        job_id = str(ident.get("value") or "")
    if not job_id:
        job_id = url.rstrip("/").rsplit("/", 1)[-1]

    return cc.row(
        source="remoteleaf", source_type="remote_board",
        company=company, title=title,
        # classify_any, never classify: classify() returns None for FULL_TIME
        # and the row would be silently dropped.
        employment_type=cc.classify_any(ld.get("employmentType"), title),
        work_mode="Remote" if telecommute else cc.work_mode(text=location),
        location=location, city=city, state=state, country=country,
        remote=bool(telecommute),
        salary_min=smin, salary_max=smax, salary_currency=cur,
        salary_period=per,
        # datePosted is the real posting date (YYYY-MM-DD); the sitemap
        # <lastmod> is a sitemap-build stamp and is deliberately ignored.
        posted=cc.ymd(iso=ld.get("datePosted")),
        apply_url=apply_url, job_id=job_id, board="remoteleaf",
        board_url=ROOT)


def load_done():
    if not os.path.exists(DONE_TXT):
        return set()
    with open(DONE_TXT, encoding="utf-8") as f:
        return {ln.strip() for ln in f if ln.strip()}


def dedupe_out():
    """Rewrite the output keeping one row per (company, identifier), and return
    the surviving key set so this run does not re-add a country clone.
    Idempotent — safe to call on every run."""
    keys = set()
    if not os.path.exists(OUT):
        return keys
    tmp = OUT + ".tmp"
    n = kept = 0
    with open(OUT, encoding="utf-8") as src, \
            open(tmp, "w", encoding="utf-8") as dst:
        for ln in src:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            n += 1
            try:
                r = json.loads(ln)
            except ValueError:
                continue
            k = (r.get("company"), r.get("job_id"))
            if k in keys:
                continue
            keys.add(k)
            au = r.get("apply_url") or ""
            if "&amp;" in au or "&#" in au:      # retro-fix pre-unescape rows
                r["apply_url"] = unescape(au)
                ln = json.dumps(r)
            dst.write(ln + "\n")
            kept += 1
    os.replace(tmp, OUT)
    if n != kept:
        print("  collapsed %d multi-country clones (%d -> %d rows)"
              % (n - kept, n, kept), flush=True)
    return keys


def interleave(buckets):
    """Proportional round-robin: every partition is consumed at a rate
    proportional to its size, so a time-bounded run samples the whole board
    instead of finishing the small partitions first."""
    keyed = []
    for name, urls in buckets.items():
        n = len(urls)
        if not n:
            continue
        for i, u in enumerate(urls):
            keyed.append(((i + 0.5) / n, name, u))
    keyed.sort(key=lambda t: t[0])
    return [(name, u) for _, name, u in keyed]


def main():
    t0 = time.time()
    s = cc.session()
    s.headers.update({"Accept": "text/html,application/xhtml+xml"})

    idx, st = get_text(s, SITEMAP_INDEX)
    if st != "ok" or "<sitemapindex" not in idx:
        raise SystemExit("sitemap index unreachable (%s) — NOT recording an "
                         "empty board" % st)

    done = load_done()
    buckets = {}
    total = 0
    for name in PARTITIONS:
        locs = sitemap_locs(s, name)
        total += len(locs)
        todo = [u for u in locs if u not in done]
        buckets[name] = todo
        print("  %-16s %6d urls, %6d todo" % (name, len(locs), len(todo)),
              flush=True)
    queue = interleave(buckets)
    remaining_at_start = len(queue)
    if LIMIT:
        queue = queue[:LIMIT]
    print("  sitemap total %d, %d already done, %d queued this run"
          % (total, total - remaining_at_start, len(queue)), flush=True)
    if not queue:
        print("DONE remoteleaf_fetch: nothing left to fetch", flush=True)
        return 0

    seen_keys = dedupe_out()
    out = cc.Writer(OUT)
    donef = open(DONE_TXT, "a", encoding="utf-8")
    lock = threading.Lock()
    counts = {"rows": 0, "dead404": 0, "no_ld": 0, "transient": 0,
              "no_apply": 0, "ext_apply": 0, "clones": 0}
    per_part = {p: 0 for p in PARTITIONS}
    stop = threading.Event()

    local = threading.local()

    def sess():
        if not hasattr(local, "s"):
            local.s = cc.session()
            local.s.headers.update({"Accept": "text/html,application/xhtml+xml"})
        return local.s

    def work(item):
        part, url = item
        if stop.is_set():
            return
        if time.time() - t0 > MAX_SECONDS:
            stop.set()
            return
        html, status = get_text(sess(), url)
        if status == "transient":
            with lock:
                counts["transient"] += 1
            return                      # NOT marked done -> retried next run
        if status == "404":
            with lock:
                counts["dead404"] += 1
                donef.write(url + "\n")
            return
        rec = parse_job(url, html)
        if rec is None:
            # 200 with no JobPosting: an HTML shell, not a dead posting.
            with lock:
                counts["no_ld"] += 1
            return                      # deliberately NOT marked done
        with lock:
            key = (rec["company"], rec["job_id"])
            if key in seen_keys:
                # a country clone of a posting already held: the URL IS done,
                # it just must not become a second row
                counts["clones"] += 1
                donef.write(url + "\n")
                return
            seen_keys.add(key)
            out.write(rec)
            donef.write(url + "\n")
            counts["rows"] += 1
            per_part[part] += 1
            if rec["apply_url"].startswith(ROOT):
                counts["no_apply"] += 1
            else:
                counts["ext_apply"] += 1
            if counts["rows"] % 500 == 0:
                donef.flush()
                print("    %d rows | %d dead | %.0fs"
                      % (counts["rows"], counts["dead404"], time.time() - t0),
                      flush=True)
        if time.time() - t0 > MAX_SECONDS:
            stop.set()

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, queue))

    out.close()
    donef.flush()
    donef.close()
    processed = counts["rows"] + counts["dead404"] + counts["clones"]
    left = remaining_at_start - processed
    cc.save_state(STATE_JSON, {
        "last_run": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sitemap_total": total, "rows_this_run": counts["rows"],
        "dead_404": counts["dead404"], "html_shell_no_jobposting":
        counts["no_ld"], "transient_failures": counts["transient"],
        "external_apply_urls": counts["ext_apply"],
        "fell_back_to_remoteleaf_url": counts["no_apply"],
        "multi_country_clones_collapsed": counts["clones"],
        "per_partition_rows": per_part,
        "remaining": max(left, 0),
        "elapsed_sec": round(time.time() - t0, 1)})
    print("\nremoteleaf: %d rows (%d with an external ATS apply URL, %d fell "
          "back to the RemoteLeaf page)" % (counts["rows"], counts["ext_apply"],
                                            counts["no_apply"]), flush=True)
    print("  %d hard-404 dead postings, %d 200-but-no-JobPosting shells, "
          "%d transient failures (all retried on re-run), %d multi-country "
          "clones collapsed"
          % (counts["dead404"], counts["no_ld"], counts["transient"],
             counts["clones"]), flush=True)
    print("  per-partition rows: %s" % per_part, flush=True)
    print("  ~%d of %d urls still to fetch — re-run to continue"
          % (max(left, 0), total), flush=True)
    print("DONE remoteleaf_fetch (%.0fs)" % (time.time() - t0), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
