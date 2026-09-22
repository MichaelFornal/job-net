#!/usr/bin/env python3
"""
intlmisc_fetch.py — five unaffiliated international / niche job sources that
each expose an unauthenticated JSON (or WP-REST / Algolia) feed.  All five
write into one shared contract_norm/intlmisc.jsonl, each with its own `source`.

  powertofly    remote_board  powertofly.com/api/v1/jobs/      US + REMOTE heavy
  wttj          remote_board  Algolia wttj_jobs_production_en  FR-heavy, 14k US
  wanted        remote_board  wanted.co.kr/api/v4/jobs         KR only
  wantedly      remote_board  wantedly.com/api/v1/projects     JP only
  berlinstartup remote_board  berlinstartupjobs.com WP REST    DE only

Everything below was probed live on 2026-07-29; the per-source comments record
the traps, because every one of these APIs answers HTTP 200 to something that
is not what it looks like.

Env:
  INTLMISC_ONLY=a,b        run only these sources (writer APPENDS, so a full
                           re-run to recover one source would duplicate the
                           others)
  INTLMISC_OUT=/path.jsonl override the output path (used to run two source
                           groups as two processes without interleaving writes
                           into one file handle)
  PTF_WORKERS=1            powertofly detail-page concurrency (see _ptf_detail
  PTF_PACE=1.0             -- more workers or less pace does NOT go faster, it
                           silently returns the marketing shell instead of the
                           job page).  PTF_PACE=2.0 measured 92% real pages,
                           1.0 -> 60%, flat out -> 26%.
  PTF_MAX_DETAIL=0         cap on how many NEW detail pages to fetch this run
                           (0 = no cap).  The apply-url cache makes the crawl
                           resumable, so a capped run can be repeated until the
                           board is drained; at the polite pace the full 9,636
                           is a ~4-6 hour job, so run it in slices.
  WANTEDLY_MAX_PAGES=100   pages per wantedly query (the feed hard-stops at 100)
  WANTEDLY_KW_PAGES=10     pages per wantedly keyword partition
  WANTED_MAX_OFFSET=40000  wanted.co.kr offset ceiling
  WTTJ_MAX_LEAVES=4000     safety stop on the wttj bisection
"""
import concurrent.futures as cf
import html
import json
import os
import random
import re
import time
import urllib.parse
from email.utils import parsedate_to_datetime

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get("INTLMISC_OUT") or os.path.join(cc.NORM_DIR,
                                                     "intlmisc.jsonl")
STATE_DIR = os.path.join(BASE, "intlmisc_state")
os.makedirs(STATE_DIR, exist_ok=True)

# 1 worker + ~1s between hits is what keeps powertofly serving REAL detail
# pages instead of its throttle shell -- see _ptf_detail.  Raising either of
# these does not make the crawl faster, it makes it silently wrong.
PTF_WORKERS = int(os.environ.get("PTF_WORKERS") or 1)
PTF_PACE = float(os.environ.get("PTF_PACE") or 1.0)
PTF_MAX_DETAIL = int(os.environ.get("PTF_MAX_DETAIL") or 0)
WANTEDLY_MAX_PAGES = int(os.environ.get("WANTEDLY_MAX_PAGES") or 100)
WANTEDLY_KW_PAGES = int(os.environ.get("WANTEDLY_KW_PAGES") or 10)
WANTED_MAX_OFFSET = int(os.environ.get("WANTED_MAX_OFFSET") or 40000)
WTTJ_MAX_LEAVES = int(os.environ.get("WTTJ_MAX_LEAVES") or 4000)


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
    except (TypeError, ValueError):
        return ""


def jsn(s, url, **kw):
    """cc.get_json, but a transport failure is LOUD instead of empty.

    Same reasoning as boards_fetch.jsn: every caller reads the answer as
    d.get(...) or [], so without this a 403/timeout is indistinguishable from
    'this board is empty today' and the source silently reports 0 rows.
    """
    d = cc.get_json(s, url, **kw)
    if isinstance(d, dict) and ("__error__" in d or "__status__" in d):
        why = d.get("__error__") or f"HTTP {d.get('__status__')}"
        print(f"    ! {url[:140]} -> {why}", flush=True)
        return {}
    return d


def get_text(s, url, tries=4, timeout=25, headers=None):
    """HTML GET with 429/5xx backoff.  Returns "" on a hard failure."""
    for i in range(tries):
        try:
            r = s.get(url, timeout=timeout, headers=headers)
            if r.status_code == 200:
                return r.text
            if r.status_code in (404, 410):
                return ""
            if r.status_code == 429:
                time.sleep(10 * (i + 1) + random.random() * 3)
                continue
        except requests.RequestException:
            pass
        time.sleep(1.2 * (i + 1))
    return ""


# ===========================================================================
# powertofly
# ===========================================================================
# The list feed at /api/v1/jobs/ is real JSON and its paginator really does
# advance (page 1 vs page 2 ids: 0 overlap; page 193 is the 36-row tail of
# meta.total=9636).  per_page IS honoured -- per_page=200 collapses 193 pages
# to 49 and page 1 of the 200-wide feed starts with exactly the same 50 ids as
# page 1 of the 50-wide feed.
#
# Two traps:
#  * /api/v1/jobs/<id> (the obvious detail route) answers 401 Unauthorized, and
#    /api/v1/jobs/<id>/ with the trailing slash answers HTTP **404 with a
#    17KB HTML shell** -- status code alone would read as "fine".
#  * the list rows carry NO apply url and NO posted date.  Both live on the
#    public page https://powertofly.com/jobs/detail/<id>: the real employer ATS
#    link is in the HTML-escaped data-job="..." attribute as custom_apply_url
#    (10/10 sampled ids resolved to workday / icims / greenhouse / careers.*),
#    and datePosted is in the JSON-LD JobPosting block.  So the detail page is
#    fetched once per job and cached on disk, which is also what makes this
#    source resumable.
PTF_LIST = "https://powertofly.com/api/v1/jobs/?page={}&per_page=200"
PTF_CACHE = os.path.join(STATE_DIR, "powertofly_detail.json")
_PTF_DATA_JOB = re.compile(r'id="js-job-detail-root"[^>]*data-job="([^"]*)"')
_PTF_LD = re.compile(r'<script type="application/ld\+json">(.*?)</script>',
                     re.S)
# EVERY genuine detail page carries this element.  Its ABSENCE is the tell for
# the throttle shell described in _ptf_detail; never relax this to a softer
# check such as "the body is non-empty".
_PTF_MARKER = 'id="js-job-detail-root"'


def _ptf_detail(sess, jid, tries=4):
    """-> (apply_url, posted YYYY-MM-DD, ok).

    ok=False means the real page never arrived — NOT that this job has no
    external apply url — so the caller must retry instead of caching the
    absence.

    The trap this guards is the nastiest one in this file.  Over its rate
    limit, powertofly does not answer 429: it answers **HTTP 200 with the
    generic marketing homepage** ("PowerToFly | Find Your Next Great Role",
    ~100KB, X-Served-By a Fastly edge with a four-figure Age).  A fetcher that
    only checks the status code, or only checks that the body is non-empty,
    reads that as "this job simply has no apply url" and caches it forever.
    The first full run did exactly that: 9,636/9,636 pages "succeeded" and only
    314 produced an apply url, while the same ids re-fetched one at a time all
    parsed fine.  Measured 2026-07-29: 6 threads -> 18% real pages, 3 threads
    -> 0%, 1 thread flat out -> 26%, 1 thread paced -> usable.  Neither a
    Cache-Control: no-cache header nor a cache-busting query string helps
    (0/30 and 4/30 respectively); only pacing does.
    """
    for i in range(tries):
        h = get_text(sess, f"https://powertofly.com/jobs/detail/{jid}",
                     headers={"Accept": "text/html"})
        if h and _PTF_MARKER in h:
            break
        if i == tries - 1:
            return "", "", False
        time.sleep(PTF_PACE * (3 * (i + 1)) + random.random())
    apply_url = ""
    m = _PTF_DATA_JOB.search(h)
    if m:
        try:
            d = json.loads(html.unescape(m.group(1)))
            apply_url = d.get("custom_apply_url") or ""
        except (ValueError, AttributeError):
            apply_url = ""
    posted = ""
    for blk in _PTF_LD.findall(h):
        try:
            d = json.loads(blk)
        except ValueError:
            continue
        if isinstance(d, dict) and d.get("@type") == "JobPosting":
            dp = d.get("datePosted")
            if dp:
                try:
                    posted = parsedate_to_datetime(dp).strftime("%Y-%m-%d")
                except (TypeError, ValueError):
                    posted = cc.ymd(iso=dp)
            break
    return apply_url, posted, True


def do_powertofly(s, out):
    jobs = []
    page = 1
    total = None
    while page <= 400:
        d = jsn(s, PTF_LIST.format(page))
        rows = d.get("data") or []
        meta = d.get("meta") or {}
        if total is None and meta.get("total"):
            total = meta["total"]
            print(f"    powertofly meta.total={total} "
                  f"pages={meta.get('pages')}", flush=True)
        if not rows:
            break
        jobs += rows
        if not meta.get("next_page"):
            break
        page += 1
        time.sleep(0.35)
    ids = [j.get("id") for j in jobs]
    print(f"    powertofly listed {len(jobs)} rows / {len(set(ids))} unique "
          f"ids over {page} pages", flush=True)

    cache = cc.load_state(PTF_CACHE)
    todo = [j["id"] for j in jobs if str(j.get("id")) not in cache]
    if PTF_MAX_DETAIL:
        todo = todo[:PTF_MAX_DETAIL]
    print(f"    powertofly detail pages: {len(cache)} cached, "
          f"{len(todo)} to fetch", flush=True)
    if todo:
        sessions = [cc.session() for _ in range(PTF_WORKERS)]
        for sx in sessions:
            sx.headers["Accept"] = "text/html,application/xhtml+xml"

        def work(pair):
            i, jid = pair
            time.sleep(PTF_PACE * PTF_WORKERS * random.uniform(0.8, 1.2))
            return jid, _ptf_detail(sessions[i % PTF_WORKERS], jid)

        def pool_pass(batch):
            failed = []
            done = 0
            with cf.ThreadPoolExecutor(max_workers=PTF_WORKERS) as ex:
                for jid, (au, po, ok) in ex.map(work, enumerate(batch)):
                    if ok:
                        cache[str(jid)] = [au, po]
                    else:
                        failed.append(jid)
                    done += 1
                    if done % 200 == 0:
                        cc.save_state(PTF_CACHE, cache)
                        got = sum(1 for v in cache.values() if v[0])
                        print(f"      ... {done}/{len(batch)} detail pages, "
                              f"{len(failed)} unreachable, {got} apply urls",
                              flush=True)
            cc.save_state(PTF_CACHE, cache)
            return failed

        misses = pool_pass(todo)
        if misses:
            print(f"      retrying {len(misses)} misses serially",
                  flush=True)
            still = []
            for jid in misses:
                au, po, ok = _ptf_detail(sessions[0], jid)
                if ok:
                    cache[str(jid)] = [au, po]
                else:
                    still.append(jid)
                time.sleep(PTF_PACE * 2)
            cc.save_state(PTF_CACHE, cache)
            if still:
                print(f"      {len(still)} detail pages still unreachable; "
                      f"re-run to pick them up", flush=True)
        for sx in sessions:
            sx.close()

    n = 0
    seen = set()
    for j in jobs:
        jid = j.get("id")
        if jid in seen:
            continue
        seen.add(jid)
        co = j.get("company") or {}
        country = j.get("country") or {}
        state = j.get("state") or {}
        city = j.get("city") or {}
        # `location` is a STRUCTURED enum on this API: Onsite / Hybrid / Remote.
        loc_mode = (j.get("location") or "").strip().lower()
        wm = {"remote": "Remote", "hybrid": "Hybrid",
              "onsite": "On-site"}.get(loc_mode, "")
        parts = [city.get("title"), state.get("code") or state.get("title"),
                 country.get("title")]
        loc = ", ".join(p for p in parts if p)
        if wm == "Remote":
            loc = ("Remote" + (f" ({loc})" if loc else "")).strip()
        au, po = cache.get(str(jid)) or ("", "")
        out.write(cc.row(
            source="powertofly", source_type="remote_board",
            company=co.get("name") or "", title=j.get("title") or "",
            employment_type=cc.classify_any(j.get("employment_type"),
                                            j.get("title")),
            work_mode=wm, remote=(wm == "Remote"),
            location=loc or (j.get("location") or ""),
            city=city.get("title") or "",
            state=state.get("code") or state.get("title") or "",
            country=country.get("id") or "",
            posted=po,
            apply_url=au or f"https://powertofly.com/jobs/detail/{jid}",
            job_id=jid, board=co.get("slug") or "powertofly",
            board_url="https://powertofly.com/jobs"))
        n += 1
    withats = sum(1 for j in jobs if (cache.get(str(j.get("id"))) or [""])[0])
    print(f"  powertofly: {n} ({withats} with a real employer ATS apply url)",
          flush=True)


# ===========================================================================
# wttj — welcometothejungle
# ===========================================================================
# The repo note was right that the jobs route is an Algolia index and wrong
# that the index name is unfindable.  The CURRENT Next.js front-end inlines
# nothing (no __NEXT_DATA__, no job links, no "algolia" string anywhere in the
# 533KB shell or in its 25 JS chunks).  The LEGACY app is still served for any
# unrouted path -- e.g. https://www.welcometothejungle.com/en/jobs.rss -- and
# its 404 shell carries the whole client config in window.env:
#     ALGOLIA_APPLICATION_ID   CSEKHVMS53
#     ALGOLIA_API_KEY_CLIENT   4bd8f6215d0cc52b26430765769e65a0  (search-only)
#     ALGOLIA_JOBS_INDEX_PREFIX wttj_jobs_production
# The prefix alone is NOT an index ("Index wttj_jobs_production does not
# exist"); the live one is the "_en" replica.  A Referer header is mandatory --
# without it every query is 403 "Method not allowed with this referer".
#
# Paginator trap: nbHits is 89,239 but paginationLimitedTo is 1000 records --
# hitsPerPage=1000&page=1 returns ZERO hits, so a naive page walk "succeeds"
# with exactly 1000 rows and no error.  The walk below therefore recursively
# bisects published_at_timestamp until every slice is <= 1000 hits.
WTTJ_APP = "CSEKHVMS53"
WTTJ_KEY = "4bd8f6215d0cc52b26430765769e65a0"
WTTJ_INDEX = "wttj_jobs_production_en"
WTTJ_URL = f"https://{WTTJ_APP}-dsn.algolia.net/1/indexes/*/queries"
WTTJ_ATTRS = ["name", "slug", "organization.name", "organization.slug",
              "contract_type", "remote", "offices", "published_at",
              "published_at_timestamp", "salary_minimum", "salary_maximum",
              "salary_currency", "salary_period", "reference", "language"]
WTTJ_PAGE_CAP = 1000          # Algolia paginationLimitedTo, measured
_WTTJ_MODE = {"fulltime": "Remote", "partial": "Hybrid", "punctual": "Hybrid",
              "no": "On-site"}


def wttj_query(s, params, tries=4):
    body = json.dumps({"requests": [{"indexName": WTTJ_INDEX,
                                     "params": urllib.parse.urlencode(params)}]})
    hdr = {"X-Algolia-Application-Id": WTTJ_APP,
           "X-Algolia-API-Key": WTTJ_KEY,
           "Content-Type": "application/json",
           "Referer": "https://www.welcometothejungle.com/"}
    for i in range(tries):
        try:
            r = s.post(WTTJ_URL, data=body, headers=hdr, timeout=30)
            if r.status_code == 200:
                return r.json()["results"][0]
            if r.status_code == 429:
                time.sleep(8 * (i + 1))
                continue
            print(f"    ! wttj HTTP {r.status_code} {r.text[:120]}",
                  flush=True)
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"    ! wttj {type(e).__name__}", flush=True)
        time.sleep(1.5 * (i + 1))
    return {}


def _wttj_filters(lo, hi):
    return json.dumps([f"published_at_timestamp>={lo}",
                       f"published_at_timestamp<{hi}"])


def do_wttj(s, out):
    stats = {"leaves": 0, "req": 0, "trunc": 0}
    seen = set()
    n = 0

    def emit(hits):
        nonlocal n
        for h in hits:
            oid = h.get("objectID") or h.get("reference")
            if not oid or oid in seen:
                continue
            seen.add(oid)
            org = h.get("organization") or {}
            offs = h.get("offices") or []
            o = offs[0] if offs else {}
            rem = (h.get("remote") or "").lower()
            wm = _WTTJ_MODE.get(rem, "")
            city = o.get("local_city") or o.get("city") or ""
            st = o.get("local_state") or o.get("state") or ""
            ctry = o.get("country_code") or ""
            loc = ", ".join(p for p in (city, st, o.get("country")) if p)
            slug = h.get("slug") or ""
            oslug = org.get("slug") or ""
            url = (f"https://www.welcometothejungle.com/en/companies/"
                   f"{oslug}/jobs/{slug}") if (oslug and slug) else ""
            per = (h.get("salary_period") or "")
            out.write(cc.row(
                source="wttj", source_type="remote_board",
                company=org.get("name") or "", title=h.get("name") or "",
                employment_type=cc.classify_any(
                    (h.get("contract_type") or "").replace("_", " "),
                    h.get("name")),
                work_mode=wm, remote=(rem == "fulltime"),
                location=loc, city=city, state=st, country=ctry,
                salary_min=num(h.get("salary_minimum")),
                salary_max=num(h.get("salary_maximum")),
                salary_currency=(h.get("salary_currency") or "").upper(),
                salary_period={"yearly": "Year", "monthly": "Month",
                               "daily": "Day", "hourly": "Hour",
                               "weekly": "Week"}.get(per, per.title()),
                posted=cc.ymd(ts=h.get("published_at_timestamp"))
                or cc.ymd(iso=h.get("published_at")),
                apply_url=url, job_id=h.get("reference") or oid,
                board=oslug or "wttj",
                board_url="https://www.welcometothejungle.com/en/jobs"))
            n += 1

    # nbHits on this index is NOT exhaustive: the response carries
    # exhaustiveNbHits=false for anything large, and a parent/child triple
    # measured 88,772 vs 12,903 + 88,169 -- the two halves "sum" to more than
    # the whole.  So nbHits may only be used as a cheap HINT to decide whether
    # to bother splitting; the authoritative test is how many hits actually
    # come back, which is exact and never exceeds the 1000-record window.
    WTTJ_SPLIT_HINT = 2000

    def count(lo, hi):
        stats["req"] += 1
        r = wttj_query(s, {"hitsPerPage": 0, "page": 0,
                           "numericFilters": _wttj_filters(lo, hi)})
        time.sleep(0.1)
        return r.get("nbHits", 0)

    def fetch(lo, hi):
        stats["req"] += 1
        r = wttj_query(s, {"hitsPerPage": WTTJ_PAGE_CAP, "page": 0,
                           "numericFilters": _wttj_filters(lo, hi),
                           "attributesToRetrieve": json.dumps(WTTJ_ATTRS),
                           "attributesToHighlight": json.dumps([])})
        hits = r.get("hits") or []
        emit(hits)
        time.sleep(0.1)
        return len(hits)

    def walk(lo, hi):
        if stats["leaves"] >= WTTJ_MAX_LEAVES:
            return
        est = count(lo, hi)
        if est == 0:
            return
        if est > WTTJ_SPLIT_HINT and hi - lo > 1:
            mid = (lo + hi) // 2
            walk(lo, mid)
            walk(mid, hi)
            return
        got = fetch(lo, hi)
        if got < WTTJ_PAGE_CAP:
            stats["leaves"] += 1
            return
        # a full 1000 came back -> the slice is capped, not complete
        if hi - lo <= 1:
            stats["trunc"] += 1     # one second holding >=1000 postings
            stats["leaves"] += 1
            return
        mid = (lo + hi) // 2
        walk(lo, mid)
        walk(mid, hi)

    total = count(0, 4000000000)
    print(f"    wttj index {WTTJ_INDEX} nbHits~{total} "
          f"(paginationLimitedTo={WTTJ_PAGE_CAP}, nbHits is an estimate)",
          flush=True)
    if not total:
        print("  wttj: 0 (index unreachable)", flush=True)
        return
    walk(1300000000, int(time.time()) + 172800)
    print(f"  wttj: {n} unique (estimate was ~{total}; {stats['leaves']} "
          f"slices, {stats['req']} requests, {stats['trunc']} 1-second slices "
          f"still capped at the 1000-record window)", flush=True)


# ===========================================================================
# wanted.co.kr
# ===========================================================================
# /api/v4/jobs is real JSON, offset really advances (offset 0 vs 100 at
# limit=100: 0 overlap) and limit IS honoured up to 100.  There is no
# total-count field; `links.next` goes null at the true end of the corpus --
# country=kr runs dry at offset ~11,619 (offset 11600 -> 19 rows, next null),
# so that ceiling is the size of the board, not a hidden window.  country=us
# and country=other are HTTP 422 "Not a valid choice"; country=all is the
# widest legal value and outruns country=kr, so that is what is walked.
WANTED_URL = ("https://www.wanted.co.kr/api/v4/jobs?country=all"
              "&job_sort=job.latest_order&locations=all&years=-1"
              "&limit=100&offset={}")


def do_wanted(s, out):
    n = 0
    off = 0
    seen = set()
    empties = 0
    while off < WANTED_MAX_OFFSET:
        d = jsn(s, WANTED_URL.format(off))
        rows = d.get("data") or []
        if not rows:
            empties += 1
            if empties >= 2:
                break
            time.sleep(6)
            continue
        empties = 0
        for j in rows:
            jid = j.get("id")
            if jid in seen:
                continue
            seen.add(jid)
            co = j.get("company") or {}
            ad = j.get("address") or {}
            loc = (ad.get("full_location") or "").strip()
            city = ad.get("district") or ""
            st = ad.get("location") or ""
            ctry = ad.get("country") or ""
            out.write(cc.row(
                source="wanted", source_type="remote_board",
                company=co.get("name") or "", title=j.get("position") or "",
                employment_type=cc.classify_any(None, j.get("position")),
                work_mode="", remote=False,
                location=loc or ", ".join(p for p in (city, st, ctry) if p),
                city=city, state=st,
                country="KR" if ctry in ("한국", "대한민국") else ctry,
                apply_url=f"https://www.wanted.co.kr/wd/{jid}",
                job_id=jid, board="wanted",
                board_url="https://www.wanted.co.kr/wdlist"))
            n += 1
        if not ((d.get("links") or {}).get("next")):
            off += 100
            break
        off += 100
        time.sleep(0.45)
    print(f"  wanted: {n} unique (walked to offset {off})", flush=True)


# ===========================================================================
# wantedly
# ===========================================================================
# /api/v1/projects is unauthenticated JSON (the v2 route answers 401 "Invalid
# Wantedly access token").  Its _metadata is a LIE worth naming: it reports
# total_objects 173,585 / total_pages 17,359, but page 100 returns 6 rows and
# page 101 returns ZERO with HTTP 200 -- the feed hard-stops at ~1,000
# projects.  per_page / limit are silently ignored (always 10 rows).
# `occupation` is also ignored (occupation=xyzzy returns the same total as
# occupation=sales).  `q` is the ONE parameter that actually filters, and
# paging inside a q is real (page 1 vs page 5 of the same q: 0 overlap), so a
# keyword sweep is the only way past 1,000 rows.  Even so this board is 100%
# Japanese-market.
WANTEDLY_URL = "https://www.wantedly.com/api/v1/projects?page={}"
WANTEDLY_KEYWORDS = [
    "エンジニア", "デザイナー", "営業", "マーケティング", "機械学習", "AI",
    "python", "React", "Go", "Ruby", "Java", "PHP", "iOS", "Android",
    "インターン", "新卒", "リモート", "データ", "PM", "SRE", "セールス",
    "コンサル", "人事", "経理", "ライター", "動画", "医療", "教育",
]


def do_wantedly(s, out):
    n = 0
    seen = set()

    def sweep(url_tpl, max_pages, label):
        nonlocal n
        added = 0
        for page in range(1, max_pages + 1):
            d = jsn(s, url_tpl.format(page))
            rows = d.get("data") or []
            if not rows:
                break
            for j in rows:
                jid = j.get("id")
                if jid in seen:
                    continue
                seen.add(jid)
                co = j.get("company") or {}
                loc = (j.get("location") or "").strip()
                title = j.get("title") or j.get("looking_for") or ""
                out.write(cc.row(
                    source="wantedly", source_type="remote_board",
                    company=co.get("name") or "", title=title,
                    employment_type=cc.classify_any(None, title),
                    # no structured remote/commitment field on this API, and
                    # the JP titles do not carry a reliable one either
                    work_mode="", remote=False,
                    location=loc, country="JP",
                    posted=cc.ymd(iso=j.get("published_at")),
                    apply_url=f"https://www.wantedly.com/projects/{jid}",
                    job_id=jid, board="wantedly",
                    board_url="https://www.wantedly.com/projects"))
                n += 1
                added += 1
            time.sleep(0.5)
        return added

    base = sweep(WANTEDLY_URL, WANTEDLY_MAX_PAGES, "feed")
    print(f"    wantedly base feed: {base} (hard stop ~100 pages x 10)",
          flush=True)
    for kw in WANTEDLY_KEYWORDS:
        tpl = (WANTEDLY_URL + "&q=" + urllib.parse.quote(kw))
        got = sweep(tpl, WANTEDLY_KW_PAGES, kw)
        print(f"    wantedly q={kw}: +{got}", flush=True)
    print(f"  wantedly: {n} unique", flush=True)


# ===========================================================================
# berlinstartupjobs
# ===========================================================================
# /wp-json/wp/v2/job_listing is a 404 -- there is no WP-Job-Manager here.  The
# jobs ARE the default post type: wp/v2/types shows post renamed "Jobs" with
# job_company / job_location / job_plan taxonomies.  x-wp-total says 146, i.e.
# the whole live board is two pages of 100; the /feed/ RSS carries only the
# newest 12, so the REST route is strictly better.
# `_embed=wp:term` resolves the taxonomy ids to company / location names in the
# same request.  The employer's real apply link is the last URL in the post
# body ("Apply here: https://jobs.ashbyhq.com/..."), so it is pulled out of the
# rendered content when it points off-site.
BSJ_URL = ("https://berlinstartupjobs.com/wp-json/wp/v2/posts"
           "?per_page=100&page={}&_embed=wp%3Aterm")
_BSJ_HREF = re.compile(r'href="(https?://[^"]+)"', re.I)
_BSJ_BARE = re.compile(r'https?://[^\s<>"\')]+', re.I)
_BSJ_SKIP = re.compile(
    r"berlinstartupjobs\.com|facebook\.|twitter\.|x\.com|linkedin\.com/(?!jobs)"
    r"|instagram\.|youtube\.|t\.me|wa\.me|mailto:", re.I)


def _bsj_apply(content, fallback):
    urls = _BSJ_HREF.findall(content) + _BSJ_BARE.findall(
        re.sub(r"<[^>]+>", " ", content))
    for u in reversed(urls):
        u = html.unescape(u).rstrip(".,);")
        if not _BSJ_SKIP.search(u):
            return u
    return fallback


def do_berlinstartup(s, out):
    n = 0
    page = 1
    while page <= 30:
        d = jsn(s, BSJ_URL.format(page))
        rows = d if isinstance(d, list) else []
        if not rows:
            break
        for j in rows:
            terms = {}
            for grp in (j.get("_embedded") or {}).get("wp:term") or []:
                for t in grp:
                    terms.setdefault(t.get("taxonomy"), []).append(
                        t.get("name"))
            company = "; ".join(terms.get("job_company") or []) or ""
            locs = terms.get("job_location") or []
            remote = any((x or "").strip().lower() == "remote" for x in locs)
            place = [x for x in locs if (x or "").strip().lower() != "remote"]
            title = html.unescape(
                ((j.get("title") or {}).get("rendered") or "")).strip()
            # "Role // Company" is the house title format; strip the suffix
            role = title.split("//")[0].strip() if "//" in title else title
            content = (j.get("content") or {}).get("rendered") or ""
            tags = " ".join(terms.get("post_tag") or [])
            out.write(cc.row(
                source="berlinstartup", source_type="remote_board",
                company=company, title=role,
                employment_type=cc.classify_any(tags, role),
                work_mode="Remote" if remote else ("On-site" if place else ""),
                remote=remote,
                location="; ".join(locs) or "Berlin, Germany",
                city=(place[0].split(",")[0].strip() if place else ""),
                country="DE",
                posted=cc.ymd(iso=(j.get("date_gmt") or "") + "Z"
                              if j.get("date_gmt") else None),
                apply_url=_bsj_apply(content, j.get("link") or ""),
                job_id=j.get("id"), board="berlinstartupjobs",
                board_url="https://berlinstartupjobs.com/"))
            n += 1
        if len(rows) < 100:
            break
        page += 1
        time.sleep(0.5)
    print(f"  berlinstartup: {n}", flush=True)


SOURCES = {
    "powertofly": do_powertofly,
    # wttj is UNREGISTERED as of 2026-09-02, not deleted.  wttj_fetch.py walks
    # the same Algolia index whole (all 88,431 postings, 100.00% coverage
    # verified against nbHits) plus the second index this file never knew
    # about -- wk_cms_organizations_production, 17,204 companies -- keeps all
    # 42 record fields instead of 15, and runs the walk 12-wide in ~15s.  It
    # is also default-ON in run_all, where this copy sat in DEFAULT_OFF `intl`
    # and refreshed only when somebody ran `--only intl`.
    #
    # do_wttj and its constants are left in place below because they document
    # the index, the 1000-record paginator trap and the referer requirement in
    # this file's own voice.  NOTE that unregistering it means INTLMISC_ONLY=
    # wttj now exits "unknown source 'wttj'" -- main() validates against this
    # dict.  Re-add the line above to bring it back; nothing else changed.
    "wanted": do_wanted,
    "wantedly": do_wantedly,
    "berlinstartup": do_berlinstartup,
}


def main():
    only = [x for x in os.environ.get("INTLMISC_ONLY", "").split(",") if x]
    for name in only:
        if name not in SOURCES:
            raise SystemExit(f"unknown source {name!r} (choose from "
                             f"{', '.join(SOURCES)})")
    out = cc.Writer(OUT)
    s = cc.session()
    for name, fn in SOURCES.items():
        if only and name not in only:
            continue
        t0 = time.time()
        try:
            fn(s, out)
        except Exception as e:  # noqa: BLE001 — one dead source is not fatal
            print(f"  {fn.__name__}: ERROR {type(e).__name__}: {e}",
                  flush=True)
        print(f"    [{name} {time.time() - t0:.0f}s]", flush=True)
    out.close()
    s.close()
    print(f"DONE intlmisc_fetch -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
