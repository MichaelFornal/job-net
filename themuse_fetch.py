#!/usr/bin/env python3
"""
themuse_fetch.py — harvest themuse.com postings into contract_norm/themuse.jsonl.

WHY NOT THE DOCUMENTED PUBLIC API
---------------------------------
The advertised source is GET /api/public/jobs?page=N (406,331 jobs, keyless).
Two measured facts kill it as the listing engine (both re-verified 2026-07-28):

  1. page is hard-capped at 99.  page_count says 20317 and page>=100 answers
     HTTP 400 {"error":"Value `page` is too high"} — so ONE query can only ever
     yield 2,000 rows and a naive pager stops there believing it is done.
  2. the keyless budget is 500 requests/HOUR, and it is real: every response
     carries X-Ratelimit-Limit: 500 / X-Ratelimit-Remaining / X-Ratelimit-Reset.
     At 20 rows a page that is 10k jobs/hour, i.e. ~41 hours for the full board.

It also carries no employment type, no salary, no structured remote flag, and
refs.landing_page is a themuse.com page — NOT the employer's ATS URL — so every
row would land as a fresh row instead of deduping against the greenhouse /
lever / workday jobs the pipeline already holds.

WHAT THIS USES INSTEAD
----------------------
  A. GET /api/public/companies?page=N      (49 pages, 967 companies)
     Only use of the 500/hr budget: 49 requests.  page IS honoured here
     (page 0 and page 1 return disjoint short_names — checked at runtime).
  B. GET /api/job-search                   (the site's own search backend)
     limit=100 (limit=200 -> HTTP 422), and it returns per-hit STRUCTURED
     employment_types, remote_work_location_type, levels, categories,
     posted_at and locations[].address.  No rate-limit headers, no 429 seen at
     the pace used here.
     *** IT HAS NO WORKING PAGINATOR. ***  Both parameters its own JS bundle
     emits are dead:
        from_param=20/40/100/1000/9980  -> HTTP 200 and byte-identical page 1
                                           (100/100 ids repeat) — the classic
                                           "offset silently ignored" trap.
        start_after=<the cursor string the API itself returned, in the exact
                     "ts,score,id" shape its bundle builds> -> HTTP 422.
     So a distinct query yields at most 100 rows and the ONLY way to go deep is
     to partition.  `count` on the response is the true size of the slice, so
     the sweep below recurses into a slice whenever count > 100:
        company_short_name (967)  ->  category (38 codes)  ->  level (5 codes)
     Beware two more ignored params: `experience_level` (returns the unfiltered
     count) and `sort`/`page`/`offset`/`from`/`after`/`search_after` (all 200 +
     page 1).  Only company_short_name, category, level, query,
     remote_work_location_type, posted_date_range, latlng+radius actually bite.
  C. GET /job/redirect/{legacy_id}   (302, empty body, ~0.13 s)
     THE APPLY URL.  Location: is the employer's real ATS link —
     boards.greenhouse.io, jobs.lever.co, *.myworkdayjobs.com, *.icims.com,
     careers.<employer>.com …  This is the whole reason the pipe is worth
     building: it turns a themuse landing page into a canonical-ATS key the
     rest of the pipeline can dedupe on.  404 = the posting is gone.

LIVE-vs-DEAD DISCRIMINATOR
--------------------------
/api/job-search answers HTTP 200 with {"count":0,"hits":[]} for a company slug
that does not exist — identical to a real company with nothing open.  Status
alone is useless.  Two guards:
  * every slug swept comes from /api/public/companies, so a 0 is a real 0;
  * a transport error / 429 / 5xx raises and is recorded in the state file as
    {"status":"failed"}, never as a completed query, so a re-run retries it
    instead of remembering "this company has no jobs".

RESUMABILITY
------------
  themuse_fetch_state.json   every completed query key -> its `count`
  themuse_apply_cache.json   legacy_id -> resolved ATS url ("" = 404/dead)
  themuse_companies.json     the 967-company roster
Already-emitted job ids are re-read from contract_norm/themuse.jsonl at start,
so a resumed run appends only new rows.  THEMUSE_BUDGET (seconds, default 1500)
bounds a run; drop it / raise it to finish the crawl.

env:
  THEMUSE_BUDGET   wall-clock seconds for this run (default 1500)
  THEMUSE_RPS      global request pace (default 7.0)
  THEMUSE_WORKERS  apply-url resolver threads (default 6)
  THEMUSE_PHASES   comma list of phases to run (default remote,full)
"""
import json
import os
import re
import sys
import threading
import time
import uuid
import concurrent.futures as cf

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "themuse.jsonl")
STATE_JSON = os.path.join(BASE, "themuse_fetch_state.json")
CACHE_JSON = os.path.join(BASE, "themuse_apply_cache.json")
COMPANIES_JSON = os.path.join(BASE, "themuse_companies.json")

SEARCH = "https://www.themuse.com/api/job-search"
COMPANIES_API = "https://www.themuse.com/api/public/companies"
REDIRECT = "https://www.themuse.com/job/redirect/%s"
SITE = "https://www.themuse.com"

BUDGET = float(os.environ.get("THEMUSE_BUDGET") or 1500)
RPS = float(os.environ.get("THEMUSE_RPS") or 8.0)
WORKERS = int(os.environ.get("THEMUSE_WORKERS") or 6)
PHASES = [p for p in (os.environ.get("THEMUSE_PHASES")
                      or "remote,full").split(",") if p]
PAGE_LIMIT = 100          # limit=200 -> HTTP 422
CELL_CAP = 100            # rows reachable per distinct query
FLUSH_EVERY = 300         # resolve+write in batches of this many new jobs

# from the site's own filter bundle (static/chunks/1985-*.js, module 83133)
CATEGORIES = [
    "account_management", "accounting_finance", "administration_office",
    "advertising_marketing", "animal_care", "arts", "business_operations",
    "cleaning_facilities", "computer_it", "construction", "customer_service",
    "data_analytics", "design_ux", "education", "energy_generation_mining",
    "entertainment_travel", "farming_outdoors", "food_hospitality",
    "healthcare", "human_resources_recruitment",
    "installation_maintenance_repairs", "legal", "management",
    "manufacturing_warehouse", "media_pr_communications", "personal_care",
    "product_management", "project_management", "protective_services",
    "real_estate", "retail", "sales", "science_engineering", "social_services",
    "software_engineering", "sports_fitness_recreation",
    "transportation_logistics", "writing_editing"]
LEVELS = ["internship", "entry", "mid", "senior", "management"]

# A FOURTH axis, applied only after category and level have already failed to
# get a cell under the ceiling.  MEASURED 2026-08-20 against the worst cell in
# the state file (company=walmart, category=software_engineering, level=senior,
# count 10000):
#     no query -> 10000 | engineer -> 6,604 | cloud -> 3,852 | python -> 2,179
#     manager/data/director/software -> 10000 | zzzqqq -> 0
# So `query` genuinely bites, and a bogus term proves it is not being ignored
# the way `employment_types` is (bogus employment_types returns the FULL count).
#
# READ THIS BEFORE TRUSTING IT.  These slices are NOT a partition: they
# overlap, and they are not exhaustive, so this raises recall without
# guaranteeing coverage.  That is safe here only because flush() dedupes on
# legacy_id against everything already emitted.  Two other candidate axes were
# probed and rejected the same day: posted_date_range returns count=0 for
# EVERY value including a deliberately bogus one, and latlng+radius returned 0
# for walmart within 25 miles of Boston, which cannot be true.
#
# The mega-employers still do not come apart: `count` maxes out at 10000, which
# is a DISPLAY CAP, not a total, so "10000" means "at least 10000, unknown".
# walmart alone accounts for ten of the 616 saturated cells.  This narrows the
# loss; it does not close it.
QUERY_TOKENS = [t for t in (os.environ.get("THEMUSE_QUERY_TOKENS") or
                            "engineer,nurse,cloud,python,analyst,designer,"
                            "accountant,technician,driver,cashier,pharmacy,"
                            "security,teacher,scientist,attorney,consultant"
                            ).split(",") if t.strip()]
# Days before a completed query cell is re-opened.  Without this the state is a
# PERMANENT watermark, not a resume point: on 2026-08-19 all 8,210 cells were
# marked done, so the run walked all 967 companies, issued 0 queries, wrote 0
# rows and burned its whole 5,400s budget doing it -- three runs in a row.
# A cell with no timestamp is a pre-expiry entry and is treated as EXPIRED,
# which is what re-opens the existing 8,210 on the first run after this change.
TTL_DAYS = float(os.environ.get("THEMUSE_TTL_DAYS") or 7)

# Split order, widest-biting axis first.  sweep() descends only while a cell is
# still over CELL_CAP, so `query` costs nothing on the ~93% of cells that
# already fit and fires only on the 616 that do not.
AXES = [("category", CATEGORIES), ("level", LEVELS), ("query", QUERY_TOKENS)]

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR", "VI", "GU", "AS", "MP"}

T0 = time.time()
_rate_lock = threading.Lock()
_next_t = [0.0]
_pause_until = [0.0]
_lock = threading.Lock()
_tls = threading.local()

STATS = {"queries": 0, "hits": 0, "rows": 0, "redirects": 0, "ats": 0,
         "dead": 0, "failed": 0, "capped": 0}


def left():
    return BUDGET - (time.time() - T0)


def throttle():
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[0]:
                wait = _pause_until[0] - now
            else:
                wait = _next_t[0] - now
                if wait <= 0:
                    _next_t[0] = max(_next_t[0] + 1.0 / RPS, now)
                    return
        time.sleep(min(wait, 3.0))


def sess():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": cc.UA,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            # /api/job-search is the search page's own XHR; without a same-site
            # referer some edges answer with the marketing shell instead.
            "Referer": SITE + "/search",
        })
        _tls.s = s
    return s


class Transient(Exception):
    """A failure that must NOT be recorded as 'this slice has no jobs'."""


def get(url, params=None, tries=4, redirect=False, timeout=40):
    last = "?"
    for i in range(tries):
        throttle()
        try:
            r = sess().get(url, params=params, timeout=timeout,
                           allow_redirects=False if redirect else True)
            if redirect:
                if r.status_code in (301, 302, 303, 307, 308):
                    return r.headers.get("location") or ""
                if r.status_code == 404:
                    return None            # posting really is gone
                last = "HTTP %d" % r.status_code
            elif r.status_code == 200:
                # NEVER trust the status alone: any unknown path under
                # www.themuse.com answers 200 with the Next.js HTML shell.
                body = r.text.lstrip()
                if not body or body[0] == "<":
                    raise Transient("HTML shell for %s" % url)
                try:
                    return r.json()
                except ValueError:
                    raise Transient("non-JSON body for %s" % url)
            else:
                last = "HTTP %d" % r.status_code
            if r.status_code in (429, 503):
                with _rate_lock:
                    _pause_until[0] = time.time() + 20 * (i + 1)
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (i + 1))
    raise Transient(last)


# ---------------------------------------------------------------- state ----
def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return default
    return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


STATE = load_json(STATE_JSON, {"queries": {}, "failed": {}})
STATE.setdefault("queries", {})
STATE.setdefault("failed", {})
# key -> unix ts of the run that completed it.  Kept in a SIDECAR dict rather
# than by changing the shape of STATE["queries"], so an older build of this
# script still reads the counts it expects.
STATE.setdefault("query_ts", {})


def cell_done(key):
    """Completed count for a cell, or None if it must be (re-)run."""
    v = STATE["queries"].get(key, PENDING_Q.get(key))
    if v is None:
        return None
    ts = STATE["query_ts"].get(key)
    if ts is None or (time.time() - ts) > TTL_DAYS * 86400:
        return None
    return v
CACHE = load_json(CACHE_JSON, {})
# Queries answered but whose hits have not been written yet.  A query is only
# promoted into STATE["queries"] once its rows are on disk — recording it any
# earlier would let a kill between "query done" and "rows flushed" mark the
# slice permanently harvested while its rows were never emitted.
PENDING_Q = {}


def emitted_ids():
    """job ids already in contract_norm/themuse.jsonl — the append-only writer
    means a resumed run must not re-emit them."""
    seen = set()
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            for line in f:
                try:
                    seen.add(str(json.loads(line).get("job_id") or ""))
                except ValueError:
                    continue
    seen.discard("")
    return seen


# ------------------------------------------------------------ companies ----
def companies():
    known = load_json(COMPANIES_JSON, None)
    if known and len(known) > 800:
        return known
    out = {}
    page = 0
    first_page_slugs = None
    while page < 200:
        d = get(COMPANIES_API, {"page": page})
        res = d.get("results") or []
        if not res:
            break
        slugs = [c.get("short_name") for c in res if c.get("short_name")]
        if page == 0:
            first_page_slugs = set(slugs)
        elif page == 1 and first_page_slugs and \
                not (first_page_slugs - set(slugs)):
            # page= is honoured here; if it were ignored page 1 would be a
            # verbatim copy of page 0 and the roster would be 20 companies.
            raise Transient("companies ?page= is being ignored")
        for c in res:
            sn = c.get("short_name")
            if sn:
                out[sn] = c.get("name") or sn
        if page + 1 >= int(d.get("page_count") or 0):
            break
        page += 1
    print("  companies: %d (%d pages)" % (len(out), page + 1), flush=True)
    if len(out) > 100:
        save_json(COMPANIES_JSON, out)
    return out


# ------------------------------------------------------------ normalize ----
def split_loc(addr):
    """'Chandler, AZ' -> (Chandler, AZ, US); 'Bahía Blanca, Argentina' ->
    (Bahía Blanca, '', Argentina)."""
    a = (addr or "").strip()
    if not a:
        return "", "", ""
    parts = [p.strip() for p in a.split(",") if p.strip()]
    if len(parts) >= 2:
        tail = parts[-1]
        if tail.upper() in US_STATES:
            return ", ".join(parts[:-1]), tail.upper(), "US"
        if re.fullmatch(r"[A-Z]{2}", tail) and len(parts) == 2:
            return parts[0], "", tail
        return ", ".join(parts[:-1]), "", tail
    return parts[0], "", ""


def norm_row(hit, apply_url, company_slug, company_name):
    locs = hit.get("locations") or []
    addrs = [(l.get("address") or "").strip() for l in locs
             if (l.get("address") or "").strip()]
    rwt = (hit.get("remote_work_location_type") or "").lower()
    remote = rwt == "virtual"
    if remote:
        mode = "Remote"
    elif rwt == "hybrid":
        mode = "Hybrid"
    elif addrs:
        mode = "On-site"
    else:
        mode = ""
    city = state = country = ""
    if addrs:
        city, state, country = split_loc(addrs[0])
    raw_types = " ".join((t.get("code") or t.get("name") or "").replace("_", " ")
                         for t in (hit.get("employment_types") or [])
                         if (t.get("code") or "") != "none")
    short = hit.get("short_title") or ""
    landing = "%s/jobs/%s/%s" % (SITE, company_slug, short) if short else ""
    return cc.row(
        source="themuse", source_type="aggregator",
        company=company_name or (hit.get("company") or {}).get("name") or "",
        title=hit.get("title") or "",
        employment_type=cc.classify_any(raw_types, hit.get("title")),
        work_mode=mode, remote=remote,
        location="; ".join(addrs) if addrs else ("Remote" if remote else ""),
        city=city, state=state, country=country,
        posted=cc.ymd(ts=hit.get("posted_at")),
        apply_url=apply_url or landing,
        job_id=hit.get("legacy_id") or hit.get("id") or "",
        board=company_slug,
        board_url="%s/search/jobs/c-%s" % (SITE, company_slug))


# ------------------------------------------------------- apply-url layer ----
def untrack(url):
    """Some employers route the Muse link through an ad tracker
    (ad.doubleclick.net/ddm/clk/...;...?https://careers.employer.com/...).
    The canonical-ATS parser downstream would key that on doubleclick.net, so
    unwrap it: the destination is the query string when the query itself is a
    URL.  Conservative on purpose — anything else is passed through as-is."""
    if not url:
        return url
    q = url.split("?", 1)
    if len(q) == 2 and q[1][:8].lower().startswith(("http://", "https://")):
        return q[1]
    return url


def resolve_apply(legacy_id):
    """302 Location off /job/redirect/{id} == the employer's own ATS URL.
    '' means the posting 404s (dead) — cached so it is asked once, ever.
    The cache holds the RAW Location; unwrapping happens on the way out so a
    change here does not need the cache thrown away."""
    key = str(legacy_id)
    if key in CACHE:
        return untrack(CACHE[key])
    try:
        loc = get(REDIRECT % key, redirect=True, tries=3, timeout=30)
    except Transient:
        return None                       # transient: do NOT cache, retry later
    with _lock:
        STATS["redirects"] += 1
        if loc is None:
            STATS["dead"] += 1
            CACHE[key] = ""
            return ""
        # a Location back onto themuse.com is the hosted-apply case; it is not
        # an employer ATS link, so the landing page is just as good.
        host = loc.split("/")[2].lower() if loc.startswith("http") else ""
        if not host or host.endswith("themuse.com"):
            CACHE[key] = ""
            return ""
        STATS["ats"] += 1
        CACHE[key] = loc
        return untrack(loc)


def flush(pending, writer, seen):
    """Resolve apply urls for a batch, then write the rows."""
    todo = [j for j in pending.values()
            if str(j["hit"].get("legacy_id") or "") not in seen]
    if not todo:
        pending.clear()
        STATE["queries"].update(PENDING_Q)
        PENDING_Q.clear()
        save_json(STATE_JSON, STATE)
        return
    ids = [j["hit"].get("legacy_id") for j in todo]
    res = {}
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for jid, url in zip(ids, ex.map(resolve_apply, ids)):
            res[jid] = url
    for j in todo:
        h = j["hit"]
        jid = h.get("legacy_id")
        url = res.get(jid)
        if url is None:                   # transient failure -> try next run
            continue
        writer.write(norm_row(h, url, j["slug"], j["name"]))
        seen.add(str(jid))
        STATS["rows"] += 1
    pending.clear()
    writer.f.flush()
    save_json(CACHE_JSON, CACHE)
    STATE["queries"].update(PENDING_Q)
    PENDING_Q.clear()
    save_json(STATE_JSON, STATE)


# ------------------------------------------------------------- the sweep ----
def qkey(params):
    return "&".join("%s=%s" % (k, params[k]) for k in sorted(params))


def run_query(params):
    p = dict(params)
    p.update({"limit": PAGE_LIMIT, "query": "",
              "preference": uuid.uuid4().hex})
    d = get(SEARCH, p)
    hits = [h.get("hit") or {} for h in (d.get("hits") or [])]
    return hits, int(d.get("count") or 0)


def sweep(base, axes, companies_map, pending, writer, seen, depth=0):
    """Depth-first partition.  A slice whose `count` exceeds the 100-row
    ceiling is split on the next axis; when the axes run out we log the loss
    instead of pretending we got it all."""
    key = qkey(base)
    done = cell_done(key)
    if done is None:
        if left() <= 0:
            return False
        try:
            hits, count = run_query(base)
        except Transient as e:
            STATE["failed"][key] = str(e)[:120]
            STATS["failed"] += 1
            print("    ! %s -> %s" % (key, e), flush=True)
            return True
        STATE["failed"].pop(key, None)
        PENDING_Q[key] = count
        STATE["query_ts"][key] = time.time()
        STATS["queries"] += 1
        STATS["hits"] += len(hits)
        for h in hits:
            jid = str(h.get("legacy_id") or "")
            if not jid or jid in seen or jid in pending:
                continue
            comp = h.get("company") or {}
            slug = comp.get("short_name") or base.get("company_short_name", "")
            pending[jid] = {"hit": h, "slug": slug,
                            "name": comp.get("name")
                            or companies_map.get(slug, "")}
        if len(pending) >= FLUSH_EVERY:
            flush(pending, writer, seen)
        done = count
    if done > CELL_CAP:
        if not axes:
            STATS["capped"] += done - CELL_CAP
            return True
        axis, values = axes[0]
        for v in values:
            sub = dict(base)
            sub[axis] = v
            if not sweep(sub, axes[1:], companies_map, pending, writer, seen,
                         depth + 1):
                return False
    return left() > 0


def main():
    print("themuse_fetch: budget %.0fs, %.1f rps, %d workers, phases=%s"
          % (BUDGET, RPS, WORKERS, ",".join(PHASES)), flush=True)
    seen = emitted_ids()
    print("  %d job ids already in %s" % (len(seen), OUT), flush=True)
    try:
        cmap = companies()
    except Transient as e:
        print("  FATAL: company roster unavailable (%s)" % e, flush=True)
        return 1
    slugs = sorted(cmap)
    writer = cc.Writer(OUT)
    pending = {}
    try:
        # PHASE 1 — the remote slice (count=6852 board-wide), first and in
        # full.  Per COMPANY first: a company's remote openings almost always
        # fit under the 100-row cell cap, whereas the board-wide category cells
        # do not, and their only remaining split would be the 967-slug company
        # axis *inside every one of the 190 category x level cells*.  The
        # board-wide pass runs afterwards as a safety net for any company that
        # is not on the /api/public/companies roster.
        if "remote" in PHASES and left() > 0:
            print("  phase remote (remote_work_location_type=virtual)",
                  flush=True)
            for i, slug in enumerate(slugs):
                if left() <= 0:
                    break
                sweep({"remote_work_location_type": "virtual",
                       "company_short_name": slug},
                      AXES,
                      cmap, pending, writer, seen)
                if i % 100 == 0:
                    flush(pending, writer, seen)
                    print("    remote %d/%d companies | %d queries | %d rows "
                          "| %.0fs left" % (i, len(slugs), STATS["queries"],
                                            STATS["rows"], left()), flush=True)
            if left() > 0:
                sweep({"remote_work_location_type": "virtual"},
                      AXES,
                      cmap, pending, writer, seen)
            flush(pending, writer, seen)
            print("    remote done: %d rows total, %.0fs left"
                  % (STATS["rows"], left()), flush=True)
        # PHASE 2 — everything else, company by company (a company is the one
        # axis whose slices are mostly under the 100-row ceiling).
        if "full" in PHASES:
            for i, slug in enumerate(slugs):
                if left() <= 0:
                    print("    budget spent at company %d/%d (%s)"
                          % (i, len(slugs), slug), flush=True)
                    break
                sweep({"company_short_name": slug},
                      AXES,
                      cmap, pending, writer, seen)
                if i % 25 == 0:
                    flush(pending, writer, seen)
                    print("    %d/%d companies | %d queries | %d rows | "
                          "%.0fs left" % (i, len(slugs), STATS["queries"],
                                          STATS["rows"], left()), flush=True)
    finally:
        flush(pending, writer, seen)
        writer.close()
        save_json(CACHE_JSON, CACHE)
        save_json(STATE_JSON, STATE)
    print("themuse_fetch done: %d rows written this run "
          "(%d queries, %d hits, %d ats urls, %d dead, %d failed queries, "
          "~%d rows lost to the 100-row cell cap)"
          % (STATS["rows"], STATS["queries"], STATS["hits"], STATS["ats"],
             STATS["dead"], STATS["failed"], STATS["capped"]), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
