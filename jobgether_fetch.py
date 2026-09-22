#!/usr/bin/env python3
"""
jobgether_fetch.py — harvest jobgether.com, an all-remote aggregator.

WHY THIS PIPE IS WORTH HAVING
  Every offer on jobgether is remote (remoteOfferType is always "Full Remote";
  the JSON-LD on the offer page says jobLocationType TELECOMMUTE), the live
  inventory is ~136k, and — the part that matters for this index — the row
  carries the EMPLOYER's own ATS link, clean, with no tracking query:

      "applyUrl": "https://job-boards.greenhouse.io/commvault/jobs/5368660008"

  so all_jobs_build.canon_key() can recognise these rows as duplicates of
  greenhouse/lever/ashby/workday/icims/... rows the index already holds, and
  the ones that are NOT duplicates are net-new remote inventory.

THE ENDPOINT
  GET https://jobgether.com/astroapi/offer/search?<filters>&page=N
  -> {"data":[50 offers], "total":N, "limit":50, "skip":0, "page":N,
      "maxPages":M}
  Unauthenticated.  Needs a browser User-Agent (a bare urllib UA is 403'd at
  the Cloudflare edge) and answers text/plain, not application/json.
  This is the XHR behind the search page's Astro island (OfferList.*.js); it is
  not documented anywhere.

  MEASURED TRAPS (2026-07-29) — every one of these answers HTTP 200:
    * page is CLAMPED TO 10.  page=11/12/50 come back with the page-1 rows and
      "page":1 echoed in the envelope, so a naive walk "collects" the same 50
      rows forever.  maxPages in the envelope says 2722 — it is a lie, the
      client itself does Math.min(ceil(total/50), 10).  500 rows is the whole
      reachable window for ANY one query.
    * limit and skip are silently IGNORED (limit=200 -> 50 rows, "limit":50;
      skip=500 -> the page-1 rows).  Paging is page= only.
    * contractType is silently IGNORED — contractType=Freelance returns the
      unfiltered feed with an unchanged total.  It looks like a partition and
      is not one; do NOT use it to split a big company.
    * an UNKNOWN param is silently ignored too ("companies=commvault" returns
      the unfiltered 136k feed), so a typo'd filter reads as a huge success.
    * company= takes the 24-hex Mongo id ONLY.  company=<slug> is HTTP 400 —
      that 400 is the one loud failure in the whole API and is how we know the
      id is real.
  Filters VERIFIED to actually partition: company (id), jobReferences (id),
  locations (slug), keyword (text).

WHY NOT THE SITEMAP
  jobgether's /sitemap/offers-{1..7}.xml holds 101,027 <loc> (99,706 unique —
  it double-lists ~1.3k offers).  That is a SUBSET of the 136k live offers, one
  page fetch each (43KB gzipped vs 330 bytes/offer here), the apply link is not
  in the JSON-LD (it is an outbound href in the body / the data-offer attribute
  on <body>, carrying utm junk), and a removed offer answers HTTP 410.  The
  search API dominates it on every axis, so the sitemap is used for exactly one
  thing: /sitemap/index-company-landings.xml, the company universe.

PARTITION STRATEGY
  Every offer belongs to exactly one company, so company partitions are
  disjoint and their union is the whole board, and almost every company fits
  inside the 500-row window (the biggest sampled was ~120 offers).  Phases run
  D -> B -> A -> B:

    phase D  DISCOVERY.  The unfiltered feed, then one `locations=<slug>`
             sweep per location slug seen so far.  A company partition can
             never reveal a NEW company, so this is the only thing that can,
             and it has to be: the company-landing sitemap holds 2,655
             companies but the first 2.7k offers already carried 1,486
             distinct companies of which 762 had no landing page.  CAPPED
             (JG_D_MAX) because location partitions overlap enormously — their
             value is the ids, and those saturate fast.
    phase B  HARVEST.  /astroapi/offer/search?company=<id>&page=1..N for every
             known company, N = min(10, ceil(total/50)).  A company over the
             500 window is drilled down by jobReferences id (taken off its own
             offers) — the only within-company filter the API honours.  Loops
             in rounds because D and B both keep adding companies.
    phase A  BACKFILL.  /remote-jobs/company-<slug> for every landing slug not
             yet resolved, scraping the company Mongo id out of the SSR island
             props.  60KB of HTML for one id and no rows, so it runs LAST — but
             it is the only route to a company no sweep ever surfaced.  Cached
             in state forever; a re-run never refetches a resolved slug.

POLITENESS / RATE
  Cloudflare rate-limits this host, and the limit TIGHTENS over a session: 6
  good rps early on, ~1 good rps after a few thousand requests from the same
  IP, with Retry-After: 60 on every bounce.  The header is misleading — the
  limiter is a token bucket, so a bounced request usually succeeds 5-10s later
  while honouring Retry-After globally would idle the crawl 60s at a time.  So
  a 429 slows the shared bucket and sleeps just that caller; only 8 in a row
  with no success in between escalates to a global pause.  robots.txt asks for
  Crawl-delay 2 (3 days for the whole board); we run a couple of rps for a
  bounded window and never re-fetch what state says is done.

RESUMABILITY
  jobgether_fetch_state.json records, per partition, done/failed plus the
  company id map and the location-slug universe.  A transient failure is
  recorded as "failed" (retried next run), never as "done with 0 rows" — the
  two are different and conflating them is how a board silently disappears
  from the index.  Offer ids already present in contract_norm/jobgether.jsonl
  are loaded at start-up and skipped, so a resumed or overlapping run cannot
  double-write a row.  Re-running until "0 pending" is how you finish a crawl
  that a time budget cut short.

ENV
  JG_RPS        target requests/sec, default 5.0 (back-off adapts downward)
  JG_WORKERS    threads, default 4
  JG_MAX_MIN    wall-clock budget in minutes, default 0 (= no budget)
  JG_D_MAX      discovery sweeps per run, default 40
  JG_ONLY       comma list of phases to run, default "a,b,d"
  JG_LIMIT      cap partitions per phase (development only)
"""
import json
import math
import os
import random
import re
import sys
import threading
import time
import concurrent.futures as cf

import requests

from datetime import datetime, timezone

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "jobgether.jsonl")
STATE = os.path.join(BASE, "jobgether_fetch_state.json")

HOST = "https://jobgether.com"
API = HOST + "/astroapi/offer/search"
COMPANY_SITEMAP = HOST + "/sitemap/index-company-landings.xml"
LANDING = HOST + "/remote-jobs/company-%s"

PAGE_CAP = 10          # server clamps page>10 back to page 1 (measured)
PAGE_SIZE = 50         # limit= is ignored; 50 is fixed
WINDOW = PAGE_CAP * PAGE_SIZE   # 500 reachable rows per query

RPS = float(os.environ.get("JG_RPS") or 5.0)
WORKERS = int(os.environ.get("JG_WORKERS") or 4)
MAX_MIN = float(os.environ.get("JG_MAX_MIN") or 0)
LIMIT = int(os.environ.get("JG_LIMIT") or 0)   # cap partitions per phase (dev)
D_MAX = int(os.environ.get("JG_D_MAX") or 40)  # discovery sweeps per run
# REFRESH TTL, added 2026-09-04.  "done" was an UNTIMESTAMPED terminal marker,
# so once a company partition was crawled it was never crawled again: this
# pipe wrote 0 rows on 11 of its last 14 runs while still costing up to 90
# minutes of budget.  themuse had the identical completed-watermark bug and
# was fixed; its twin was not.  jobgether is also one of the `remote` family
# that run_all deliberately never state_refreshes -- correctly, because this
# file also holds the slug_ids map that costs ~8,977s to rebuild -- so the
# expiry has to live here, keyed per company, rather than in rotate_family.
#
# A FIFTH of the expired partitions per run, not a third: this host paces with
# Cloudflare and its throughput degrades within a session, so a big re-queue
# buys 429s rather than rows.  Five buckets also means the whole corpus cycles
# in five runs, which is inside the TTL.
TTL_DAYS = float(os.environ.get("JG_TTL_DAYS") or 7)
TTL_SHARDS = int(os.environ.get("JG_TTL_SHARDS") or 5)
PHASES = [p.strip() for p in (os.environ.get("JG_ONLY") or "a,b,d").split(",")
          if p.strip()]
TRIES = 6
TIMEOUT = 45

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

_deadline = [0.0]
_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_tls = threading.local()

_state = {}
_seen = set()          # offer ids already written (this run or a previous one)
_rows = [0]
_n429 = [0]
_nfail = [0]
_ok_streak = [0]
_bounce = [0]      # consecutive 429s with no 200 in between


def _ok():
    """Walk the interval back toward the configured rate after a clean run of
    requests, so one 429 does not permanently halve the crawl."""
    with _rate_lock:
        _bounce[0] = 0
        _ok_streak[0] += 1
        if _ok_streak[0] >= 40 and _interval[0] > 1.0 / RPS:
            _ok_streak[0] = 0
            _interval[0] = max(_interval[0] / 1.1, 1.0 / RPS)


# ---- http ------------------------------------------------------------------
def budget_left():
    return _deadline[0] <= 0 or time.time() < _deadline[0]


def throttle():
    while True:
        with _rate_lock:
            now = time.time()
            if now >= _pause_until[0]:
                wait = _next_t[0] - now
                if wait <= 0:
                    _next_t[0] = max(_next_t[0] + _interval[0], now)
                    return
            else:
                wait = _pause_until[0] - now
        time.sleep(min(max(wait, 0.0), 5.0))


def sess():
    s = getattr(_tls, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": HOST + "/search-offers",
        })
        _tls.s = s
    return s


class Transient(Exception):
    """The host did not answer usefully.  NOT the same as 'no jobs'."""


def _cool(resp):
    """Back-off for a 429.

    Cloudflare answers Retry-After: 60, but that header is a lie about this
    limiter's behaviour: it is a token bucket, and a retry 5-10s later
    routinely succeeds while a *sibling* request in the same second still
    429s.  Measured 2026-07-29 at a steady 1.2 rps: 66 x 200 against 18 x 429,
    i.e. a fifth of requests bounce and every one of them was fine on the next
    try.  Honouring Retry-After globally on each of those bounces pauses the
    whole crawl for 60s a fifth of the time and drops real throughput below
    0.2 rps — that is what the first cut of this function did.

    So: a 429 slows the shared bucket and sleeps the ONE caller briefly.  Only
    a run of consecutive 429s with no success in between (i.e. an actual block,
    not bucket noise) escalates to the full global pause.
    """
    try:
        ra = float(resp.headers.get("Retry-After") or 0)
    except ValueError:
        ra = 0
    with _rate_lock:
        _n429[0] += 1
        _ok_streak[0] = 0
        _bounce[0] += 1
        _interval[0] = min(_interval[0] * 1.15, 2.0)   # floor 0.5 rps
        hard = _bounce[0] >= 8
        if hard:
            _bounce[0] = 0
            wait = min(max(ra, 30.0), 90.0)
            if time.time() >= _pause_until[0]:
                _pause_until[0] = time.time() + wait
                print("    429 x8 in a row (#%d total) -> global pause %.0fs, "
                      "rate now %.2f rps"
                      % (_n429[0], wait, 1.0 / _interval[0]), flush=True)
    if not hard:
        time.sleep(4 + 8 * random.random())


def api(**params):
    """One search call.  Returns the envelope dict, or raises Transient.

    Never returns {} for a transport problem: an empty 'data' from this
    function means the query genuinely matched nothing.
    """
    last = ""
    for attempt in range(TRIES):
        if not budget_left():
            raise Transient("out of time budget")
        throttle()
        try:
            r = sess().get(API, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 429:
            _cool(r)
            last = "429"
            continue
        if r.status_code == 400:
            # the only loud failure this API has: a malformed company id.
            return {"__bad__": True, "data": [], "total": 0}
        if r.status_code != 200:
            last = "HTTP %d" % r.status_code
            time.sleep(2 * (attempt + 1))
            continue
        body = r.text.lstrip()
        # status 200 + an HTML shell is this host's house style for anything it
        # does not recognise, so the body is what decides, not the code.
        if not body or body[0] not in "{[":
            last = "non-json body (HTTP 200, %d bytes)" % len(r.text)
            time.sleep(2 * (attempt + 1))
            continue
        try:
            d = json.loads(body)
        except ValueError:
            last = "unparseable json"
            time.sleep(2 * (attempt + 1))
            continue
        if not isinstance(d, dict) or "data" not in d:
            last = "json without a data key"
            time.sleep(2 * (attempt + 1))
            continue
        _ok()
        return d
    raise Transient(last or "unknown")


# ---- phase A: company id map ----------------------------------------------
# The landing page is an SSR search page; the OfferList island's props carry
# the selected company as {label, slug, value:<24hex>}.  A slug that no longer
# exists 301s to /search-offers?company=<slug>, which renders WITHOUT that
# block — presence of the 24-hex value is the live/dead discriminator, not the
# status code (the redirect target is a normal HTTP 200 page).
_COMPANY_RX = re.compile(
    r"&quot;company&quot;:\[0,\{&quot;label&quot;:\[0,&quot;(.*?)&quot;\],"
    r"&quot;slug&quot;:\[0,&quot;(.*?)&quot;\],"
    r"&quot;value&quot;:\[0,&quot;([0-9a-f]{24})&quot;\]")


def landing_company(slug):
    """(company_id, name) for a landing slug, or (None, None) if dead."""
    last = ""
    for attempt in range(TRIES):
        if not budget_left():
            raise Transient("out of time budget")
        throttle()
        try:
            r = sess().get(LANDING % slug, timeout=TIMEOUT,
                           headers={"Accept": "text/html"})
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 429:
            _cool(r)
            last = "429"
            continue
        if r.status_code == 404:
            return None, None
        if r.status_code != 200:
            last = "HTTP %d" % r.status_code
            time.sleep(2 * (attempt + 1))
            continue
        m = _COMPANY_RX.search(r.text)
        if m:
            _ok()
            return m.group(3), _unesc(m.group(1))
        if "astro-island" in r.text:
            _ok()
            return None, None          # really rendered, really no company
        last = "html shell without an astro-island"
        time.sleep(2 * (attempt + 1))
    raise Transient(last or "unknown")


def _unesc(s):
    for a, b in (("&#39;", "'"), ("&amp;", "&"), ("&quot;", '"'),
                 ("&lt;", "<"), ("&gt;", ">")):
        s = s.replace(a, b)
    return s


def company_slugs():
    """The company universe, from the sitemap (cached in state)."""
    slugs = _state.get("company_slugs")
    if slugs:
        return slugs
    throttle()
    r = sess().get(COMPANY_SITEMAP, timeout=TIMEOUT,
                   headers={"Accept": "application/xml"})
    if r.status_code != 200 or "<loc>" not in r.text:
        raise Transient("company sitemap: HTTP %d, %d bytes"
                        % (r.status_code, len(r.text)))
    slugs = []
    for loc in re.findall(r"<loc>(.*?)</loc>", r.text):
        tail = loc.rstrip("/").rsplit("/", 1)[-1]
        if tail.startswith("company-"):
            slugs.append(tail[len("company-"):])
    slugs = sorted(set(slugs))
    if not slugs:
        raise Transient("company sitemap parsed to 0 slugs")
    _state["company_slugs"] = slugs
    save_state()
    return slugs


# ---- row mapping -----------------------------------------------------------
_PAREN = re.compile(r"^(.+?)\s*\((.+)\)$")


def num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    if not f or f != f or f in (float("inf"), float("-inf")):
        return ""
    return round(f)


def place(offer):
    """(location, city, state, country) from the structured country list.

    jobgether names US states as 'California (USA)' and countries bare
    ('Brazil'), and a remote offer is routinely open to several at once — so
    city/state/country are only filled when there is exactly one, and the
    joined string (';'-separated, which is what all_jobs_build's statewide
    matcher splits on) always carries the full set.
    """
    names = [c.get("name") for c in (offer.get("countries") or [])
             if c.get("name")]
    conts = [c.get("name") if isinstance(c, dict) else c
             for c in (offer.get("continents") or [])]
    conts = [c for c in conts if c]
    allnames = names + [c for c in conts if c not in names]
    loc = "; ".join(allnames) or (offer.get("requiredLocations") or "")
    city = state = country = ""
    if len(names) == 1:
        m = _PAREN.match(names[0])
        if m:
            state, country = m.group(1).strip(), m.group(2).strip()
        else:
            country = names[0].strip()
    return loc or "Remote", city, state, country


def salary(offer):
    """jobgether stores one point value per offer, and 0 when it has none.
    Verified against the offer page's JSON-LD baseSalary: when both exist they
    agree exactly, so this is the employer's figure, not an invention."""
    s = offer.get("salary") or {}
    lo, hi = num(s.get("min")), num(s.get("max"))
    if not lo and not hi:
        lo = hi = num(s.get("average"))
    if not lo and not hi:
        return "", "", "", ""
    cur = (s.get("currency") or "").upper()
    ref = lo or hi
    # a point value in the hundreds is a rate, not an annual figure
    per = "Year" if ref >= 2000 else "Hour"
    return lo, hi or lo, cur, per


def to_row(offer):
    cd = offer.get("companyData") or {}
    rot = offer.get("remoteOfferType") or ""
    # structured remote flag; jobgether is an all-remote board but do not
    # hardcode it — read the field, so a future hybrid listing is labelled
    # honestly instead of being laundered into the remote index.
    is_remote = "remote" in rot.lower()
    wm = "Remote" if is_remote else cc.work_mode(text=rot)
    loc, city, st, ctry = place(offer)
    lo, hi, cur, per = salary(offer)
    slug = offer.get("slug") or offer.get("_id") or ""
    board = cd.get("slug") or ""
    board_url = ("%s/remote-jobs/company-%s" % (HOST, board) if board else
                 "%s/search-offers?company=%s"
                 % (HOST, offer.get("company") or ""))
    apply_url = (offer.get("applyUrl") or "").strip()
    if not apply_url:
        # jobgether-hosted "easy apply" offers have no outbound ATS link; the
        # offer page is then the only place to apply.  Rare (0/500 in the
        # sample) but it must not produce an empty apply_url.
        apply_url = "%s/offer/%s" % (HOST, slug)
    return cc.row(
        source="jobgether", source_type="remote_board",
        company=cd.get("name") or "",
        title=offer.get("title") or "",
        # classify_any, never classify: contractType is "Full time" for ~90%
        # of the board and cc.classify returns None for those, which would
        # throw away nine rows in ten.
        employment_type=cc.classify_any(offer.get("contractType"),
                                        offer.get("title")),
        work_mode=wm, remote=is_remote,
        location=loc, city=city, state=st, country=ctry,
        salary_min=lo, salary_max=hi, salary_currency=cur, salary_period=per,
        posted=cc.ymd(iso=offer.get("createdAt")),
        apply_url=apply_url,
        job_id=offer.get("_id") or "",
        board=board or (offer.get("company") or ""), board_url=board_url)


# ---- crawling --------------------------------------------------------------
_new_companies = {}
_loc_slugs = set()


def harvest(offers, out):
    """Write the offers we have not written before; note unseen companies."""
    n = 0
    with _lock:
        for o in offers:
            oid = o.get("_id")
            cd = o.get("companyData") or {}
            cid = o.get("company") or cd.get("_id")
            # Queue the company even for an offer we already hold — discovery
            # must not depend on which partition reached the offer first.  It
            # goes straight into the STATE, not a scratch dict: the landing
            # sitemap is not the company universe (1,486 distinct companies
            # showed up in the first 2.7k offers, 762 of them with no landing
            # page at all), so these ids are the crawl's real backlog and they
            # have to survive a restart.
            if cid and cid not in _state["companies_by_id"]:
                _state["companies_by_id"][cid] = {"status": "pending",
                                                  "key": cd.get("slug") or "",
                                                  "via": "offer"}
                _new_companies.setdefault(cid, cd.get("slug") or "")
            _loc_slugs.update(s for s in (o.get("requiredLocationsIds") or [])
                              if isinstance(s, str))
            if not oid or oid in _seen:
                continue
            _seen.add(oid)
            out.write(to_row(o))
            n += 1
        _rows[0] += n
    return n


def walk(out, label, **filters):
    """Page 1..min(10, total/50) of one query.  Returns (total, fetched).

    Raises Transient so the caller can mark the partition failed rather than
    done-with-zero.
    """
    d = api(page=1, **filters)
    if d.get("__bad__"):
        return -1, 0
    total = int(d.get("total") or 0)
    got = harvest(d.get("data") or [], out)
    pages = min(PAGE_CAP, max(1, math.ceil(total / PAGE_SIZE)))
    for p in range(2, pages + 1):
        d = api(page=p, **filters)
        rows = d.get("data") or []
        if not rows:
            break
        # page> the real end silently re-serves page 1; harvest() dedupes on
        # offer id so that cannot inflate the count, but stop anyway.
        if int(d.get("page") or p) != p:
            break
        got += harvest(rows, out)
    return total, got


def drill(out, cid, out_of_window):
    """A company past the 500-row window, split by jobReferences id.

    jobReferences is the only within-company filter the API honours
    (contractType is accepted and ignored), and the ids come off the offers we
    already hold, so no extra taxonomy fetch is needed.
    """
    jrs = sorted(out_of_window)
    print("    ! company %s exceeds the 500 window; drilling %d jobReferences"
          % (cid, len(jrs)), flush=True)
    got = 0
    for jr in jrs:
        if not budget_left():
            break
        try:
            _t, n = walk(out, "jr", company=cid, jobReferences=jr)
            got += n
        except Transient as e:
            print("      jr %s -> %s" % (jr, e), flush=True)
    return got


def do_company(out, key, cid):
    """Crawl one company partition.  Returns rows written."""
    seen_jr = set()
    d = api(company=cid, page=1)
    if d.get("__bad__"):
        # company= only accepts the 24-hex id; a 400 means our id is junk.
        with _lock:
            _state["companies_by_id"][cid] = {"status": "bad_id", "key": key}
        return 0
    total = int(d.get("total") or 0)
    rows = d.get("data") or []
    got = harvest(rows, out)
    for o in rows:
        seen_jr.update(o.get("jobReferences") or [])
    pages = min(PAGE_CAP, max(1, math.ceil(total / PAGE_SIZE)))
    for p in range(2, pages + 1):
        d = api(company=cid, page=p)
        rows = d.get("data") or []
        if not rows or int(d.get("page") or p) != p:
            break
        got += harvest(rows, out)
        for o in rows:
            seen_jr.update(o.get("jobReferences") or [])
    if total > WINDOW:
        got += drill(out, cid, seen_jr)
    with _lock:
        # `at` is what makes "done" expirable.  An undated done marker is a
        # permanent veto, which is exactly how this pipe froze.
        _state["companies_by_id"][cid] = {"status": "done", "key": key,
                                          "total": total, "rows": got,
                                          "at": _today()}
    return got


# ---- state -----------------------------------------------------------------
def load_state():
    st = cc.load_state(STATE)
    st.setdefault("company_slugs", [])
    st.setdefault("slug_ids", {})        # landing slug -> id | "" (dead)
    st.setdefault("companies_by_id", {})  # company id -> {status,...}
    st.setdefault("sweeps", {})          # phase-D query key -> status
    return st


def save_state():
    cc.save_state(STATE, _state)


def load_seen():
    """Offer ids already in the output file, so a resume cannot double-write."""
    if not os.path.exists(OUT):
        return
    bad = 0
    with open(OUT, encoding="utf-8") as f:
        for line in f:
            try:
                _seen.add(json.loads(line).get("job_id"))
            except ValueError:
                bad += 1
    _seen.discard(None)
    _seen.discard("")
    if bad:
        print("  %d unparseable lines in %s (ignored)" % (bad, OUT), flush=True)


# ---- phases ----------------------------------------------------------------
def phase_a(pool):
    """Resolve landing slug -> company id.  One fetch per company, EVER:
    the map is cached in state and a re-run starts straight at phase B."""
    slugs = company_slugs()
    todo = [s for s in slugs if s not in _state["slug_ids"]]
    have = len(slugs) - len(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("phase A: %d companies, %d already resolved, %d to fetch"
          % (len(slugs), have, len(todo)), flush=True)
    if not todo:
        return

    def one(slug):
        if not budget_left():
            return slug, None, None, "budget"
        try:
            cid, name = landing_company(slug)
        except Transient as e:
            return slug, None, None, str(e)
        return slug, cid, name, None

    done = 0
    # as_completed, not map: map yields in submission order, so one slow slug
    # stalls the progress display behind it and a wedged crawl looks identical
    # to a healthy one.
    futs = [pool.submit(one, s) for s in todo]
    for fut in cf.as_completed(futs):
        slug, cid, name, err = fut.result()
        done += 1
        with _lock:
            if err:
                if err != "budget":
                    _nfail[0] += 1
            else:
                _state["slug_ids"][slug] = cid or ""
                if cid and cid not in _state["companies_by_id"]:
                    _state["companies_by_id"][cid] = {"status": "pending",
                                                      "key": slug,
                                                      "name": name or ""}
            if done % 100 == 0:
                save_state()
                print("  A %d/%d resolved (%d dead, %d transient, %d 429s)"
                      % (done, len(todo),
                         sum(1 for v in _state["slug_ids"].values() if not v),
                         _nfail[0], _n429[0]), flush=True)
        if not budget_left():
            print("  A: time budget reached at %d/%d" % (done, len(todo)),
                  flush=True)
            break
    save_state()


def phase_b(pool, out, rounds=6):
    """Crawl every known company partition, re-checking the backlog after each
    round: a sweep in phase D keeps adding companies while B is running."""
    for rnd in range(rounds):
        if not budget_left():
            return
        n = _phase_b_round(pool, out, rnd + 1)
        if not n:
            return


def _today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _expired(cid, v):
    """Is this 'done' partition old enough, and in this run's shard, to redo?

    An entry written before the TTL existed has no `at` at all; those are the
    frozen ones, so they are the whole point and count as expired.  Sharding is
    on the company id so a partition lands in the same bucket every run, and
    the bucket rotates with the date -- five consecutive runs therefore cover
    the corpus exactly once, with no run paying for more than a fifth of it.
    """
    if not TTL_DAYS:
        return False
    at = v.get("at") or ""
    try:
        age = (datetime.now(timezone.utc)
               - datetime.strptime(at, "%Y-%m-%d").replace(tzinfo=timezone.utc)
               ).days
    except ValueError:
        age = 10 ** 6          # never stamped: frozen since before the TTL
    if age < TTL_DAYS:
        return False
    day = datetime.now(timezone.utc).toordinal()
    try:
        bucket = int(cid) % TTL_SHARDS
    except (TypeError, ValueError):
        bucket = sum(map(ord, str(cid))) % TTL_SHARDS
    return bucket == day % TTL_SHARDS


def _phase_b_round(pool, out, rnd):
    with _lock:
        todo = [(cid, dict(v)) for cid, v in _state["companies_by_id"].items()
                if v.get("status") != "done" or _expired(cid, v)]
    if LIMIT:
        todo = todo[:LIMIT]
    print("phase B round %d: %d company partitions known, %d to crawl"
          % (rnd, len(_state["companies_by_id"]), len(todo)), flush=True)
    if not todo:
        return 0

    def one(item):
        cid, v = item
        if not budget_left():
            return cid, None, "budget"
        try:
            return cid, do_company(out, v.get("key") or "", cid), None
        except Transient as e:
            return cid, None, str(e)

    done = 0
    futs = [pool.submit(one, it) for it in todo]
    for fut in cf.as_completed(futs):
        cid, n, err = fut.result()
        done += 1
        with _lock:
            if err:
                if err != "budget":
                    _nfail[0] += 1
                    # failed != empty.  Left pending so the next run retries.
                    v = _state["companies_by_id"].setdefault(cid, {})
                    v["status"] = "failed"
                    v["error"] = err[:120]
            if done % 50 == 0:
                save_state()
                print("  B %d/%d companies | %d rows | %d 429s | %d failed | "
                      "%.2f rps" % (done, len(todo), _rows[0], _n429[0],
                                    _nfail[0], 1.0 / _interval[0]), flush=True)
        if not budget_left():
            print("  B: time budget reached at %d/%d" % (done, len(todo)),
                  flush=True)
            break
    save_state()
    return done


def phase_d(pool, out, cap=D_MAX):
    """DISCOVERY, not harvest.  API only, no HTML.

    A company partition can never reveal a new company (all its offers are its
    own), so something has to introduce companies that the landing sitemap
    misses — and it misses a lot.  These sweeps do it: the unfiltered feed,
    then one `locations=<slug>` partition per location slug seen so far, each
    worth up to 500 offers and, more importantly, the companyData._id on every
    one of them.

    Deliberately CAPPED.  Location partitions overlap enormously (they are
    each the top 500 by relevance for that location, and the popular ones share
    most of their rows): measured, sweeps past the first few dozen returned
    ~200 new offers a minute against ~2,000 fetched.  Their value is the
    company ids, which saturate quickly, so a few dozen sweeps buys the
    discovery and phase B — where every request is a disjoint partition — gets
    the rest of the budget.
    """
    done_lab = set(k for k, v in _state["sweeps"].items() if v == "done")
    # Re-seed the slug queue from state.  Without this a resumed run finds
    # "feed" already done, has nothing to sweep, and can never discover a new
    # location slug — because the slugs only arrive on the offers a sweep
    # returns.
    _loc_slugs.update(_state.get("loc_slugs") or [])
    if not _loc_slugs:
        # and if we have no slugs at all, the feed has to be re-run whatever
        # state says, because it is the only thing that can produce them.
        done_lab.discard("feed")
    filt_of = {"feed": {}}
    queued = ["feed"]
    for s in sorted(_loc_slugs):
        filt_of["loc:" + s] = {"locations": s}
        queued.append("loc:" + s)
    n_run = 0
    while queued and n_run < cap and budget_left():
        batch = [q for q in queued[:WORKERS * 2] if q not in done_lab]
        queued = queued[len(queued[:WORKERS * 2]):]
        if not batch:
            batch = []

        def one(lab):
            if not budget_left():
                return lab, None
            try:
                return lab, walk(out, lab, **filt_of[lab])
            except Transient as e:
                print("  D %s -> %s" % (lab, e), flush=True)
                return lab, None

        for lab, res in pool.map(one, batch):
            if res is not None:
                _state["sweeps"][lab] = "done"
                done_lab.add(lab)
                n_run += 1
        with _lock:
            _state["loc_slugs"] = sorted(_loc_slugs)
        save_state()
        print("  D %d/%d sweeps | %d rows | %d companies known | %.2f rps"
              % (n_run, cap, _rows[0], len(_state["companies_by_id"]),
                 1.0 / _interval[0]), flush=True)
        with _lock:
            fresh = sorted(_loc_slugs)
        for s in fresh:
            lab = "loc:" + s
            if lab not in done_lab and lab not in filt_of:
                filt_of[lab] = {"locations": s}
                queued.append(lab)
    _state["loc_slugs"] = sorted(_loc_slugs)
    save_state()
    print("phase D: %d sweeps run, %d location slugs known, %d companies now "
          "known" % (n_run, len(_loc_slugs), len(_state["companies_by_id"])),
          flush=True)


def main():
    global _state
    if MAX_MIN:
        _deadline[0] = time.time() + MAX_MIN * 60
    _state = load_state()
    load_seen()
    print("jobgether_fetch: %.1f rps x %d workers, budget %s min, "
          "%d offer ids already on disk"
          % (RPS, WORKERS, MAX_MIN or "inf", len(_seen)), flush=True)

    out = cc.Writer(OUT)
    t0 = time.time()
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            # D before B, A last, deliberately.  D and B are pure API — 50
            # offers per request, ~330 bytes an offer — so they turn the
            # budget straight into rows; A is 2.7k 60KB HTML fetches that
            # produce no rows at all, only company ids, and at the rate this
            # host allows that is half an hour before the first row lands.
            # A is worth running once the API phases have drained, because it
            # is the only way to reach a company that never appears on a sweep.
            if "d" in PHASES:
                phase_d(pool, out)
            if "b" in PHASES:
                phase_b(pool, out)
            if "a" in PHASES:
                phase_a(pool)
            if "b" in PHASES:
                phase_b(pool, out)
    finally:
        out.close()
        save_state()

    dt = time.time() - t0
    ndone = sum(1 for v in _state["companies_by_id"].values()
                if v.get("status") == "done")
    npend = len(_state["companies_by_id"]) - ndone
    print("\nDONE jobgether_fetch: %d rows written this run in %.0fs "
          "(%d ids total on disk)" % (_rows[0], dt, len(_seen)), flush=True)
    print("  companies: %d done, %d pending/failed; %d 429s, %d transient "
          "failures" % (ndone, npend, _n429[0], _nfail[0]), flush=True)
    if npend:
        print("  re-run to finish — state in %s" % STATE, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
