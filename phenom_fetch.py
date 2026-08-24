#!/usr/bin/env python3
"""
phenom_fetch.py — Phenom People career-site fetcher.

ROUTE
-----------------------------------------------------------------------------
Every Phenom-hosted career site exposes the same unauthenticated search widget
its own SPA calls:

  POST https://{host}/widgets
  {"ddoKey":"refineSearch","from":N,"size":100,"jobs":true,
   "country":"us","lang":"en_us","pageName":"search-results",
   "siteType":"external","keywords":"","selected_fields":{}}
  -> {"refineSearch":{"status":200,"hits":n,"totalHits":T,
       "data":{"jobs":[{title,type,city,state,country,postedDate,jobId,
                        jobSeqNo,applyUrl,locale,...}]}}}

`country` / `lang` are IGNORED by the endpoint — measured 2026-08-20 on three
boards, "us" / "all" / absent / "en_global" all return the identical
totalHits.  So there is no country partition to sweep: one paged walk is the
whole board.

The board list is tenants/phenom.csv, built and PROVED by
phenom_discover_boards.py (a host earns its row by answering this exact call
with this exact payload shape, never by looking like a Phenom URL).  BUILTIN
below is the fallback when that file is absent.

WHAT WENT WRONG BEFORE (fixed here)
-----------------------------------------------------------------------------
1. THREE OF SIX BOARDS SILENTLY RETURNED ZERO.  The 2026-08 run logged
     aerotek: 0 contract of 0 seen (total None)
     allegis: 0 ...        mlaresources: 0 ...
   while teksystems and actalent worked.  It was NOT a schema change.  All
   three hostnames had stopped resolving:

     careers.aerotek.com       NXDOMAIN -> moved to jobs.aerotek.com   (7,216)
     careers.mlaresources.com  NXDOMAIN -> rebranded careers.mlaglobal.com (54)
     careers.allegisgroup.com  NXDOMAIN -> Allegis Group's own board left
                                           Phenom for careers-allegisgroup.
                                           icims.com; there is no Phenom board
                                           to fetch, so it is recorded
                                           dead:migrated-off-phenom, not
                                           "empty".

   The reason the failure was INVISIBLE is the real defect, and it is rule 3:
   cc.post_json swallows a DNS failure into {"__error__": ...}, the old loop
   read `.get("refineSearch")` off that as {}, found no jobs and `break`ed —
   so an unreachable board and a board with no postings produced byte-identical
   output.  `total = rs.get("totalHits") or 0` then printed "total None" for
   both.  This file raises Dead/Transient instead: a board that errored stays
   PENDING in phenom_fetch_state.json and is retried, a board that is provably
   gone is recorded with the reason that proved it, and neither is ever
   recorded as a completed empty board.

2. THE 8,000-ROW WINDOW.  `if frm >= 8000: break` truncated any large board
   with no warning, and boards ARE over it: careers.oreillyauto.com carries
   18,011 postings and jobs.aerotek.com 7,216 (measured 2026-08-20).  Worse,
   8,000 was not even the real wall — the endpoint's own Elasticsearch window
   is `from + size < 10000`, past which it answers HTTP 200, an unchanged
   totalHits and ZERO jobs.  So the walk now runs to totalHits, DETECTS the
   window, and gets past it by re-querying under `selected_fields` facet
   filters (state, then type, then city), deduping on jobSeqNo.  Whatever a
   board still cannot deliver is printed as a WINDOW line rather than silently
   becoming that board's row count.

3. apply_url WAS A TRACKING REDIRECT.  The payload's own `applyUrl` is, on the
   Allegis brands,
     https://apply.aerotek.com/v1/s/?opco=AERO&params=<opaque encrypted blob>
   — an opaque per-request parameter blob, i.e. exactly the redirect/tracking
   URL rule 4 forbids: all_jobs_build dedupes on apply_url, so a blob that
   re-encodes between runs makes every row a permanent false-unique.  This
   file emits the employer's own canonical Phenom posting URL,

     https://{host}/{site_path}/job/{jobSeqNo}/{title-slug}

   built from stable fields (jobSeqNo is derived from the req id).  Verified
   2026-08-20 by fetching one random posting per board across 25 boards: 23
   rendered the posting, 1 was a 410 for a req pulled between fetch and check,
   and 1 was a wrong site_path — which is the whole reason site_path is
   PROVED per board rather than guessed.  All three cheap heuristics for it are
   wrong on some board (PNC's jobs say locale en_US but only /global/en
   renders; DHL's home page 303s to /amer/en but only /global/en renders;
   allianz's home page says /global/en and that prefix answers 410), so
   phenom_discover_boards.py decides it by fetching a real posting under each
   candidate prefix and keeping the one that renders.

4. NOTHING THIS PIPE EVER WROTE HAD A DATE.  See the note on ymd() below —
   9,535 rows, 0 dated, and all_jobs_build keeps undated rows unconditionally
   past its posted cutoff, so they could never age out.

Env knobs
  PHENOM_KEEP_ALL=1     keep every employment type -> contract_norm/phenom_all.jsonl
  PHENOM_BUDGET_MIN=N   wall-clock budget; unfinished boards stay PENDING
  PHENOM_RPS=f          request rate (default 5)
  PHENOM_WORKERS=n      concurrent boards (default 8)
  PHENOM_MAX_ROWS=n     per-board safety cap (default 50000)
  PHENOM_BOARDS=a,b     restrict to these board ids (smoke tests)
  PHENOM_LIMIT=n        only the n biggest pending boards (smoke tests)

Output: contract_norm/phenom.jsonl (or phenom_all.jsonl with KEEP_ALL).
"""
import collections
import csv
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANT_CSV = os.path.join(BASE, "tenants", "phenom.csv")
STATE_PATH = os.path.join(BASE, "phenom_fetch_state.json")

KEEP_ALL = os.environ.get("PHENOM_KEEP_ALL") == "1"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
OUT_NAME = "phenom_all.jsonl" if KEEP_ALL else "phenom.jsonl"
OUT_PATH = os.path.join(cc.NORM_DIR, OUT_NAME)

RPS = float(os.environ.get("PHENOM_RPS", "5"))
WORKERS = int(os.environ.get("PHENOM_WORKERS", "8"))
BUDGET_MIN = float(os.environ.get("PHENOM_BUDGET_MIN", "0"))
MAX_ROWS = int(os.environ.get("PHENOM_MAX_ROWS", "50000"))
ONLY = [b.strip() for b in os.environ.get("PHENOM_BOARDS", "").split(",")
        if b.strip()]
LIMIT = int(os.environ.get("PHENOM_LIMIT", "0"))
PAGE = int(os.environ.get("PHENOM_PAGE", "100"))
TIMEOUT = int(os.environ.get("PHENOM_TIMEOUT", "30"))
TRIES = 4

# Fallback board list when tenants/phenom.csv is missing.  Hosts as of
# 2026-08-20; careers.aerotek.com / careers.mlaresources.com no longer resolve
# and careers.allegisgroup.com is gone from Phenom entirely (see the header).
BUILTIN = [
    # board, company, host, site_path, source_type
    ("teksystems", "TEKsystems", "careers.teksystems.com", "us/en", "agency"),
    ("actalent", "Actalent", "careers.actalentservices.com", "us/en",
     "agency"),
    ("astoncarter", "Aston Carter", "careers.astoncarter.com", "us/en",
     "agency"),
    ("aerotek", "Aerotek", "jobs.aerotek.com", "us/en", "agency"),
    ("mlaglobal", "MLA Global", "careers.mlaglobal.com", "us/en", "agency"),
]
# Boards that left the platform.  Seeded into state so a run never re-probes
# them, and so the state file says WHY rather than leaving a silent hole where
# a board used to be.
RETIRED = {
    "allegis": "dead:migrated-off-phenom-to-icims",
    "mlaresources": "dead:renamed-to-mlaglobal",
}


# ---- throttle: the ats_fetch.py speed_up()/slow_down() pair ----------------
# One shared bucket.  Boards are distinct hosts but they share our egress IP,
# and the 429s that matter come from the shared Phenom edge, not the tenant.
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_ok_streak = [0]


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
        _pause_until[0] = time.time() + 15
        _interval[0] = min(_interval[0] * 1.5, 0.5)


def speed_up():
    """Walk the interval back toward the configured rate after a clean run.

    Without this, slow_down() is a one-way ratchet: a handful of 429s in the
    first minutes pins the whole run at 2 rps for its remaining hours.  That
    exact bug was measured in this repo taking throughput from 39 to 21
    boards/min — RAISING the configured rate made it worse, because more 429s
    meant more permanent penalty.  Recovery is deliberately slower than the
    backoff (0.9x per 40 clean responses vs 1.5x per 429) so a host that is
    genuinely refusing still wins the argument.
    """
    with _rate_lock:
        _ok_streak[0] += 1
        if _ok_streak[0] < 40:
            return
        _ok_streak[0] = 0
        floor = 1.0 / RPS
        if _interval[0] > floor:
            _interval[0] = max(_interval[0] * 0.9, floor)


class Transient(Exception):
    """Unreachable / rate-limited.  Raised so the board is left PENDING rather
    than recorded as a completed empty board — the rule-3 distinction."""


class Dead(Exception):
    """The board is provably not there.  Carries the discriminator that proved
    it, so state says WHY and a later reader can tell a real retirement from a
    misclassification."""
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def body(frm, size=None, sf=None):
    return {"ddoKey": "refineSearch", "from": frm, "size": size or PAGE,
            "jobs": True, "country": "us", "lang": "en_us",
            "pageName": "search-results", "siteType": "external",
            "keywords": "", "selected_fields": sf or {}}


def post_page(s, host, frm, sf=None):
    """Throttled POST -> the refineSearch envelope.

    Never returns an empty dict for a failure.  "Empty board" and "could not
    reach the board" must not be the same value: the first is recorded done,
    the second has to stay pending.
    """
    url = "https://%s/widgets" % host
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.post(url, json=body(frm, sf=sf), timeout=TIMEOUT)
        except requests.exceptions.SSLError as e:
            raise Dead("tls-%s" % type(e).__name__)
        except requests.exceptions.ConnectionError as e:
            # NXDOMAIN is the DNS asserting the host is gone — that is what
            # careers.aerotek.com and careers.mlaresources.com now answer, and
            # believing "0 jobs" instead is what hid the outage for weeks.
            # Every other connection error is a blip and stays transient.
            if "NameResolutionError" in str(e) or "nodename" in str(e):
                raise Dead("dns-nxdomain")
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue

        code = r.status_code
        ct = (r.headers.get("content-type") or "").lower()
        if code == 200:
            if "json" not in ct:
                # A Phenom career site is an SPA behind wildcard routing: it
                # answers 200 with an HTML shell for ANY path, including
                # /widgets on hosts that are not Phenom at all.  Status alone
                # would read this as a live, empty board.
                raise Dead("200-html-shell")
            try:
                d = r.json()
            except ValueError:
                raise Dead("200-unparseable")
            rs = d.get("refineSearch") if isinstance(d, dict) else None
            if not isinstance(rs, dict) or not (
                    "totalHits" in rs or "hits" in rs or "data" in rs):
                raise Dead("200-not-refinesearch")
            speed_up()
            return rs
        if code in (404, 410, 405, 501):
            raise Dead("http-%d-no-widgets" % code)
        if code in (400, 401):
            raise Dead("http-%d-rejected" % code)
        if code in (429, 403, 503, 502, 504, 408, 500):
            # 403 stays TRANSIENT.  On these hosts it is a Cloudflare/edge
            # penalty against our IP, not the tenant asserting anything; the
            # cost of guessing wrong is deleting a live board's rows.
            slow_down()
            last = "HTTP %d" % code
        else:
            last = "HTTP %d" % code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s@%s: %s" % (host, frm, last))


# ---- the 10,000-row deep-paging window -------------------------------------
# The widgets search is Elasticsearch underneath and enforces the default
# index.max_result_window: `from + size` must be strictly under 10,000.
# Measured 2026-08-20 on careers.oreillyauto.com (18,011 postings):
#   from=9800 size=100  -> 100 jobs        from=9900 size=100 -> 0 jobs
#   from=9000 size=1000 -> 0 jobs          totalHits still 18,011, status 200
# i.e. the endpoint reports success and an unchanged total while serving
# nothing.  The old `if frm >= 8000: break` guard turned that into a silent
# truncation at an even lower number; here the wall is DETECTED and worked
# around by filtering, and whatever is still missing is REPORTED.
WINDOW = int(os.environ.get("PHENOM_WINDOW", "10000"))
# Partition axes, tried in order.  `selected_fields` takes {field: [value]};
# verified against oreillyauto — state="Texas" -> 2,161 of 18,011,
# type="Full time" -> 14,452.  ("category" and "country" returned 0 there, so
# they sit last and an empty partition simply contributes nothing.)
AXES = ("state", "type", "city", "category")
MAX_DEPTH = int(os.environ.get("PHENOM_MAX_DEPTH", "3"))


def _values(jobs, axis):
    """Distinct values of a partition axis among the rows we DID get back."""
    out, seen = [], set()
    for j in jobs:
        v = j.get(axis)
        if isinstance(v, list):
            v = v[0] if v else None
        v = str(v).strip() if v is not None else ""
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def harvest(s, host, sf, got, notes, depth=0):
    """Page one (possibly filtered) slice of a board into `got`, splitting on a
    facet when the deep-paging window closes before the total is reached.

    `got` is keyed by jobSeqNo, so a posting matching two partitions (a
    multi-location req) is stored once.
    """
    frm, total = 0, None
    mine = []
    while True:
        if _deadline[0] and time.time() > _deadline[0]:
            raise Transient("wall-clock budget exhausted mid-board")
        rs = post_page(s, host, frm, sf)
        if total is None:
            # totalHits may legitimately be 0 (an employer with no open reqs).
            # It is only meaningless when the ENVELOPE is missing, and
            # post_page has already raised Dead in that case — which is exactly
            # the distinction `rs.get("totalHits") or 0` erased.
            total = rs.get("totalHits")
            if total is None:
                raise Dead("refinesearch-without-totalhits")
        jobs = ((rs.get("data") or {}).get("jobs")) or []
        if not jobs:
            break
        for j in jobs:
            key = (j.get("jobSeqNo") or j.get("jobId") or j.get("reqId")
                   or "%s|%s" % (j.get("title"), j.get("location")))
            got.setdefault(key, j)
            mine.append(j)
        frm += len(jobs)
        if frm >= total or len(got) >= MAX_ROWS:
            return
        if frm + PAGE >= WINDOW:
            break                        # window closed — partition below
    if total is None or frm >= total or len(got) >= MAX_ROWS:
        return
    if depth >= MAX_DEPTH:
        notes.append("depth %d gave up with %d of %d under %s"
                     % (depth, frm, total, sf))
        return
    axis = next((a for a in AXES if a not in (sf or {})), None)
    if not axis:
        notes.append("no axis left with %d of %d under %s" % (frm, total, sf))
        return
    vals = _values(mine, axis)
    if not vals:
        notes.append("axis %s had no values with %d of %d" % (axis, frm, total))
        return
    for v in vals:
        sub = dict(sf or {})
        sub[axis] = [v]
        harvest(s, host, sub, got, notes, depth + 1)


# ---- normalisation ---------------------------------------------------------
_SLUG = re.compile(r"[^a-z0-9]+")
_TRACKING = re.compile(
    r"(?:^|//)(?:apply|track|click|go|redirect)\.|[?&](?:params|utm_|s_id|"
    r"src|source|ref)=", re.I)


def slug(title):
    return _SLUG.sub("-", (title or "").lower()).strip("-")[:120]


# Phenom stamps a COLON-LESS utc offset: "2026-08-13T14:09:14.603+0000".
# Python 3.9's datetime.fromisoformat is strict and refuses it, cc.ymd catches
# the ValueError and returns "" — so every Phenom row this pipe has ever
# written was undated.  Measured 2026-08-20 on the shipped output:
# contract_norm/phenom_all.jsonl, 9,535 rows, 0 with a posted date.  That is
# the Recruitee " UTC" failure documented in cc.ymd's own docstring, one
# format over, and it is worse than cosmetic: all_jobs_build keeps undated
# rows UNCONDITIONALLY past its posted cutoff, so 50k undated Phenom rows
# could never age out of the master.
# cc.ymd is shared and out of scope for this task, so the offset is repaired
# here, at the one call site that sees it.
_OFFSET = re.compile(r"([+-]\d{2})(\d{2})$")


def ymd(iso):
    s = (iso or "").strip()
    if not s:
        return ""
    return cc.ymd(iso=_OFFSET.sub(r"\1:\2", s))


def posting_url(host, site_path, j):
    """The employer's own canonical posting URL — stable across runs.

    Built from jobSeqNo (the platform's own deterministic id, e.g.
    AEROUSJP006218426ENUS for req JP-006218426) rather than from the payload's
    applyUrl, which on the Allegis brands is an opaque encrypted redirect.
    Verified 2026-08-20 to render the posting on aerotek/teksystems/lowes/
    summitllc (us/en) and pnc (global/en).
    """
    jid = (j.get("jobSeqNo") or j.get("jobId") or j.get("reqId") or "").strip()
    if not jid:
        return ""
    sp = (site_path or "us/en").strip("/")
    t = slug(j.get("title"))
    return "https://%s/%s/job/%s%s" % (host, sp, jid, "/" + t if t else "")


def ats_url(j):
    """The payload's applyUrl, kept only when it is a real ATS deep link.

    careers.pnc.com hands back pnc.wd5.myworkdayjobs.com/... — genuinely
    useful, it collapses with the workday pipe in all_jobs_build.  Aerotek
    hands back apply.aerotek.com/v1/s/?params=<blob> — a tracking redirect,
    dropped.
    """
    u = (j.get("applyUrl") or "").strip()
    if not u or not u.startswith("http"):
        return ""
    return "" if _TRACKING.search(u) else u


_CANON = [None]      # lazily-bound all_jobs_build.canon_key


def collapses(u):
    """True when all_jobs_build can give this URL a tier-1 canonical ATS key.

    Borrowed from the builder rather than reimplemented, so the two can never
    disagree about what counts as a known ATS.  Imported lazily because
    all_jobs_build pulls in openpyxl and this is the only thing needed from it;
    if the import fails for any reason the answer is False, which just restores
    the previous preference rather than breaking the fetch.
    """
    if not u:
        return False
    if _CANON[0] is None:
        try:
            import all_jobs_build as _ajb
            _CANON[0] = _ajb.canon_key
        except Exception:                    # noqa: BLE001
            _CANON[0] = False
    if not _CANON[0]:
        return False
    try:
        return bool(_CANON[0](u))
    except Exception:                        # noqa: BLE001
        return False


def norm_row(j, board, company, host, site_path, source_type,
             url_mode="phenom"):
    et = CLASSIFY(j.get("type"), j.get("title"))
    if not et:
        return None
    loc = j.get("location") or ", ".join(
        x for x in (j.get("city"), j.get("state"), j.get("country")) if x)
    # url_mode is decided per board by the discovery pass, by fetching a real
    # posting and seeing whether it renders.  "ats" means this board's own
    # /{site_path}/job/... 404s (jobs.trinity-health.org does exactly that
    # while its widgets API works fine), so its underlying ATS deep link is
    # the employer's real posting URL and the constructed one would be a
    # 404 stamped on every row.
    mine_first = posting_url(host, site_path, j)
    theirs = ats_url(j)
    # PREFER THE EMPLOYER'S REAL ATS LINK WHENEVER IT COLLAPSES.  Measured
    # 2026-08-20 over the eight largest phenom boards: 499 of 800 sampled
    # postings (62%) hand back a Workday/iCIMS/Ashby deep link that
    # all_jobs_build.canon_key already keys at tier 1 -- oreillyauto 99%,
    # tjx/rtx/lowes/ascension 100%.  Keeping the constructed phenom URL
    # instead threw that away and landed each of those rows a SECOND time
    # beside the copy the workday/icims pipes already hold.  Boards whose
    # applyUrl is a tracking blob (aerotek, actalent, dhl: 0%) are unaffected
    # -- ats_url() already returns "" for those and the constructed URL wins.
    if collapses(theirs):
        apply_url = theirs
    else:
        apply_url = (theirs or mine_first) if url_mode == "ats" else \
            (mine_first or theirs)
    if not apply_url:
        return None
    r = cc.row(
        source="phenom", source_type=source_type or "ats", company=company,
        title=j.get("title"), employment_type=et,
        work_mode=cc.work_mode(text=" ".join(
            str(j.get(k) or "") for k in ("city", "location", "type"))),
        location=loc, city=j.get("city") or "", state=j.get("state") or "",
        country=j.get("country") or "",
        posted=ymd(j.get("postedDate")) or ymd(j.get("dateCreated")),
        apply_url=apply_url,
        job_id=j.get("jobId") or j.get("reqId") or j.get("jobSeqNo"),
        board=board, board_url="https://%s" % host)
    return r


# ---- boards ----------------------------------------------------------------
def load_boards():
    if os.path.exists(TENANT_CSV):
        out = []
        with open(TENANT_CSV, newline="", encoding="utf-8") as f:
            for d in csv.DictReader(f):
                host = (d.get("host") or "").strip()
                if not host:
                    continue
                board = (d.get("board") or host.split(".")[0]).strip()
                out.append((board, (d.get("company") or board).strip(), host,
                            (d.get("site_path") or "us/en").strip(),
                            (d.get("source_type") or "ats").strip(),
                            int(d.get("total") or 0),
                            (d.get("url_mode") or "phenom").strip()))
        if out:
            return out
    print("  (no %s — falling back to %d built-in boards)"
          % (TENANT_CSV, len(BUILTIN)))
    return [(b, c, h, sp, st, 0, "phenom") for b, c, h, sp, st in BUILTIN]


# ---- run -------------------------------------------------------------------
_lock = threading.Lock()
_counts = collections.Counter()
_dead_reasons = collections.Counter()
_deadline = [0.0]
_capped = []
_window_closed = []


def fetch_board(rec, out):
    board, company, host, site_path, source_type, _hint, url_mode = rec
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                      "Content-Type": "application/json"})
    try:
        got, notes = {}, []
        harvest(s, host, {}, got, notes)
        rows = []
        for j in got.values():
            r = norm_row(j, board, company, host, site_path, source_type,
                         url_mode)
            if r:
                rows.append(r)
        if len(got) >= MAX_ROWS:
            with _lock:
                _capped.append((board, MAX_ROWS))
        if notes:
            with _lock:
                _window_closed.append((board, notes))
        # Write only after the WHOLE board came back clean, so a mid-board
        # failure never leaves a half-board recorded as done.
        with _lock:
            for r in rows:
                out.write(r)
        return len(rows), len(got)
    finally:
        s.close()


def work(rec, out, state):
    board = rec[0]
    st = state.get(board)
    if isinstance(st, str) and (st == "done" or st.startswith("dead:")):
        return
    if _deadline[0] and time.time() > _deadline[0]:
        return                      # budget gone: leave it PENDING
    try:
        kept, seen = fetch_board(rec, out)
    except Dead as e:
        with _lock:
            state[board] = "dead:%s" % e.reason
            _counts["dead"] += 1
            _dead_reasons[e.reason] += 1
            print("  phenom %-22s DEAD %s" % (board, e.reason), flush=True)
        return
    except Transient as e:
        with _lock:
            # Neither done nor dead: the next run retries it.
            state[board] = "err:%s" % str(e)[:80]
            _counts["err"] += 1
            print("  phenom %-22s ERR (still pending) %s"
                  % (board, str(e)[:60]), flush=True)
        return
    except Exception as e:                                   # noqa: BLE001
        with _lock:
            state[board] = "err:%s" % type(e).__name__
            _counts["err"] += 1
            print("  phenom %-22s ERR (still pending) %s"
                  % (board, type(e).__name__), flush=True)
        return
    with _lock:
        state[board] = "done"
        _counts["done"] += 1
        _counts["rows"] += kept
        _counts["seen"] += seen
        print("  phenom %-22s %6d kept of %6d unique postings"
              % (board, kept, seen), flush=True)


def main():
    boards = load_boards()
    if ONLY:
        boards = [b for b in boards if b[0] in ONLY]

    state = cc.load_state(STATE_PATH)
    for b, why in RETIRED.items():
        state.setdefault(b, why)

    # run_all.py truncates the output before each refresh (see its `pre=`), so
    # an empty output file means a NEW run and every "done" from the previous
    # one is stale.  dead: entries are kept — a retired board does not come
    # back, and re-probing it every run is the cost this state file exists to
    # avoid.  err: entries are pending by construction.
    fresh = not os.path.exists(OUT_PATH) or os.path.getsize(OUT_PATH) == 0
    if fresh:
        state = {k: v for k, v in state.items()
                 if isinstance(v, str) and v.startswith("dead:")}

    pending = [b for b in boards
               if not (isinstance(state.get(b[0]), str)
                       and (state[b[0]] == "done"
                            or state[b[0]].startswith("dead:")))]
    # Biggest boards first: with a wall-clock budget the expensive boards must
    # start early or they are the ones that never finish.
    pending.sort(key=lambda b: -b[5])
    if LIMIT:
        pending = pending[:LIMIT]

    if BUDGET_MIN:
        _deadline[0] = time.time() + BUDGET_MIN * 60
    print("phenom: %d boards, %d pending%s%s"
          % (len(boards), len(pending),
             " (fresh run)" if fresh else " (resuming)",
             ", budget %.0fmin" % BUDGET_MIN if BUDGET_MIN else ""),
          flush=True)

    out = cc.Writer(OUT_PATH)
    t0 = time.time()
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            list(ex.map(lambda r: work(r, out, state), pending))
    finally:
        out.close()
        cc.save_state(STATE_PATH, state)
        left = [b[0] for b in boards
                if not (isinstance(state.get(b[0]), str)
                        and (state[b[0]] == "done"
                             or state[b[0]].startswith("dead:")))]
        for board, cap in _capped:
            print("  CAP %s stopped at %d rows (raise PHENOM_MAX_ROWS)"
                  % (board, cap))
        for board, notes in _window_closed:
            for n in notes:
                print("  WINDOW %s: %s" % (board, n))
        print("DONE phenom: %d rows from %d boards in %.1fs "
              "(%d dead, %d errored, %d still pending)"
              % (_counts["rows"], _counts["done"], time.time() - t0,
                 _counts["dead"], _counts["err"], len(left)))
        for r, c in _dead_reasons.most_common(10):
            print("    dead: %-32s %d" % (r, c))
    return 0


if __name__ == "__main__":
    sys.exit(main())
