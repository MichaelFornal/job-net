#!/usr/bin/env python3
"""
eightfold_fetch.py — Eightfold AI Talent Intelligence career sites.

Tenant list: tenants/eightfold.csv (tenant, n_rows, example_url).  Only 10
tenants, but they are enterprises (Starbucks, Micron, Eaton, Microsoft, Boston
Scientific, PayPal, CoStar, Fortive, LG CNS, PTC).  Nine of the ten are live
and a full sweep on 2026-07-29 pulled **30,007 postings** off them — Starbucks
alone is 21,735.  The volume is entirely per-tenant, so the length of the queue
tells you nothing about the length of the run.

THERE ARE TWO API GENERATIONS AND THE OLD ONE ANSWERS 403 ON THE NEW ONE.
This is the single dangerous thing about this platform:

    GET {t}.eightfold.ai/api/apply/v2/jobs?domain={d}&start=0&num=10   (legacy)
        -> {"count": N, "positions": [...]}
    GET {t}.eightfold.ai/api/pcsx/search?domain={d}&start=0&num=10     (PCSX)
        -> {"status":200,"error":{...},"data":{"count":N,"positions":[...]}}

A PCSX tenant answers the LEGACY path with **HTTP 403** and the body
`{"message": "Not authorized for PCSX"}`; a legacy tenant answers the PCSX path
with **HTTP 403** and `{"message": "PCSX is not enabled for this user."}`.
403 is the canonical "you are being rate limited / blocked" signal, and this
repo has already paid for reading a 403 as throttling once (ats_fetch.gj, the
breezy note).  Reading it that way here would trip a global backoff and then
mark 8 of 10 LIVE tenants as dead: the measured split on 2026-07-29 was 8
tenants PCSX-only, 2 (costar, lgcns) legacy-only, and the 403 is 100%
correlated with "wrong generation", never with request rate — it was reproduced
at one request per tenant, cold.  So a 403 whose body mentions PCSX is ROUTING
information: try the other generation before concluding anything.  A 403 with
any other body is treated as a real block and retried.

DEAD TENANT: **HTTP 404 on `https://{t}.eightfold.ai/careers`.**
  * eightfold.ai has NO wildcard DNS — an invented subdomain fails to resolve
    (NXDOMAIN), which is the cleanest possible dead signal but only catches
    hosts that were never provisioned.
  * A tenant that WAS provisioned and has since been retired keeps its DNS and
    its TLS, and the app answers 404 with a small branded error page on
    /careers and on both API paths.  ptc.eightfold.ai is exactly this: 2 rows
    in the corpus, resolves fine, 404 everywhere today.
  Both are recorded dead.  Anything else (timeout, connection reset, 5xx, 429)
  leaves the tenant PENDING so the next run retries it.

NEITHER GENERATION PAGES ATOMICALLY.  Postings shift rank between requests, so
consecutive pages overlap and a small tail is missed: microsoft reports 1,730
and yields 1,685 distinct postings, bostonscientific 592 -> 590.  Per-board
dedupe on apply_url keeps the file's rows==distinct-apply_url invariant; the
~2% shortfall on the churny boards is the platform's, not this script's, and
successive runs pick up different slices of it.

PAGING: `num` is hard-capped at 10 on BOTH generations — num=25/50/100, and the
aliases size/limit/pageSize/numResults, all return exactly 10 positions.  So a
board costs ceil(count/10) requests: Starbucks alone is ~2,190.  That is why
this script paces per host and runs the tenants concurrently (each tenant is
its own hostname); deep offsets are fine, start=21830 of 21834 returns the last
4 rows.

POSTED DATES: PCSX gives postedTs AND creationTs, and postedTs drifts — every
Starbucks posting sampled reported postedTs == today while creationTs was days
older, i.e. postedTs is a refresh stamp, not a publish date.  Rows therefore
take min(creationTs, postedTs), and a first-seen ledger
(eightfold_posted_ledger.json) pins the earliest date ever observed for a job
id so a refreshed posting can never walk its date forward into a "new this
week" cut.  THE LEDGER IS APPEND-ONLY AND MUST NOT BE ROTATED OR DELETED — it
is the only memory of when these postings were first seen.

RATE LIMITING arrives as HTTP 405, not 429 — see the _WAF note below, and the
long comment on RPS.  It is the second 200-adjacent lie on this platform and
the more expensive one.

OUTPUT  contract_norm/eightfold.jsonl
STATE   eightfold_fetch_state.json.  Per tenant either the string "done", the
        string "dead:<why>", or a DICT — a dict is a partially fetched board
        carrying {gen, domain, company, start, count, written}, and the next
        run resumes at `start` instead of re-paging from zero.  A tenant that
        is not reached at all never appears, so it is retried.  Nothing but a
        clean finish or a confirmed death stops the retry.
Env: EF_WORKERS, EF_RPS (per host), EF_WAF_PAUSE, EF_BUDGET_MIN,
     EF_ONLY=tenant1,tenant2
"""
import concurrent.futures as cf
import csv
import html as _html
import os
import re
import socket
import sys
import threading
import time
from datetime import datetime, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "eightfold.csv")
OUT = os.path.join(cc.NORM_DIR, "eightfold.jsonl")
STATE = os.path.join(BASE, "eightfold_fetch_state.json")
LEDGER = os.path.join(BASE, "eightfold_posted_ledger.json")

WORKERS = int(os.environ.get("EF_WORKERS") or 10)
# Per-host, and deliberately low.  The big tenants sit behind CloudFront with
# an AWS WAF rate rule: an 8 rps sweep of Starbucks got 2,380 pages in and then
# every request — including start=0 — answered HTTP 405 for the next several
# minutes (see _WAF below).  AWS rate-based rules count over a 5-minute window,
# so staying near 2.5 rps keeps a single host under ~750 requests per window.
# Starbucks (21.8k postings / 10 per page = ~2,190 requests) is ~15 min at this
# rate; every other tenant finishes in under two.
RPS = float(os.environ.get("EF_RPS") or 2.5)
PAGE = 10               # server-side hard cap, see PAGING above
MAX_PAGES = 4000        # 40k postings/tenant, ~2x the largest board observed
CKPT = 25               # save the resume cursor every N pages (250 postings)
TIMEOUT = 45
TRIES = 5
BUDGET_MIN = int(os.environ.get("EF_BUDGET_MIN") or 0)
ONLY = [x for x in (os.environ.get("EF_ONLY") or "").split(",") if x]

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

LEGACY = "/api/apply/v2/jobs?domain={d}&start={s}&num={n}"
PCSX = "/api/pcsx/search?domain={d}&start={s}&num={n}"
# The 403 that means "wrong API generation", NOT "slow down".  BOTH directions
# of the mismatch answer 403, with two different one-line JSON bodies, and both
# were observed on 2026-07-29:
#   PCSX tenant asked on the LEGACY path  -> {"message":"Not authorized for
#                                             PCSX"}                 (eaton)
#   legacy tenant asked on the PCSX path  -> {"message":"PCSX is not enabled
#                                             for this user."}       (costar,
#                                                                     lgcns)
# Matching only the first of those cost a full pass in testing: the two legacy
# tenants tripped slow_down(), sat out a 20s platform pause five times each and
# still ended as err: rather than being routed to the API they do serve.  The
# discriminator is a 403 whose body mentions PCSX at all; a 403 with any other
# body is a real block and is retried.
_WRONG_GEN = re.compile(rb"PCSX", re.I)

# THE RATE LIMIT DOES NOT ARRIVE AS 429.  The larger tenants are fronted by
# CloudFront + AWS WAF, and the WAF challenge is served as **HTTP 405** with an
# HTML "Human Verification" page and the header `x-amzn-waf-action: captcha`.
# 405 is Method Not Allowed — nothing in this repo's retry vocabulary reads it
# as throttling, and a fetcher that classified it structurally would either
# call the tenant dead or burn its retries in 20 seconds and give up on a
# 21,800-posting board.  Measured 2026-07-29 on starbucks at 8 rps: clean 200s
# up to start=2380, then EVERY request 405 for minutes, start=0 included, then
# a clean 200 again with no intervention.  So it is a real, temporary, per-IP
# block: back off HARD (the pause below is minutes, not seconds), never dead,
# never abandoned.
_WAF = re.compile(rb"Human Verification|awswaf|challenge-container", re.I)
WAF_PAUSE = int(os.environ.get("EF_WAF_PAUSE") or 240)

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {}
_pause_until = {}
_interval = {}
_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_writer = None
_counts = {"rows": 0, "live": 0, "dead": 0, "empty": 0, "err": 0}
_n429 = 0
_failed_now = set()


class Transient(Exception):
    """Retryable — the tenant stays PENDING."""


class DeadTenant(Exception):
    """Confirmed retired or never provisioned."""


class WrongGeneration(Exception):
    """This path is not this tenant's API generation; try the other one."""


def throttle(host):
    while True:
        with _rate_lock:
            now = time.time()
            _interval.setdefault(host, 1.0 / RPS)
            _next_t.setdefault(host, 0.0)
            _pause_until.setdefault(host, 0.0)
            if now < _pause_until[host]:
                wait = _pause_until[host] - now
            else:
                wait = _next_t[host] - now
                if wait <= 0:
                    _next_t[host] = max(_next_t[host] + _interval[host], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(host, seconds=20):
    global _n429
    with _rate_lock:
        _pause_until[host] = max(_pause_until.get(host, 0.0),
                                 time.time() + seconds)
        _interval[host] = min(_interval.get(host, 1.0 / RPS) * 1.5, 2.0)
        _n429 += 1


def resolves(host):
    """False only on a definitive NXDOMAIN, confirmed twice.

    A single getaddrinfo failure can be the local resolver having a bad
    second; a tenant is only called dead on DNS when two attempts a second
    apart both say the name does not exist.
    """
    for i in range(2):
        try:
            socket.getaddrinfo(host, 443)
            return True
        except socket.gaierror:
            if i:
                return False
            time.sleep(1.0)
        except OSError:
            return True         # not a name problem — let HTTP decide
    return False


def get(s, host, path, want_json=True):
    url = f"https://{host}{path}"
    last = None
    attempt = 0
    # WAF waits get their OWN budget rather than consuming the ordinary retry
    # budget: five 2-second retries cannot outlast a block that lasts minutes,
    # and letting a captcha eat the retries would fail the page for a reason
    # that has nothing to do with the page.
    waf_left = 3
    while attempt < TRIES:
        throttle(host)
        try:
            r = s.get(url, timeout=TIMEOUT)
            code = r.status_code
            if code == 200:
                if not want_json:
                    return r.text or ""
                try:
                    return r.json()
                except ValueError:
                    # The careers SPA is served with HTTP 200 on some paths;
                    # an API path answering HTML means the route is not the one
                    # this tenant serves, not that we should retry it.
                    raise WrongGeneration("200 non-JSON")
            waf = ((r.headers.get("x-amzn-waf-action") or "").lower()
                   == "captcha") or (code == 405
                                     and _WAF.search(r.content or b""))
            if waf:
                last = f"HTTP {code} WAF-captcha"
                if waf_left <= 0:
                    break
                waf_left -= 1
                slow_down(host, WAF_PAUSE)
                # No extra sleep and no attempt++: throttle() at the top of the
                # next pass already blocks on the pause this just set.
                continue
            if code == 403 and _WRONG_GEN.search(r.content or b""):
                raise WrongGeneration("Not authorized for PCSX")
            if code == 404:
                raise DeadTenant(f"HTTP 404 {path.split('?')[0]}")
            if code in (429, 418):
                slow_down(host)
                last = f"HTTP {code}"
            elif code == 403:
                # 403 WITHOUT the wrong-generation body is a genuine block.
                slow_down(host)
                last = "HTTP 403 (blocked)"
            else:
                last = f"HTTP {code}"
        except requests.RequestException as e:
            last = type(e).__name__
        attempt += 1
        time.sleep(2 * attempt)
    raise Transient(f"{url}: {last}")


# ---- tenant discovery ------------------------------------------------------
_DOMAIN_RX = [re.compile(r'domain&\#34;:\s*&\#34;([^&<"]+)&\#34;'),
              re.compile(r'"domain"\s*:\s*"([^"]+)"'),
              re.compile(r'[?&]domain=([A-Za-z0-9.\-]+)')]
_COMPANY_RX = [re.compile(r'companyName&\#34;:\s*&\#34;([^&<"]+)'),
               re.compile(r'"companyName"\s*:\s*"([^"]+)"')]


def discover(s, host, tenant):
    """(domain, company) from the tenant's careers page.

    The `domain` query parameter is NOT optional — both APIs answer with an
    empty envelope (count null, no positions) when it is missing or wrong, with
    HTTP 200, which is precisely the "live board with no jobs" shape.  It is
    never guessed: it is read out of the SPA shell, which embeds it dozens of
    times.  companyName comes from the same page and is the real employer name
    ("Eaton Corporation", "CoStar Group"), which the tenant slug only
    approximates.
    """
    body = get(s, host, "/careers", want_json=False)
    dom = comp = ""
    for rx in _DOMAIN_RX:
        m = rx.search(body)
        if m:
            dom = _html.unescape(m.group(1)).strip()
            break
    for rx in _COMPANY_RX:
        m = rx.search(body)
        if m:
            comp = _html.unescape(m.group(1)).strip()
            break
    if not dom:
        raise DeadTenant("careers page carries no domain= config")
    return dom, comp


# ---- normalization ---------------------------------------------------------
_MODE = {"remote": "Remote", "fully_remote": "Remote", "hybrid": "Hybrid",
         "onsite": "On-site", "on-site": "On-site", "in_office": "On-site"}


def ledger_date(key, iso):
    """Earliest date ever observed for this posting.

    Only ever moves a date EARLIER.  postedTs is refreshed by the platform, so
    without this a posting that has been open for a year re-enters every
    "posted in the last N days" cut every time it is refreshed.
    """
    with _lock:
        prev = _ledger.get(key)
        if prev and (not iso or prev <= iso):
            return prev
        _ledger[key] = iso or TODAY
        return _ledger[key]


def to_row(tenant, host, company, p):
    jid = str(p.get("id") or "").strip()
    title = (p.get("name") or p.get("posting_name") or "").strip()
    if not jid or not title:
        return None
    locs = [x for x in (p.get("locations") or []) if x]
    if not locs:
        std = [x for x in (p.get("standardizedLocations") or []) if x]
        locs = std
    single = p.get("location")
    if not locs and single:
        locs = [single]
    locstr = "; ".join(dict.fromkeys(locs))
    std0 = ([x for x in (p.get("standardizedLocations") or []) if x] or [""])[0]
    city = state = country = ""
    if std0:
        # standardizedLocations are 'Seattle, WA, US' / 'Calama, Antofagasta,
        # CL' — city, region, ISO-2.  Only the 3-part form is parsed; anything
        # else is left blank rather than guessed.
        parts = [x.strip() for x in std0.split(",")]
        if len(parts) == 3 and re.fullmatch(r"[A-Z]{2}", parts[2]):
            city, state, country = parts
    opt = str(p.get("workLocationOption") or p.get("work_location_option")
              or "").strip().lower()
    flex = str(p.get("locationFlexibility") or p.get("location_flexibility")
               or "").strip().lower()
    mode = _MODE.get(opt) or _MODE.get(flex) or ""
    if not mode and re.search(r"\bremote\b", locstr, re.I):
        mode = "Remote"
    # posted: see POSTED DATES in the header — the EARLIER of the two stamps,
    # then pinned by the ledger.
    ts = [p.get("creationTs"), p.get("postedTs"),
          p.get("t_create"), p.get("t_update")]
    ts = [t for t in ts if isinstance(t, (int, float)) and t > 0]
    iso = cc.ymd(ts=min(ts)) if ts else ""
    posted = ledger_date(f"{tenant}:{jid}", iso)
    # `department` ("Engineering", "Retail Stores") is deliberately NOT fed to
    # classify_any: it is an org unit, and a company with a "Contracts" or
    # "Temporary Staffing" department would have every one of its permanent
    # roles relabelled contingent.
    return cc.row(
        source="eightfold", source_type="ats", company=company or tenant,
        title=title,
        # Neither generation exposes an employment-type field (legacy `type` is
        # the literal string "ATS", i.e. the record source).  classify_any is
        # mandatory: cc.classify returns None for full-time roles and would
        # drop ~30k of the ~30.2k rows this pipe exists to fetch.
        employment_type=cc.classify_any(None, title),
        work_mode=mode, location=locstr, city=city, state=state,
        country=country, remote=(mode == "Remote"),
        posted=posted,
        # The canonical per-posting URL on the tenant's own host — exactly the
        # form the corpus already holds, so the two copies collapse.
        apply_url=f"https://{host}/careers/job/{jid}",
        # The id that appears IN the apply URL, following the convention every
        # other fetcher in this repo uses, so job_id and the canonical ATS key
        # parsed from apply_url can never disagree.  Eightfold's other id
        # (displayJobId / atsJobId, e.g. "R39220") is the customer's own
        # requisition number and does not appear in any URL.
        job_id=jid,
        board=tenant, board_url=f"https://{host}/careers")


# ---- per-tenant work -------------------------------------------------------
def page_api(s, host, dom, gen, start):
    path = (PCSX if gen == "pcsx" else LEGACY).format(d=dom, s=start, n=PAGE)
    d = get(s, host, path)
    if gen == "pcsx":
        data = (d or {}).get("data") or {}
        return data.get("positions") or [], data.get("count")
    return (d or {}).get("positions") or [], (d or {}).get("count")


def pick_generation(s, host, dom):
    """Probe both generations once.  Returns (gen, positions, count).

    Order matters only for speed: PCSX is 8 of the 10 known tenants.  A
    WrongGeneration on one path is NOT an error — it is the answer to which
    path to use.  Only a tenant that rejects BOTH is dead.
    """
    errs = []
    for gen in ("pcsx", "legacy"):
        try:
            pos, cnt = page_api(s, host, dom, gen, 0)
            if cnt is None and not pos:
                # 200 with an empty envelope: the domain= did not match this
                # tenant.  Not a rate limit, not an empty board.
                errs.append(f"{gen}: empty envelope (bad domain=)")
                continue
            return gen, pos, cnt
        except WrongGeneration as e:
            errs.append(f"{gen}: {e}")
        except DeadTenant as e:
            errs.append(f"{gen}: {e}")
    raise DeadTenant("both API generations refused — " + "; ".join(errs))


def fetch_tenant(tenant):
    """Page one tenant, CHECKPOINTING as it goes.

    A 2,190-request board cannot be all-or-nothing.  The first version of this
    was, and the Starbucks WAF block at page 239 threw away 2,380 pages of
    work.  So each page is written and the cursor is recorded: a run that dies
    mid-board leaves state {"status":"partial","start":N} and the next run
    resumes at exactly page N.  Duplicates are bounded by the checkpoint
    interval (at most CKPT*PAGE rows re-fetched after a hard kill), not by the
    board size, and the canonical apply_url collapses those downstream anyway.
    """
    host = f"{tenant}.eightfold.ai"
    if not resolves(host):
        raise DeadTenant("NXDOMAIN")
    prev = _state.get(tenant)
    prev = prev if isinstance(prev, dict) else {}
    s = cc.session()
    try:
        dom = prev.get("domain") or ""
        company = prev.get("company") or ""
        gen = prev.get("gen") or ""
        start = int(prev.get("start") or 0)
        count = prev.get("count")
        written = int(prev.get("written") or 0)
        # Neither generation pages atomically: a posting whose rank shifts
        # between two requests is served on both pages.  Measured over a
        # 14,363-row pass: 49 apply_urls emitted twice (92 rows on microsoft,
        # 4 on micron, 2 on eaton).  Small, but it breaks the one invariant
        # this pipe is checked against — rows == distinct apply_url — so the
        # ids seen so far in THIS board are held for the length of the board.
        # It is intentionally not persisted across runs: a resumed board picks
        # up at the cursor and never re-reads the pages it already wrote.
        seen = set()
        if not dom:
            dom, company = discover(s, host, tenant)
        if not gen:
            gen, pos, count = pick_generation(s, host, dom)
            n = flush(tenant, host, company, gen, pos, seen)
            written += n
            start = PAGE
            checkpoint(tenant, dom, company, gen, start, count, written)

        total = count if isinstance(count, int) else 0
        pages = 0
        while pages < MAX_PAGES:
            if total and start >= total:
                break
            batch, c = page_api(s, host, dom, gen, start)
            if c is not None:
                count = c
                total = c if isinstance(c, int) else total
            if not batch:
                break
            written += flush(tenant, host, company, gen, batch, seen)
            start += PAGE
            pages += 1
            if pages % CKPT == 0:
                checkpoint(tenant, dom, company, gen, start, count, written)
        return written, gen, count
    finally:
        s.close()


def flush(tenant, host, company, gen, batch, seen):
    rows = []
    for p in batch:
        if not isinstance(p, dict):
            continue
        r = to_row(tenant, host, company, p)
        if r and r["apply_url"] not in seen:
            seen.add(r["apply_url"])
            rows.append(r)
    emit(rows)
    return len(rows)


def checkpoint(tenant, dom, company, gen, start, count, written):
    with _lock:
        _state[tenant] = {"status": "partial", "domain": dom,
                          "company": company, "gen": gen, "start": start,
                          "count": count, "written": written}
        cc.save_state(STATE, _state)
        cc.save_state(LEDGER, _ledger)


def emit(rows):
    global _writer
    with _lock:
        if _writer is None:
            _writer = cc.Writer(OUT)
        for r in rows:
            _writer.write(r)
        _counts["rows"] += len(rows)


def settled(tenant):
    """True once a tenant needs no more work.

    A dict value is a PARTIAL board — it carries the resume cursor and must be
    picked up again, so it is deliberately not settled.  Only the literal
    strings written at the end of a clean board, or on a confirmed dead one,
    stop the retry.
    """
    v = _state.get(tenant)
    return isinstance(v, str) and (v == "done" or v.startswith("dead"))


def work(tenant):
    if settled(tenant):
        return
    try:
        written, gen, count = fetch_tenant(tenant)
    except DeadTenant as e:
        with _lock:
            _state[tenant] = f"dead:{e}"[:200]
            _counts["dead"] += 1
        print(f"  {tenant}: DEAD ({e})", flush=True)
        return
    except Exception as e:               # noqa: BLE001
        # The checkpoint written inside fetch_tenant stays in the state file,
        # so the tenant resumes mid-board rather than restarting.  It is NOT
        # overwritten with an err: string here — that would throw the cursor
        # away, which is the bug this whole checkpoint exists to prevent.
        with _lock:
            _counts["err"] += 1
            _failed_now.add(tenant)
        print(f"  {tenant}: transient {type(e).__name__}: {e}", flush=True)
        return
    with _lock:
        _state[tenant] = "done"
        _counts["live" if written else "empty"] += 1
    print(f"  {tenant}: {written} rows via {gen} (api count={count})",
          flush=True)


def main():
    with open(TENANTS, newline="", encoding="utf-8") as f:
        tenants = [(r.get("tenant") or "").strip()
                   for r in csv.DictReader(f) if (r.get("tenant") or "").strip()]
    if ONLY:
        tenants = [t for t in tenants if t in ONLY]
    todo = [t for t in tenants if not settled(t)]
    # Longest board first: with one worker per tenant the run is as long as its
    # slowest tenant, and Starbucks alone is ~10x the next one.
    todo.sort(key=lambda t: -int((_state.get(t) or {}).get("count") or 0)
              if isinstance(_state.get(t), dict) else 0)
    print(f"eightfold: {len(tenants)} tenants, {len(todo)} to fetch",
          flush=True)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    stopped = False
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(work, t) for t in todo]
        for _ in cf.as_completed(futs):
            with _lock:
                cc.save_state(STATE, _state)
                cc.save_state(LEDGER, _ledger)
            if deadline and time.time() > deadline:
                stopped = True
                for fu in futs:
                    fu.cancel()
                break
    cc.save_state(STATE, _state)
    cc.save_state(LEDGER, _ledger)
    if _writer is not None:
        _writer.close()
    if _n429:
        print(f"  rate-limit backoffs: {_n429}", flush=True)
    print(f"DONE eightfold_fetch: {_counts}  -> {OUT}", flush=True)
    if stopped:
        print("BUDGET REACHED — unreached tenants stay pending.", flush=True)
        return 0
    if _failed_now:
        print(f"{len(_failed_now)} tenants failed transiently — re-run to "
              f"retry: {sorted(_failed_now)}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
