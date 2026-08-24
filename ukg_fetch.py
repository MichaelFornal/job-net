#!/usr/bin/env python3
"""
ukg_fetch.py — UKG Pro Recruiting (ex-UltiPro) multi-tenant fetcher.

ROUTE  (verified 2026-08-20 against real tenants mined out of all_jobs_master)
-----------------------------------------------------------------------------
    POST https://{HOST}/{TENANT}/JobBoard/{boardUuid}/JobBoardView/LoadSearchResults
    Content-Type: application/json
    {"opportunitySearch": {"Top": 500, "Skip": 0}}

    -> {"opportunities":[{Id,Title,RequisitionNumber,FullTime,JobCategoryName,
                          Locations:[...],PostedDate,BriefDescription,
                          JobLocationType,OpportunityType,...}, ...],
        "totalCount": n,
        "locations":  [ ...facet... ]}

One request per 500 jobs; `Skip` pages.  No auth, no cookie, no CSRF token, no
per-customer secret.  Measured 2026-08-20:

    ADV1023AVR  (Advarra, recruiting.ultipro.com)     totalCount 11
    DEA1005DEAC (Deaconess, deaconess.rec.pro.ukg.net) totalCount 1006
                Top=1000 -> 1000 rows, Skip=1000 -> the remaining 6

THE EMPTY-BODY TRAP  (this one is silent, and it is the reason for send_body())
------------------------------------------------------------------------------
POSTing `{}` — or anything without an `opportunitySearch` object — is NOT an
error.  It answers

    HTTP 200  application/json  {"opportunities":[],"totalCount":0,"locations":[]}

on a board that has 11 live jobs.  A fetcher that posts an empty body reads
every tenant on the platform as a live-but-empty board and writes that verdict
into its state file permanently.  `opportunitySearch` is therefore built in one
place and asserted non-empty before the request goes out.

RULE 1 — A 200 IS NOT SUCCESS.  Payload shape is the only verdict.
------------------------------------------------------------------
Measured discriminators on recruiting.ultipro.com:

    real tenant + real uuid          -> 200 application/json, "opportunities" list
    real tenant + fake uuid          -> 404 text/html, 17,144 B ("Not Found" shell)
    fake tenant                      -> 404 text/html, 17,144 B
    recruiting2 tenant asked on pod 1-> 404 text/html, 17,144 B

So every refusal on this platform arrives as an HTML page, and HTML is exactly
what `r.json()` chokes on.  ok() below accepts a response only when it is a
JSON object carrying an `opportunities` LIST and an integer `totalCount`; the
status code alone is never consulted.  A 200 whose body is an edge/WAF
challenge is classified Transient, not Dead — a blocked board is unknown, not
retired.

RULE 3 — dead / empty / error are three different verdicts.
-----------------------------------------------------------
    Dead      404 shell at the recorded host AND at the sibling pod AND the
              board uuid could not be re-read off /{TENANT}/JobBoardView.
              Recorded in state; never retried.
    live, 0   200 with a well-formed JSON payload and totalCount 0.  A real
              board between requisitions.  Recorded as live.
    Transient 429 / 5xx / timeout / connection reset / 403 / 200-challenge.
              RAISED, so the board is left absent from the state file and the
              next run picks it up again.  Never recorded as an empty board.

RULE 2 — throttle decays.  slow_down()/speed_up() are the ats_fetch.py pair:
1.5x per 429 up to a 1.0 s/req ceiling, 0.9x back toward the configured rate
after 40 clean responses.  Buckets are per HOST, because recruiting.ultipro.com
and recruiting2.ultipro.com are two shared pods carrying 320 of the 384 boards
while the other 64 sit on their own single-tenant vanity host.

RULE 4 — apply_url is the employer's own posting URL:

    https://{HOST}/{TENANT}/JobBoard/{boardUuid}/OpportunityDetail?opportunityId={Id}

That is the same URL shape that already appears in all_jobs_master.csv (it is
what the tenant list was mined out of), it is what the board's own job cards
link to, and it contains no redirect hop and no tracking parameter.  Id is a
server-side GUID and is stable across runs.

POD / HOST IDENTITY
-------------------
`recruiting.ultipro.com` and `recruiting2.ultipro.com` are numbered pods and a
tenant lives on exactly one of them; asking the wrong pod gives the 404 shell.
`recruiting3..8.ultipro.com` do not resolve (measured: DNS failure), so the pod
space is exactly two.  The `*.rec.pro.ukg.net` vanity hosts are NOT aliases of
those pods — DEA1005DEAC answers on deaconess.rec.pro.ukg.net and 404s on both
ultipro pods — so the host is part of board identity and the state key is
`host|TENANT|uuid`, never the tenant alone.

Sibling-pod retry is therefore attempted only for *.ultipro.com boards (a
tenant that migrated pods), and only on the dead path, where it costs nothing.

WHAT THE PAYLOAD DOES *NOT* TELL YOU
------------------------------------
`JobLocationType` looks like a remote flag and is not one.  Measured across 14
boards / 3,112 postings: type 2 covers both "Remote - U.S.A" (Advarra) and
ordinary in-office Evansville billing jobs (Deaconess), and one Advarra posting
whose location literally reads "Remote - U.S.A" carries type None.  Only 25 of
66 type-2 rows say "remote" anywhere.  It is not used.  work_mode comes from
the location display text and the title, the same evidence a human reads off
the card, and stays blank when there is none — a wrong remote flag is worse
than no flag.

`OpportunityType` is likewise not a talent-community marker: the 51 rows with
OpportunityType 1 in that sample are ordinary requisitions with ordinary
requisition numbers.  Nothing is filtered on it.

There is no employment-type field beyond the `FullTime` boolean — the detail
routes (LoadOpportunity / LoadOpportunityDetails) do not exist, they 404 — so
FullTime is handed to cc.classify_any as the raw type, which lets a title like
"... (Contract)" still win over it.

POSTED DATES
------------
`PostedDate` is the employer's own posting timestamp and was present on
3,112/3,112 sampled postings, so this family needs no ledger and no first-seen
fallback — but it does NOT go straight into `posted`: the fractional second is
variable-length and Python 3.9's fromisoformat (which cc.ymd is built on)
rejects anything that is not 3 or 6 digits.  See posted_of().

OUT OF SCOPE: UKG Ready (secure*.saashr.com) — see ukg_candidates.csv.

Output: contract_norm/ukg.jsonl    State: ukg_fetch_state.json
Tenants: tenants/ukg.csv           Census: ukg_candidates.csv
"""
import concurrent.futures as cf
import csv
import html
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "ukg.csv")

SOURCE = "ukg"

# UKG_KEEP_ALL defaults ON: every modern pipe in this repo writes a full
# employment-type dump and lets the consumer filter.  Setting it to 0 writes to
# a DIFFERENT path OUTSIDE contract_norm/ on purpose — cc.norm_files() reads
# every *.jsonl stem in that directory, so a contract-only file sitting next to
# the full one would be counted as a second source and duplicate every row.
KEEP_ALL = os.environ.get("UKG_KEEP_ALL", "1") != "0"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
OUT = (os.path.join(cc.NORM_DIR, "ukg.jsonl") if KEEP_ALL
       else os.path.join(BASE, "ukg_contract_only.jsonl"))
STATE = os.path.join(BASE, "ukg_fetch_state.json" if KEEP_ALL
                     else "ukg_contract_fetch_state.json")

RPS = float(os.environ.get("UKG_RPS") or 3.0)          # per HOST bucket
WORKERS = int(os.environ.get("UKG_WORKERS") or 12)
TOP = int(os.environ.get("UKG_TOP") or 500)            # 1000 verified working
TIMEOUT = int(os.environ.get("UKG_TIMEOUT") or 60)
TRIES = int(os.environ.get("UKG_TRIES") or 4)
BUDGET_MIN = float(os.environ.get("UKG_BUDGET_MIN") or 0)   # 0 = no limit
LIMIT = int(os.environ.get("UKG_LIMIT") or 0)               # 0 = all boards
REFRESH = os.environ.get("UKG_REFRESH") == "1"
ONLY = (os.environ.get("UKG_ONLY") or "").strip()      # substring board filter
MAX_PAGES = int(os.environ.get("UKG_MAX_PAGES") or 60)      # 30k jobs ceiling

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

POD_HOSTS = ("recruiting.ultipro.com", "recruiting2.ultipro.com")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {}
_interval = {}
_pause_until = {}
_n429 = {}
_ok_streak = {}

_state = cc.load_state(STATE)
_stats = {"live": 0, "empty": 0, "dead": 0, "transient": 0, "moved": 0,
          "dup": 0, "rows": 0}


class Transient(Exception):
    """Retryable.  Raised so the board is left PENDING rather than recorded as
    a completed empty board — the distinction rule 3 is about."""


class Dead(Exception):
    """The board is provably gone.  Carries the discriminator that proved it,
    so the state file records WHY and a later reader can tell a real retirement
    from a misclassification."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


# ---- throttle: per-host bucket, 429 backoff that DECAYS -------------------
def _bucket(host):
    with _rate_lock:
        if host not in _interval:
            _interval[host] = 1.0 / RPS
            _next_t[host] = 0.0
            _pause_until[host] = 0.0


def throttle(host):
    _bucket(host)
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[host]:
                wait = _pause_until[host] - now
            else:
                wait = _next_t[host] - now
                if wait <= 0:
                    _next_t[host] = max(_next_t[host] + _interval[host], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(host):
    _bucket(host)
    with _rate_lock:
        _pause_until[host] = time.time() + 20
        _interval[host] = min(_interval[host] * 1.5, 1.0)
        _n429[host] = _n429.get(host, 0) + 1


def speed_up(host):
    """Walk a host's interval back toward its configured rate after a run of
    clean responses.  Without this, slow_down() is a one-way ratchet: a handful
    of 429s in the opening minutes pin the pod at its floor for the rest of the
    run whatever UKG_RPS says.  Recovery is deliberately slower than the
    backoff (0.9x per 40 clean responses vs 1.5x per 429) so a host that is
    genuinely refusing still wins the argument."""
    _bucket(host)
    with _rate_lock:
        n = _ok_streak.get(host, 0) + 1
        if n < 40:
            _ok_streak[host] = n
            return
        _ok_streak[host] = 0
        floor = 1.0 / RPS
        if _interval[host] > floor:
            _interval[host] = max(_interval[host] * 0.9, floor)


# ---- payload-shape validation (rule 1) ------------------------------------
_CHALLENGE = re.compile(
    r"_Incapsula_|incapsula|Request unsuccessful|Just a moment|"
    r"cf-browser-verification|/cdn-cgi/challenge|captcha|Access Denied|"
    r"Pardon Our Interruption", re.I)


def ok_payload(d):
    """True only for the real search payload: a JSON object with an
    `opportunities` LIST and an integer `totalCount`.  Nothing else counts —
    not a 200, not a JSON body of some other shape."""
    return (isinstance(d, dict)
            and isinstance(d.get("opportunities"), list)
            and isinstance(d.get("totalCount"), int)
            and not isinstance(d.get("totalCount"), bool))


def send_body(skip):
    """The one place the request body is built.  `opportunitySearch` MUST be
    present and MUST carry Top: a body without it answers 200 with
    totalCount 0 on a board full of live jobs (see module docstring)."""
    body = {"opportunitySearch": {"Top": TOP, "Skip": int(skip),
                                  "QueryString": ""}}
    assert body["opportunitySearch"].get("Top"), "empty opportunitySearch"
    return body


def search(s, host, tenant, uuid, skip):
    """POST one page.  -> payload dict.  Raises Dead or Transient."""
    url = ("https://%s/%s/JobBoard/%s/JobBoardView/LoadSearchResults"
           % (host, tenant, uuid))
    last = None
    for attempt in range(TRIES):
        throttle(host)
        try:
            r = s.post(url, json=send_body(skip), timeout=TIMEOUT,
                       allow_redirects=False)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        code = r.status_code
        body = r.text or ""
        if code == 200:
            if _CHALLENGE.search(body[:4000]):
                # An edge challenge is "unknown", never "retired".
                last = "challenge_200"
                slow_down(host)
                time.sleep(2 * (attempt + 1))
                continue
            try:
                d = r.json()
            except ValueError:
                # 200 carrying HTML.  This USED to raise Dead, which retires
                # the board permanently with no recovery path -- and a
                # maintenance page, a WAF interstitial or a login redirect all
                # look exactly like this while the board is perfectly alive.
                # That is the repo's own liveness rule: `blocked` is not
                # `dead`, it is UNKNOWN.  Retry within this call, then leave
                # the board PENDING rather than condemning it.
                body = (r.text or "")[:400].lower()
                if attempt < TRIES - 1:
                    slow_down(host)
                    time.sleep(2 * (attempt + 1))
                    continue
                raise Transient("http200_not_json (%s)" % body[:60].strip())
            if ok_payload(d):
                speed_up(host)
                return d
            # A 200 whose JSON is the wrong shape is a stronger signal than
            # HTML -- the app parsed the request and answered something else --
            # but it is still only ONE observation.  Dead is reserved for the
            # discriminators the header documents (404 at the recorded host AND
            # the sibling pod), so this stays retryable too.
            if attempt < TRIES - 1:
                time.sleep(1 + attempt)
                continue
            raise Transient("http200_bad_shape keys=%s"
                            % sorted(list(d)[:6]) if isinstance(d, dict)
                            else "http200_bad_shape")
        if code in (404, 410):
            # The measured retirement signal on this platform: a 17KB
            # "Not Found" HTML shell for a fake tenant, a fake board uuid, or
            # the right tenant asked on the wrong pod.
            raise Dead("http%d" % code)
        if code in (429, 418, 503):
            slow_down(host)
            last = "HTTP %d" % code
        elif code == 403:
            # Edge refusal, not an application refusal: UKG's own "this board
            # does not exist" answer is the 404 shell above.  Blocked ≠ dead.
            slow_down(host)
            last = "HTTP 403"
        elif code in (301, 302, 303, 307, 308):
            last = "HTTP %d -> %s" % (code, r.headers.get("Location", ""))
        else:
            last = "HTTP %d" % code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s skip=%s failed after %d tries: %s"
                    % (url, skip, TRIES, last))


# ---- board-uuid / company recovery off the rendered board page -------------
_ALT_RX = re.compile(r'GetSmallHeaderLogo[^>]{0,200}?\balt="([^"]{1,150})"')


def board_page(s, host, tenant):
    """GET /{TENANT}/JobBoardView (the tenant root 302s here).  Returns the
    HTML or None.  Only ever called on the dead path or when the tenant list
    carries no company name, so its ~700KB cost is paid rarely."""
    url = "https://%s/%s/JobBoardView" % (host, tenant)
    for attempt in range(2):
        throttle(host)
        try:
            r = s.get(url, timeout=TIMEOUT, allow_redirects=True,
                      headers={"Accept": "text/html"})
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200 and r.text:
            return r.text
        if r.status_code in (404, 410):
            return None
        time.sleep(2 * (attempt + 1))
    return None


def uuid_from_page(body, tenant):
    """The live board uuid, re-read off the board's own markup.  Every board
    page links its own /{TENANT}/JobBoard/{uuid}/... assets dozens of times, so
    the modal match is the right one."""
    if not body:
        return None
    rx = re.compile(re.escape("/%s/JobBoard/" % tenant)
                    + r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                      r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", re.I)
    hits = {}
    for m in rx.finditer(body):
        u = m.group(1).lower()
        if u == "00000000-0000-0000-0000-000000000000":
            continue
        hits[u] = hits.get(u, 0) + 1
    if not hits:
        return None
    return max(hits.items(), key=lambda kv: kv[1])[0]


def company_from_page(body):
    if not body:
        return ""
    m = _ALT_RX.search(body)
    return sq(m.group(1)) if m else ""


# ---- normalisation ---------------------------------------------------------
_WS = re.compile(r"\s+")


def sq(v):
    return _WS.sub(" ", html.unescape(str(v or ""))).strip()


def _name_of(code_or_name):
    """UKG writes Country/State either as {"Code":..,"Name":..} or as null."""
    if isinstance(code_or_name, dict):
        return sq(code_or_name.get("Name")) or sq(code_or_name.get("Code"))
    return sq(code_or_name)


# UKG's placeholders for "no city on this requisition".  They are stripped
# BEFORE the display string is assembled, or a country-wide posting renders as
# "NA, North Carolina, United States" and a remote one as "United States of
# America, United States".
_CITY_PLACEHOLDER = {"", "na", "n/a", "none", "any", "various", "tbd",
                     "united states of america", "united states", "usa", "us",
                     "remote", "virtual"}


def loc_parts(l):
    """(display, city, state, country) for one Locations[] entry."""
    if not isinstance(l, dict):
        return ("", "", "", "")
    addr = l.get("Address") if isinstance(l.get("Address"), dict) else {}
    city = sq(addr.get("City"))
    state = _name_of(addr.get("State"))
    country = _name_of(addr.get("Country"))
    if city.lower() in _CITY_PLACEHOLDER:
        city = ""
    disp = sq(l.get("LocalizedName"))
    if not disp:
        disp = ", ".join(x for x in (city, state, country) if x)
    return (disp, city, state, country)


# YYYY-MM-DD at the front of an ISO timestamp.
_DATE_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})\b")
_FRAC = re.compile(r"^(.*T\d{2}:\d{2}:\d{2})\.(\d+)(.*)$")


def posted_of(v):
    """`PostedDate` -> YYYY-MM-DD.

    UKG stamps a VARIABLE-LENGTH fractional second — '...:27.56Z' with two
    digits, '...:27.042Z' with three — and Python 3.9's fromisoformat, which is
    what cc.ymd() is built on, accepts only 3 or 6.  Measured on the first
    25-board smoke run: 73 of 742 rows came back undated for that reason alone,
    and an undated row is not a visible failure — all_jobs_build keeps undated
    rows unconditionally past its posted cutoff, so they never age out.  This
    is the same shape of bug cc.ymd's own docstring records for Recruitee's
    trailing ' UTC'.

    The fraction is padded/truncated to 6 digits so cc.ymd still does the
    parsing; the bare date prefix is the last resort.
    """
    s = sq(v)
    if not s:
        return ""
    m = _FRAC.match(s)
    if m:
        s = "%s.%s%s" % (m.group(1), (m.group(2) + "000000")[:6], m.group(3))
    d = cc.ymd(iso=s)
    if d:
        return d
    m = _DATE_PREFIX.match(sq(v))
    return m.group(1) if m else ""


def to_row(host, tenant, uuid, company, o):
    title = sq(o.get("Title"))
    jid = sq(o.get("Id"))
    if not title or not jid:
        return None
    locs = [x for x in (o.get("Locations") or []) if isinstance(x, dict)]
    parts = [loc_parts(x) for x in locs]
    disp = [p[0] for p in parts if p[0]]
    # Multi-location requisitions stay ONE row (one Id, one apply_url); the
    # extra sites ride along in `location` so a geo filter can still see them.
    location = "; ".join(dict.fromkeys(disp))[:300]
    city, state, country = (parts[0][1], parts[0][2], parts[0][3]) if parts \
        else ("", "", "")
    # JobLocationType is NOT a remote flag on this platform (module docstring),
    # so work mode is read off the same text a human reads off the card.
    mode = cc.work_mode(text=location + " " + title)
    return cc.row(
        source=SOURCE, source_type="ats",
        company=company or tenant,
        title=title,
        # FullTime is the only type field UKG Pro publishes.  classify_any
        # consults the title FIRST for contingent wording, so a posting called
        # "... (Contract)" is still labelled Contract even though FullTime is
        # true; when nothing says otherwise it resolves to Full-time/Part-time.
        employment_type=CLASSIFY(
            "Full-time" if o.get("FullTime") else "Part-time", title),
        work_mode=mode, location=location,
        city=city, state=state, country=country,
        remote=(mode == "Remote"),
        posted=posted_of(o.get("PostedDate")),
        apply_url=("https://%s/%s/JobBoard/%s/OpportunityDetail"
                   "?opportunityId=%s" % (host, tenant, uuid, jid)),
        job_id=jid, board="%s/%s" % (host, tenant),
        board_url="https://%s/%s/JobBoard/%s" % (host, tenant, uuid))


# ---- one board -------------------------------------------------------------
def page_board(s, host, tenant, uuid, company):
    """Every opportunity on one board.  Raises Dead / Transient."""
    rows = []
    seen = set()
    skip = 0
    total = None
    for _page in range(MAX_PAGES):
        d = search(s, host, tenant, uuid, skip)
        opps = d.get("opportunities") or []
        if total is None:
            total = d.get("totalCount") or 0
        got = 0
        for o in opps:
            if not isinstance(o, dict):
                continue
            jid = sq(o.get("Id"))
            if not jid or jid in seen:
                continue
            seen.add(jid)
            r = to_row(host, tenant, uuid, company, o)
            if r is None:
                continue
            if not KEEP_ALL and not r["employment_type"]:
                continue
            rows.append(r)
            got += 1
        if not opps:
            break
        skip += len(opps)
        if skip >= (total or 0):
            break
        if got == 0 and len(seen) >= (total or 0):
            break
    return rows, (total or 0)


def resolve_and_fetch(s, host, tenant, uuid, company):
    """-> (host, uuid, rows, total, note).  Raises Dead / Transient.

    On a Dead verdict at the recorded coordinates, two cheap recoveries are
    tried before the board is retired for good:
      1. the sibling pod, for *.ultipro.com boards only (a tenant that moved
         pods; vanity *.rec.pro.ukg.net hosts are NOT pod aliases and are
         never substituted);
      2. the board uuid re-read off /{TENANT}/JobBoardView, for a board whose
         uuid was rotated under a host that is still alive.
    """
    try:
        rows, total = page_board(s, host, tenant, uuid, company)
        return host, uuid, rows, total, ""
    except Dead as e:
        # Bind the reason to a NORMAL name: Python 3 deletes the `except ... as
        # X` target at the end of the clause, so re-raising `X` at the bottom
        # of this function would be a NameError on the one path that matters —
        # the path where a board really is dead.
        first_reason = e.reason

    cands = [host]
    if host in POD_HOSTS:
        cands += [h for h in POD_HOSTS if h != host]

    for h in cands:
        body = board_page(s, h, tenant)
        u2 = uuid_from_page(body, tenant)
        if not u2:
            continue
        if not company:
            company = company_from_page(body)
        try:
            rows, total = page_board(s, h, tenant, u2, company)
        except Dead:
            continue
        note = []
        if h != host:
            note.append("pod:%s->%s" % (host, h))
        if u2 != uuid:
            note.append("uuid:%s->%s" % (uuid[:8], u2[:8]))
        return h, u2, rows, total, ",".join(note)

    raise Dead(first_reason)


def work(w, tenant, boards):
    """One TENANT (all of its boards, on every host it answers on), fetched
    and recorded atomically.

    The work unit is the tenant, not the board, because 36 of the 348 tenants
    publish two or more boards and a requisition can sit on both.  Measured on
    the first full 384-board run: 330 opportunity GUIDs appeared on two boards
    of the SAME tenant, 362 rows in all.  Those are one posting each, but the
    board uuid is inside the apply_url, so board-at-a-time fetching emits two
    different apply_urls for them — 362 permanent false-uniques in
    all_jobs_build, which is precisely what rule 4 forbids.  Boards are walked
    in sorted (host, uuid) order and a GUID already seen on an earlier board of
    the same tenant is dropped, so the surviving apply_url is a deterministic
    function of the board list and does not move between runs.

    The unit is keyed by the TENANT CODE, not by (host, tenant): CEN1046CENEQ
    and GIB1002GIBIN each answer on their own vanity host AND on the shared
    gusea1p01.rec.pro.ukg.net, and a (host, tenant) unit would let the same
    requisition through twice, once per host.

    Atomicity is the other half: if ANY board of the tenant comes back
    Transient, nothing is written and nothing is recorded, so the retry cannot
    double-write the boards that did succeed.  Dead boards inside an otherwise
    healthy tenant are recorded as dead and contribute no rows.
    """
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA,
                      "Accept": "application/json, text/plain, */*",
                      "Accept-Language": "en-US,en;q=0.9",
                      "Content-Type": "application/json"})
    seen = set()
    pending = []          # (key, record, rows)
    try:
        for host, uuid, company in sorted(boards):
            key = "%s|%s|%s" % (host, tenant, uuid)
            s.headers["Origin"] = "https://%s" % host
            s.headers["Referer"] = ("https://%s/%s/JobBoard/%s"
                                    % (host, tenant, uuid))
            try:
                h2, u2, rows, total, note = resolve_and_fetch(
                    s, host, tenant, uuid, company)
            except Dead as e:
                pending.append((key, {"status": "dead", "why": e.reason,
                                      "at": TODAY}, []))
                continue
            except Transient as e:
                with _lock:
                    _stats["transient"] += 1
                print("  TRANSIENT %s: %s" % (key, e), flush=True)
                return
            except Exception as e:                               # noqa: BLE001
                with _lock:
                    _stats["transient"] += 1
                print("  TRANSIENT %s: %s: %s" % (key, type(e).__name__, e),
                      flush=True)
                return
            fresh = []
            for r in rows:
                if r["job_id"] in seen:
                    continue
                seen.add(r["job_id"])
                fresh.append(r)
            pending.append((key, {"status": "live", "jobs": len(fresh),
                                  "total": total, "at": TODAY,
                                  "host": h2, "uuid": u2,
                                  "dup_dropped": len(rows) - len(fresh),
                                  "note": note}, fresh))
    finally:
        s.close()

    with _lock:
        for key, rec, rows in pending:
            _state[key] = rec
            if rec["status"] == "dead":
                _stats["dead"] += 1
                continue
            for r in rows:
                w.write(r)
            _stats["rows"] += len(rows)
            _stats["dup"] += rec.get("dup_dropped") or 0
            _stats["live" if rows else "empty"] += 1
            if rec.get("note"):
                _stats["moved"] += 1


# ---- tenants ---------------------------------------------------------------
UUID_RX = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                     r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
HOST_RX = re.compile(r"^[A-Za-z0-9.-]+\.(ultipro\.com|ukg\.net)$")


def load_boards():
    out = []
    if not os.path.exists(TENANTS):
        print("ukg: no %s — run ukg_discover_boards.py first" % TENANTS,
              flush=True)
        return out
    with open(TENANTS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            host = (r.get("host") or "").strip().lower()
            tenant = (r.get("tenant") or "").strip()
            uuid = (r.get("board_uuid") or "").strip().lower()
            if not (HOST_RX.match(host) and tenant and UUID_RX.match(uuid)):
                continue
            out.append((host, tenant, uuid, (r.get("company") or "").strip()))
    return out


def main():
    boards = load_boards()
    if ONLY:
        boards = [b for b in boards if ONLY.lower() in
                  ("%s|%s|%s|%s" % b).lower()]
    # Work unit = tenant (see work()).  UKG_LIMIT counts tenants.
    units = {}
    for host, tenant, uuid, company in boards:
        units.setdefault(tenant, []).append((host, uuid, company))
    todo = [(t, bs) for t, bs in sorted(units.items())
            if REFRESH or any(
                (_state.get("%s|%s|%s" % (h, t, u)) or {}).get("status")
                not in ("live", "dead") for h, u, _c in bs)]
    already = len(units) - len(todo)
    random.shuffle(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("ukg: %d boards / %d tenants, %d tenants to fetch (%d already "
          "recorded)  keep_all=%s out=%s"
          % (len(boards), len(units), len(todo), already, KEEP_ALL,
             os.path.basename(OUT)), flush=True)
    if not todo:
        return 0

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, w, *u) for u in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 25 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                    print("  %d/%d  %s" % (done, len(todo), _stats), flush=True)
                if deadline and time.time() > deadline:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        with _lock:
            cc.save_state(STATE, _state)
        w.close()
    print("DONE ukg: %s  (429/backoffs: %s)%s"
          % (_stats, dict(_n429),
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
