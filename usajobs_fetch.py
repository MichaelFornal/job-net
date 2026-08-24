#!/usr/bin/env python3
"""
usajobs_fetch.py — federal job opportunity announcements (OPM / USAJOBS).

Federal postings are absent from the Getro / Lever / Greenhouse backbone
entirely, so every row here is a marginal company the master does not hold.

TWO PATHS, and the keyless one is the one that works TODAY
============================================================================
keyless (default)  GET https://data.usajobs.gov/api/historicjoa
                   NO auth of any kind.  Despite the name, the endpoint is
                   documented as "all CURRENT and past job postings" and that
                   is what it returns: measured 2026-07-29, announcements with
                   positionOpenDate == today and positionOpeningStatus
                   "Accepting applications" with close dates a month out.
                   The build spec claimed this was a dead archive; it is not.
                   Filtering to the still-open slice turns it into a live
                   federal board with structured salary, schedule, appointment
                   type and per-announcement open/close dates.

keyed (optional)   GET https://data.usajobs.gov/api/search
                   Needs USAJOBS_EMAIL + USAJOBS_API_KEY.  Measured 401 with a
                   browser UA, and 401 again with the documented
                   Host: data.usajobs.gov + email-as-User-Agent headers but no
                   Authorization-Key.  There is no keyless volume here at all.
                   Request the key at https://developer.usajobs.gov/apirequest/

Set USAJOBS_MODE=keyless|keyed|auto (default auto: keyed when both env vars
are present, else keyless).  Both paths emit the SAME apply_url shape
(https://www.usajobs.gov/job/<controlNumber>), so rows from either mode dedupe
against each other cleanly.

MEASURED TRAPS (all verified 2026-07-29 unless noted)
============================================================================
1. PageSize is NOT a valid historicjoa parameter.  Passing it returns
   HTTP 400 "Invalid parameter; make sure you provide a proper parameter.
   Parameter: PageSize" — as a PLAIN TEXT body, not JSON, so a fetcher that
   calls .json() on it dies with a decode error instead of a clear message.
   Page size is fixed at 500.
2. paging.metadata.totalCount is a REMAINING count, not a total.  It walks
   down 63994 -> 63494 -> 62994 as you page.  Treating it as a fixed total and
   computing "pages = total/500" over-runs by a factor of ~2; treating a drop
   as an error stops the walk early.  Termination is: empty data array.
3. The continuationToken paginator is real — page 1 and page 2 of the same
   window shared ZERO of 500 control numbers when checked.  (The spec warns
   about ignored offset params on other sources; this one is honest.)
4. teleworkEligible is "N" on 100% of 2,500 sampled CURRENT announcements.
   The field is structurally present but carries no signal in the live window,
   so this fetcher does NOT manufacture remote=True from it.  Rows are marked
   Remote only when a position location literally says so, and Hybrid only on
   an actual teleworkEligible == "Y".  Inflating remote here would poison the
   remote cut of the master for ~700 rows/day.
5. data.usajobs.gov answers HTTP 503 with an HTML body under load.  Status
   alone is not enough — every response is validated as a dict carrying a
   "data" array before it counts, and a body starting with "<" is retried,
   never recorded as "this window has no jobs".
6. The LAST page of a token walk drops the "paging" key entirely — it answers
   {"data": [...494 records...]} and nothing else.  Measured 2026-07-29 on the
   final page of a 90-day window.  A validator that requires both "paging" and
   "data" (the shape every other page has) rejects it, and the records lost
   are the WORST ones to lose: the tail of the walk is the freshest slice —
   that final page was entirely 2026-07-27/28 announcements.  So the contract
   here is: "data" is required, "paging" absent means this was the last page.
7. A dead announcement is indistinguishable from a live one by HTTP status:
   https://www.usajobs.gov/job/<n> returns 200 for a live control number and
   404 for a fabricated one, but the historicjoa record itself is the
   discriminator — positionOpeningStatus plus positionCloseDate.  We filter on
   the record, not by probing 60k URLs.
8. The keyed /api/search path is hard-capped at 10,000 rows per query
   (documented on developer.usajobs.gov/guides/rate-limiting, 500/page).  A
   flat page walk therefore silently truncates.  The keyed path here splits
   recursively on the refiner facets the API returns until every leaf query is
   under the cap.

ENV
  USAJOBS_EMAIL / USAJOBS_API_KEY   enable the keyed live-search path
  USAJOBS_MODE      auto | keyless | keyed          (default auto)
  USAJOBS_DAYS      keyless open-date window, days  (default 90)
  USAJOBS_MAX_PAGES pages per RUN in keyless mode, API calls in keyed mode
                    (default 600)
  USAJOBS_KEEP_CLOSED=1   keep closed/cancelled announcements too
  USAJOBS_KEEP_REVIEW=1   keep "Applications under review" (closed to new
                          applicants, but the announcement is still live)
  USAJOBS_APPEND=1  append to an existing jsonl instead of starting fresh
  USAJOBS_SLEEP     seconds between requests        (default 0.6)

Resumable: usajobs_fetch_state.json holds the window and the continuation
token, so an interrupted run picks up on the next page instead of re-walking
(and re-appending) everything.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
SOURCE = "usajobs"
SOURCE_TYPE = "ats"          # USAJOBS is the federal government's own ATS
OUT = os.path.join(cc.NORM_DIR, "usajobs.jsonl")
STATE_PATH = os.path.join(BASE, "usajobs_fetch_state.json")

HOST = "https://data.usajobs.gov"
HISTORIC = HOST + "/api/historicjoa"
SEARCH = HOST + "/api/search"
BOARD_URL = "https://www.usajobs.gov"
JOB_URL = "https://www.usajobs.gov/job/%s"
SIGNUP = "https://developer.usajobs.gov/apirequest/"

DAYS = int(os.environ.get("USAJOBS_DAYS") or 90)
MAX_PAGES = int(os.environ.get("USAJOBS_MAX_PAGES") or 600)
SLEEP = float(os.environ.get("USAJOBS_SLEEP") or 0.6)
KEEP_CLOSED = os.environ.get("USAJOBS_KEEP_CLOSED") == "1"
KEEP_REVIEW = os.environ.get("USAJOBS_KEEP_REVIEW") == "1"
# Append by DEFAULT.  This used to be opt-in, so a bare run opened OUT in "w"
# mode and any partial crawl replaced the corpus with whatever it managed to
# collect.  LazyWriter defers the truncate to the first row, so a total failure
# was survivable — but a partial one was not, and this exits `0 if n else 1`,
# which makes a short run look like a failure worth retrying (and re-truncating).
# USAJOBS_FRESH=1 for a deliberate clean rebuild.  USAJOBS_APPEND is kept as a
# no-op alias so existing callers and docs keep working.
APPEND = os.environ.get("USAJOBS_FRESH") != "1"
MODE = (os.environ.get("USAJOBS_MODE") or "auto").lower()

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
TIMEOUT = 120
TRIES = 5
SEARCH_ROW_CAP = 10000        # documented per-query ceiling on /api/search
SEARCH_PER_PAGE = 500

# positionOpeningStatus values seen in the wild.  Anything in DEAD is over.
DEAD_STATUS = {"job closed", "job canceled", "job cancelled",
               "candidate selected", "position filled"}
REVIEW_STATUS = {"applications under review"}

# USAJOBS carries a handful of OPM test announcements ("Space Monster (Green)"
# is a real one, agency AM00).  They are rare and always closed, but filter
# them by title so they can never reach the vet queue.
TEST_TITLE = re.compile(r"space monster|^\s*z*test (job|announcement)", re.I)

# The only remote signal available in the keyless payload is the position
# location text; USAJOBS writes remote roles as "Anywhere in the U.S. (remote
# job)".  "Location Negotiable After Selection" is NOT remote — it means the
# duty station is picked later — so it is deliberately excluded.
RX_REMOTE = re.compile(r"remote job|\(remote\)|anywhere in the u\.?s", re.I)
# USAJOBS puts these placeholders in the CITY slot.  They are policy strings,
# not places, and letting them through means city="Anywhere in the U.S.
# (remote job)" in the master's city column.
PSEUDO_CITY = re.compile(r"anywhere in the u\.?s|location negotiable|"
                         r"multiple locations|negotiable after selection",
                         re.I)

SAL_PERIOD = {
    "per year": "Year", "per hour": "Hour", "per month": "Month",
    "per week": "Week", "per day": "Day", "school year": "Year",
    "per academic year": "Year", "fee basis": "", "without compensation": "",
    "pa": "Year", "ph": "Hour", "pm": "Month", "pw": "Week", "pd": "Day",
    "py": "Year", "sy": "Year",
}


def log(msg):
    print(msg, flush=True)


class LazyWriter:
    """cc.Writer, but the output file is only truncated once a real row is in
    hand.

    Why: a run that dies before its first row (a 401, a dead host, a bad
    parameter) must not leave a ZERO-BYTE usajobs.jsonl behind — downstream
    that is indistinguishable from "the federal government posted no jobs
    today".  Deferring the truncate means yesterday's file survives a failed
    start, and the state file records the failure explicitly.
    """

    def __init__(self, path, append=False):
        self.path = path
        self.mode = "a" if append else "w"
        self.f = None
        self.n = 0

    def write(self, d):
        if self.f is None:
            self.f = open(self.path, self.mode, encoding="utf-8")
        self.f.write(json.dumps(d) + "\n")
        self.n += 1
        if self.n % 500 == 0:
            self.f.flush()

    def close(self):
        if self.f is not None:
            self.f.flush()
            self.f.close()
            self.f = None


def num(v):
    try:
        f = float(v)
        return round(f, 2) if f else ""
    except (TypeError, ValueError):
        return ""


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
    return {}


def save_state(st):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.replace(tmp, STATE_PATH)


class Transient(Exception):
    """The host failed to answer usably.  NOT the same as 'no jobs'."""


def get_json(s, url, params=None, want=("data",)):
    """GET that validates the BODY, not the status code.

    Returns a dict, or raises Transient after TRIES attempts.  A 400 with the
    plain-text 'Invalid parameter' body is fatal and raises SystemExit — it is
    a bug in the caller, not something to retry 5 times.
    """
    last = ""
    for attempt in range(TRIES):
        try:
            r = s.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        body = (r.text or "").lstrip()
        if r.status_code == 400 and "Invalid parameter" in body:
            raise SystemExit("USAJOBS rejected a parameter: %s" % body[:200])
        if r.status_code == 401:
            raise SystemExit(
                "USAJOBS /api/search returned 401 Unauthorized.\n"
                "  The Authorization-Key is missing, wrong, or not yet "
                "approved.\n  Request one at " + SIGNUP)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After") or 30)
            log("    429 from USAJOBS — sleeping %ds" % wait)
            time.sleep(wait)
            last = "HTTP 429"
            continue
        if r.status_code != 200 or not body or body[0] == "<":
            # 503 + an HTML maintenance page is the common failure here.
            last = "HTTP %d%s" % (r.status_code,
                                  " (html body)" if body[:1] == "<" else "")
            time.sleep(3 * (attempt + 1))
            continue
        try:
            d = r.json()
        except ValueError:
            last = "non-json body"
            time.sleep(2 * (attempt + 1))
            continue
        if not isinstance(d, dict) or any(k not in d for k in want):
            last = "unexpected shape: %s" % list(d)[:6]
            time.sleep(2 * (attempt + 1))
            continue
        return d
    raise Transient(last or "unknown")


# ---------------------------------------------------------------- row build
def _loc_bits(locs):
    """(display, city, state, country, is_remote) from a location list."""
    parts, city, state, country = [], "", "", ""
    for L in locs or []:
        c = (L.get("positionLocationCity") or "").strip()
        st = (L.get("positionLocationState") or "").strip()
        co = (L.get("positionLocationCountry") or "").strip()
        txt = ", ".join(x for x in (c, st, co) if x)
        if txt:
            parts.append(txt)
        if not city and c and not PSEUDO_CITY.search(c):
            city, state, country = c, st, co
        elif not country and co:
            country = co
    disp = "; ".join(parts[:8])
    if len(parts) > 8:
        disp += "; +%d more" % (len(parts) - 8)
    return disp, city, state, country, bool(RX_REMOTE.search(disp))


def _emp_raw(appointment, schedule):
    """Feed cc.classify_any a string that says what the federal words mean.

    A federal 'Term' appointment is an explicitly time-limited hire (not to
    exceed 4 years); left as the bare word 'Term' it falls through to
    Full-time.  'Detail' is a temporary reassignment.  Everything else already
    matches the shared vocabulary.
    """
    a = (appointment or "").strip()
    low = a.lower()
    if low == "term":
        a = "fixed-term"
    elif low == "detail":
        a = "temporary detail"
    elif low.startswith("internship"):
        a = "internship"
    elif low.startswith("recent graduate"):
        a = ""            # a hiring path, not an employment type
    return (a + " " + (schedule or "")).strip()


def row_from_historic(j):
    disp, city, state, country, is_remote = _loc_bits(
        j.get("positionlocations") or j.get("positionLocations"))
    telework = (j.get("teleworkEligible") or "").upper() == "Y"
    if is_remote:
        mode = "Remote"
    elif telework:
        mode = "Hybrid"       # federal "telework eligible" = partial, not full
    else:
        mode = ""
    period = SAL_PERIOD.get((j.get("salaryType") or "").strip().lower(), "")
    cn = j.get("usajobsControlNumber")
    agency = (j.get("hiringAgencyName") or j.get("hiringDepartmentName") or
              "U.S. Federal Government")
    return cc.row(
        source=SOURCE, source_type=SOURCE_TYPE,
        company=agency, title=j.get("positionTitle"),
        employment_type=cc.classify_any(
            _emp_raw(j.get("appointmentType"), j.get("workSchedule")),
            j.get("positionTitle")),
        work_mode=mode, remote=is_remote,
        location=disp, city=city, state=state, country=country,
        salary_min=num(j.get("minimumSalary")),
        salary_max=num(j.get("maximumSalary")),
        salary_currency="USD" if period else "",
        salary_period=period,
        posted=cc.ymd(iso=j.get("positionOpenDate")),
        apply_url=JOB_URL % cn, job_id=str(cn),
        board="usajobs", board_url=BOARD_URL)


def is_live(j):
    """Live == still open to applicants.  Returns (keep, reason).

    Measured over 2,994 sampled announcements (2026-07-29), status vs "close
    date is in the future":
        Accepting applications      1940 total, 1828 still open
        <status is null>             503 total,  502 still open   <-- keep!
        Applications under review    411 total,   42 still open
        Job closed / canceled /
          candidate selected         140 total,   36 still open
    Two things follow.  (a) A NULL status is not a dead record — it is a brand
    new announcement whose status has not been stamped yet, and filtering on
    `status == "Accepting applications"` alone would silently drop 500 live
    federal jobs per window, most of them the freshest ones.  (b) A status of
    "Accepting applications" is not proof either: 112 of them had already
    passed their close date, so the date gets the final say.
    """
    status = (j.get("positionOpeningStatus") or "").strip().lower()
    close = (j.get("positionCloseDate") or "")[:10]
    if TEST_TITLE.search(j.get("positionTitle") or ""):
        return False, "test-record"
    if KEEP_CLOSED:
        return True, ""
    if status in DEAD_STATUS:
        return False, "status:" + status
    if status in REVIEW_STATUS and not KEEP_REVIEW:
        return False, "under-review"
    if close and close < TODAY:
        # "Accepting applications" with a past close date happens — the status
        # field lags.  The date wins.
        return False, "closed:" + close
    return True, ""


# ------------------------------------------------------------- keyless walk
def run_keyless(s, out):
    st = load_state()
    start = (datetime.now(timezone.utc) - timedelta(days=DAYS)
             ).strftime("%Y-%m-%d")
    resume = (st.get("mode") == "keyless" and st.get("window") == start
              and st.get("token") and not st.get("done"))
    if resume:
        log("resuming keyless walk at page %d (window from %s)"
            % (st.get("pages", 0) + 1, start))
        url = HOST + st["token"]
        params = None
        pages = st.get("pages", 0)
        kept = st.get("kept", 0)
        seen = st.get("seen", 0)
    else:
        url, params = HISTORIC, {"StartPositionOpenDate": start}
        pages = kept = seen = 0
        log("keyless historicjoa walk, announcements opened since %s" % start)

    skipped = {}
    ids = set()
    t0 = time.time()
    done = False
    last_token = st.get("token") if resume else None
    # MAX_PAGES caps THIS run, not the cumulative walk — a resumed run that
    # inherited pages >= MAX_PAGES from state would otherwise skip the loop
    # entirely and report the carried-over counters as if it had fetched.
    pages_run = 0
    while pages_run < MAX_PAGES:
        try:
            d = get_json(s, url, params)
        except Transient as e:
            # A transport failure is NOT "no more jobs".  Persist the token so
            # the next run resumes, and exit non-zero.
            save_state({"mode": "keyless", "window": start,
                        "token": token_of(url), "pages": pages, "kept": kept,
                        "seen": seen, "done": False, "error": str(e)})
            out.close()
            raise SystemExit(
                "USAJOBS historicjoa failed after %d pages: %s — state saved, "
                "re-run to resume (rows so far: %d)" % (pages, e, kept))
        params = None
        data = d.get("data") or []
        meta = (d.get("paging") or {}).get("metadata") or {}
        if not data:
            done = True
            break
        pages += 1
        pages_run += 1
        for j in data:
            seen += 1
            cn = j.get("usajobsControlNumber")
            if cn in ids:
                continue
            ids.add(cn)
            keep, why = is_live(j)
            if not keep:
                skipped[why.split(":")[0]] = skipped.get(
                    why.split(":")[0], 0) + 1
                continue
            out.write(row_from_historic(j))
            kept += 1
        nxt = (d.get("paging") or {}).get("next")
        if not nxt:
            done = True
            break
        url = HOST + nxt
        last_token = nxt
        save_state({"mode": "keyless", "window": start, "token": nxt,
                    "pages": pages, "kept": kept, "seen": seen, "done": False})
        if pages % 10 == 0:
            log("  page %d | %d scanned | %d live rows | remaining~%s | %.0fs"
                % (pages, seen, kept, meta.get("totalCount"), time.time() - t0))
        time.sleep(SLEEP)

    # Keep the token when the walk stopped on the page cap — that is what a
    # re-run resumes from.  Clearing it would silently restart the window and
    # re-append every row already written.
    save_state({"mode": "keyless", "window": start,
                "token": None if done else last_token,
                "pages": pages, "kept": kept, "seen": seen, "done": done})
    log("keyless: %d pages this run (%d cumulative), %d announcements "
        "scanned, %d live rows in %.0fs"
        % (pages_run, pages, seen, kept, time.time() - t0))
    log("  dropped: " + (", ".join("%s=%d" % kv for kv in
                                   sorted(skipped.items())) or "none"))
    if not done:
        log("  WARNING: hit the %d-page cap before the window was exhausted — "
            "re-run to continue (state saved)." % MAX_PAGES)
    return kept


def token_of(url):
    return url[len(HOST):] if url.startswith(HOST) else None


# --------------------------------------------------------------- keyed walk
def keyed_headers(email, key):
    # Documented shape: Host + the consumer's email as User-Agent + the key.
    return {"Host": "data.usajobs.gov", "User-Agent": email,
            "Authorization-Key": key, "Accept": "application/json"}


def row_from_search(item):
    md = item.get("MatchedObjectDescriptor") or {}
    cn = item.get("MatchedObjectId") or md.get("PositionID")
    locs = []
    for L in md.get("PositionLocation") or []:
        locs.append({"positionLocationCity": L.get("CityName"),
                     "positionLocationState": L.get("CountrySubDivisionCode"),
                     "positionLocationCountry": L.get("CountryCode")})
    disp, city, state, country, is_remote = _loc_bits(locs)
    disp = md.get("PositionLocationDisplay") or disp
    details = ((md.get("UserArea") or {}).get("Details") or {})
    # Newer payloads expose an explicit RemoteIndicator; older ones only
    # TeleworkEligible.  Prefer the structured flag when it is there.
    ri = details.get("RemoteIndicator")
    tel = details.get("TeleworkEligible")
    if ri in (True, "True", "true", "Yes"):
        is_remote = True
    is_remote = is_remote or bool(RX_REMOTE.search(disp))
    mode = "Remote" if is_remote else (
        "Hybrid" if tel in (True, "True", "true", "Yes") else "")
    rem = (md.get("PositionRemuneration") or [{}])[0]
    period = SAL_PERIOD.get(
        str(rem.get("Description") or rem.get("RateIntervalCode") or ""
            ).strip().lower(), "")
    sched = "; ".join(x.get("Name") or "" for x in
                      (md.get("PositionSchedule") or []))
    offer = "; ".join(x.get("Name") or "" for x in
                      (md.get("PositionOfferingType") or []))
    return cc.row(
        source=SOURCE, source_type=SOURCE_TYPE,
        company=(md.get("OrganizationName") or md.get("DepartmentName")
                 or "U.S. Federal Government"),
        title=md.get("PositionTitle"),
        employment_type=cc.classify_any(_emp_raw(offer, sched),
                                        md.get("PositionTitle")),
        work_mode=mode, remote=is_remote,
        location=disp, city=city, state=state, country=country,
        salary_min=num(rem.get("MinimumRange")),
        salary_max=num(rem.get("MaximumRange")),
        salary_currency="USD" if period else "", salary_period=period,
        posted=cc.ymd(iso=md.get("PublicationStartDate")
                      or md.get("PositionStartDate")),
        # PositionURI is the /GetJob/ViewDetails/<n> form, which 301s to
        # /job/<n>; ApplyURI carries a ?PostingChannelID=RESTAPI query.  Both
        # normalize to the same canonical URL, which is also what the keyless
        # path emits — so the two modes dedupe against each other.
        apply_url=JOB_URL % cn, job_id=str(cn),
        board="usajobs", board_url=BOARD_URL)


SPLIT_AXES = [("Organization", "Organization"),
              ("JobCategoryCode", "JobCategoryCode"),
              ("PositionScheduleTypeCode", "PositionScheduleTypeCode"),
              ("SalaryBucket", "SalaryBucket"),
              ("GradeBucket", "GradeBucket")]


def search_page(s, params, page):
    p = dict(params)
    p.update({"ResultsPerPage": SEARCH_PER_PAGE, "Page": page})
    d = get_json(s, SEARCH, p, want=("SearchResult",))
    return d.get("SearchResult") or {}


def run_keyed(s, out, email, key):
    s.headers.update(keyed_headers(email, key))
    log("keyed /api/search walk (10,000-row cap per query -> facet split)")
    written = [0]
    ids = set()
    calls = [0]
    stats = {"transient": 0}

    def walk(params, depth):
        if calls[0] >= MAX_PAGES:
            return
        try:
            sr = search_page(s, params, 1)
        except Transient as e:
            # One flaky branch must not kill the crawl, and it must NOT be
            # recorded as "this facet has no jobs".
            log("  ! %s transient: %s (branch skipped, NOT empty)"
                % (params or "<all>", e))
            stats["transient"] += 1
            return
        calls[0] += 1
        total = int(sr.get("SearchResultCountAll") or 0)
        items = sr.get("SearchResultItems") or []
        if not items:
            return
        if total > SEARCH_ROW_CAP and depth < len(SPLIT_AXES):
            refs = ((sr.get("UserArea") or {}).get("Refiners") or {})
            axis, param = SPLIT_AXES[depth]
            vals = [r.get("RefinementValue") for r in (refs.get(axis) or [])
                    if r.get("RefinementValue")]
            if vals:
                log("  %d rows > cap; splitting on %s into %d parts"
                    % (total, axis, len(vals)))
                for v in vals:
                    sub = dict(params)
                    sub[param] = v
                    walk(sub, depth + 1)
                return
            return walk(params, depth + 1)
        pages = min((total + SEARCH_PER_PAGE - 1) // SEARCH_PER_PAGE,
                    SEARCH_ROW_CAP // SEARCH_PER_PAGE) or 1
        page = 1
        while page <= pages and calls[0] < MAX_PAGES:
            if page > 1:
                try:
                    sr = search_page(s, params, page)
                except Transient as e:
                    log("  ! page %d transient: %s (partition incomplete)"
                        % (page, e))
                    stats["transient"] += 1
                    break
                calls[0] += 1
                items = sr.get("SearchResultItems") or []
            if not items:
                break
            fresh = 0
            for it in items:
                cn = str(it.get("MatchedObjectId") or "")
                if not cn or cn in ids:
                    continue
                ids.add(cn)
                fresh += 1
                out.write(row_from_search(it))
                written[0] += 1
            # The measured anti-pattern: a paginator that ignores Page and
            # hands back page 1 forever.  Zero new ids on a full page means
            # exactly that — stop instead of "collecting" the same 500 rows.
            if page > 1 and fresh == 0 and len(items) == SEARCH_PER_PAGE:
                log("  WARNING: page %d returned 0 new ids — Page appears to "
                    "be ignored for %s; stopping this branch" % (page, params))
                break
            page += 1
            time.sleep(SLEEP)

    walk({}, 0)
    log("keyed: %d rows from %d API calls" % (written[0], calls[0]))
    if stats["transient"]:
        log("  %d branches ended on a transient error — they are incomplete, "
            "not empty; re-run to retry." % stats["transient"])
    if calls[0] >= MAX_PAGES:
        log("  WARNING: hit the %d API-call cap (USAJOBS_MAX_PAGES) — the "
            "facet walk was truncated." % MAX_PAGES)
    return written[0]


def main():
    email = os.environ.get("USAJOBS_EMAIL") or ""
    key = os.environ.get("USAJOBS_API_KEY") or ""
    mode = MODE
    if mode == "auto":
        mode = "keyed" if (email and key) else "keyless"
    if mode == "keyed" and not (email and key):
        raise SystemExit(
            "USAJOBS_MODE=keyed but credentials are missing.\n"
            "  Set BOTH:\n"
            "    export USAJOBS_EMAIL='you@example.com'   # sent as User-Agent\n"
            "    export USAJOBS_API_KEY='...'             # Authorization-Key\n"
            "  Request a key (free) at %s\n"
            "  Without a key, run with USAJOBS_MODE=keyless — the historicjoa\n"
            "  endpoint needs no auth and still carries the open federal "
            "announcements." % SIGNUP)
    if mode == "keyless" and not (email and key):
        log("NOTE: no USAJOBS_EMAIL/USAJOBS_API_KEY set — using the keyless "
            "historicjoa path.\n      Free key: %s (adds the live /api/search "
            "index)." % SIGNUP)

    fresh = not APPEND
    st = load_state()
    if mode == "keyless":
        start = (datetime.now(timezone.utc) - timedelta(days=DAYS)
                 ).strftime("%Y-%m-%d")
        if (st.get("mode") == "keyless" and st.get("window") == start
                and st.get("token") and not st.get("done")):
            fresh = False       # resuming: must append, not truncate
    if fresh and os.path.exists(OUT):
        log("fresh run: %s will be overwritten on the first row" % OUT)
    out = LazyWriter(OUT, append=not fresh)
    s = cc.session()
    try:
        n = run_keyed(s, out, email, key) if mode == "keyed" \
            else run_keyless(s, out)
    finally:
        out.close()
        s.close()
    log("DONE usajobs_fetch (%s) -> %s" % (mode, OUT))
    return 0 if n else 1


if __name__ == "__main__":
    sys.exit(main())
