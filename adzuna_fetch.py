#!/usr/bin/env python3
"""
adzuna_fetch.py — Adzuna job-search API (multi-country aggregator).

    GET https://api.adzuna.com/v1/api/jobs/{country}/search/{page}
        ?app_id=..&app_key=..&results_per_page=50&what=..&where=..

KEY REQUIRED.  Free, self-serve: https://developer.adzuna.com/signup
Set BOTH env vars before running:
    export ADZUNA_APP_ID='...'
    export ADZUNA_APP_KEY='...'
With no key the script prints that, probes the endpoint to prove the request
shape is right, and exits 2.  It never writes an empty jsonl — a zero-byte
file downstream is indistinguishable from "Adzuna has no jobs".

WHAT WAS VERIFIED WITHOUT A KEY (measured 2026-07-29)
============================================================================
* The machine-readable contract is public:
  https://api.adzuna.com/v1/api-docs/adzuna (Swagger 1.2, HTTP 200, no auth).
  Every parameter and field name below was read out of that document, not
  guessed: path is jobs/{country}/search/{page}; app_id + app_key are the only
  REQUIRED query params; results_per_page, what, what_and, what_phrase,
  what_or, what_exclude, title_only, where, distance, max_days_old, category,
  sort_by, sort_direction, salary_min, salary_max, salary_include_unknown,
  full_time, part_time, contract, permanent, company are optional.
  The response is Adzuna::API::Response::JobSearchResults = {count, mean,
  results[]}, and each result carries: id, adref, title, description,
  full_description, created, company{display_name}, location{display_name,
  area[]}, category{tag,label}, contract_time, contract_type, salary_min,
  salary_max, salary_is_predicted, redirect_url, latitude, longitude.
* Auth behaviour, so failures are readable rather than mysterious:
    - no app_id/app_key at all  -> HTTP 400 with an HTML page
      ("Uh oh, something isn't right"), NOT JSON.  Their edge rejects the
      request before the API sees it.
    - bogus app_id/app_key      -> HTTP 401 with
      {"exception":"AUTH_FAIL","display":"Authorisation failed",...}
      i.e. the path and parameters are well-formed and reach the auth layer;
      only the credential is missing.  That is the strongest pre-key proof
      available, and running this script with no key reproduces it (see
      probe_without_key(), which runs automatically on the no-key path).
  So: HTML body == malformed request or edge rejection; JSON with an
  "exception" key == the API answered and refused; JSON with "results" ==
  real data.  Status code alone never decides.

WHAT IS *NOT* PROVEN UNTIL THE KEY LANDS
============================================================================
* The reachable page depth.  `count` is a total, not a promise (the same trap
  measured on other sources in this batch).  The walk therefore stops on:
  count exhausted, a short page, a page that repeats the previous page's ids
  (the classic ignored-page-param failure), or zero new ids — whichever comes
  first, and it says which one it was.
* The free-tier call quota.  Both of Adzuna's published limit pages are dead,
  so ADZUNA_MAX_CALLS (default 200) is a deliberately conservative budget,
  persisted per-day in adzuna_fetch_state.json so repeated runs on one day
  cannot silently blow through it.
* Whether results_per_page is honoured above 50.  50 is the documented-safe
  value and what this uses; the walk reads the ACTUAL length of results[]
  rather than assuming it got what it asked for.

TWO KNOWN, UNFIXABLE-BY-DESIGN WEAKNESSES — READ BEFORE INTEGRATING
============================================================================
1. apply_url is an Adzuna redirect (redirect_url), not the employer's ATS
   link.  Adzuna exposes no employer URL at all, and their T&Cs require the
   redirect.  That means these rows CANNOT be recognised as duplicates of the
   same job already held from Greenhouse/Lever/Getro, so Adzuna will inflate
   the master with re-labelled copies of jobs already in it.
   ADZUNA_RESOLVE=1 turns on an opt-in pass that follows each redirect and
   keeps the final URL when it lands on a recognised ATS host (greenhouse,
   lever, ashby, workday, smartrecruiters, ...), which restores the canonical
   dedupe key.  It costs one extra request per row and is UNVERIFIED — no key
   was available to obtain a real redirect_url to test it against.
2. There is NO remote field anywhere in the Adzuna schema (confirmed against
   the swagger above).  work_mode/remote here are INFERRED from title and
   location text only, which is why the inference is deliberately narrow —
   a bare "remote" appearing in a description is not enough.

ENV
  ADZUNA_APP_ID / ADZUNA_APP_KEY   credentials (required)
  ADZUNA_COUNTRIES   comma list, default "us"   (gb us at au be br ca ch de es
                     fr in it mx nl nz pl sg za)
  ADZUNA_DAYS        max_days_old, default 30
  ADZUNA_MAX_CALLS   API call budget per day, default 200
  ADZUNA_PER_PAGE    results_per_page, default 50
  ADZUNA_MAX_PAGES   page cap per partition, default 40
  ADZUNA_WHERE       comma list of place names to cross with the category
                     axis, e.g. "Boston,New York,San Francisco" — use this
                     when a run reports "hit the page cap"
  ADZUNA_RESOLVE=1   resolve redirect_url -> employer ATS url (opt-in, slow)
  ADZUNA_RESOLVE_MAX rows to resolve per run, default 500
  ADZUNA_APPEND=1    append instead of starting a fresh file
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
SOURCE = "adzuna"
SOURCE_TYPE = "aggregator"
OUT = os.path.join(cc.NORM_DIR, "adzuna.jsonl")
STATE_PATH = os.path.join(BASE, "adzuna_fetch_state.json")

API = "https://api.adzuna.com/v1/api/jobs/%s/search/%d"
CATS = "https://api.adzuna.com/v1/api/jobs/%s/categories"
SIGNUP = "https://developer.adzuna.com/signup"
SWAGGER = "https://api.adzuna.com/v1/api-docs/adzuna"

COUNTRIES = [c.strip().lower() for c in
             (os.environ.get("ADZUNA_COUNTRIES") or "us").split(",")
             if c.strip()]
DAYS = int(os.environ.get("ADZUNA_DAYS") or 30)
MAX_CALLS = int(os.environ.get("ADZUNA_MAX_CALLS") or 200)
PER_PAGE = int(os.environ.get("ADZUNA_PER_PAGE") or 50)
MAX_PAGES = int(os.environ.get("ADZUNA_MAX_PAGES") or 40)
WHERE = [w.strip() for w in (os.environ.get("ADZUNA_WHERE") or "").split(",")
         if w.strip()]
RESOLVE = os.environ.get("ADZUNA_RESOLVE") == "1"
RESOLVE_MAX = int(os.environ.get("ADZUNA_RESOLVE_MAX") or 500)
# Append by DEFAULT — see the usajobs_fetch.py note; same LazyWriter shape.
# This one had an extra hole: `fresh = not APPEND and not done`, and the state
# resets whenever `st["date"] != TODAY`, so `done` is empty on the first run of
# every calendar day and the file was truncated afresh each morning.
# ADZUNA_FRESH=1 for a deliberate clean rebuild; ADZUNA_APPEND kept as a no-op.
APPEND = os.environ.get("ADZUNA_FRESH") != "1"
SLEEP = float(os.environ.get("ADZUNA_SLEEP") or 1.0)
TIMEOUT = 40
TRIES = 4
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

VALID_COUNTRIES = {"gb", "us", "at", "au", "be", "br", "ca", "ch", "de", "es",
                   "fr", "in", "it", "mx", "nl", "nz", "pl", "sg", "za"}
TLD = {"us": "com", "gb": "co.uk", "au": "com.au", "ca": "ca", "in": "in",
       "nz": "co.nz", "za": "co.za", "sg": "sg", "br": "com.br",
       "de": "de", "fr": "fr", "es": "es", "it": "it", "nl": "nl",
       "pl": "pl", "at": "at", "be": "be", "ch": "ch", "mx": "com.mx"}
CURRENCY = {"us": "USD", "gb": "GBP", "ca": "CAD", "au": "AUD", "nz": "NZD",
            "in": "INR", "za": "ZAR", "sg": "SGD", "br": "BRL", "mx": "MXN",
            "ch": "CHF", "pl": "PLN", "de": "EUR", "fr": "EUR", "es": "EUR",
            "it": "EUR", "nl": "EUR", "at": "EUR", "be": "EUR"}

# Fallback partition axis when the categories endpoint cannot be read.
# These are the Adzuna category tags for the jobs vertical.
FALLBACK_CATEGORIES = [
    "it-jobs", "engineering-jobs", "accounting-finance-jobs", "sales-jobs",
    "healthcare-nursing-jobs", "teaching-jobs", "trade-construction-jobs",
    "hospitality-catering-jobs", "pr-advertising-marketing-jobs",
    "logistics-warehouse-jobs", "admin-jobs", "customer-services-jobs",
    "hr-jobs", "legal-jobs", "creative-design-jobs", "retail-jobs",
    "consultancy-jobs", "manufacturing-jobs", "scientific-qa-jobs",
    "social-work-jobs", "energy-oil-gas-jobs", "property-jobs",
    "charity-voluntary-jobs", "maintenance-jobs", "travel-jobs",
    "domestic-help-cleaning-jobs", "part-time-jobs", "graduate-jobs",
    "other-general-jobs"]

# Narrow on purpose — see weakness (2) in the docstring.  Matched against the
# TITLE and the LOCATION only, never the free-text description, because
# "must be able to work at a remote site" is not a remote job.
RX_REMOTE = re.compile(
    r"\bremote\b|\bwork from home\b|\bwfh\b|\btelecommut|\bfully[- ]remote\b|"
    r"\bhome[- ]based\b|\banywhere\b", re.I)
RX_REMOTE_FP = re.compile(
    r"remote site|remote location|remote area|remote camp|remote support|"
    r"remote sensing|remote monitoring|remote desktop|remote hands", re.I)
RX_HYBRID = re.compile(r"\bhybrid\b", re.I)

# Hosts that carry a canonical ATS job key the pipeline can dedupe on.
ATS_HOSTS = re.compile(
    r"(^|\.)(greenhouse\.io|boards\.greenhouse\.io|job-boards\.greenhouse\.io|"
    r"lever\.co|jobs\.lever\.co|ashbyhq\.com|jobs\.ashbyhq\.com|"
    r"myworkdayjobs\.com|wd\d+\.myworkdayjobs\.com|smartrecruiters\.com|"
    r"recruitee\.com|breezy\.hr|bamboohr\.com|workable\.com|jobvite\.com|"
    r"icims\.com|taleo\.net|successfactors\.com|oraclecloud\.com|"
    r"personio\.de|join\.com|teamtailor\.com|pinpointhq\.com|rippling\.com|"
    r"applytojob\.com|jazz\.co|jazzhr\.com|paylocity\.com|dayforcehcm\.com)$",
    re.I)


def log(msg):
    print(msg, flush=True)


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
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


class LazyWriter:
    """Truncates the output only when the first real row arrives, so a run
    that dies before any data leaves the previous file intact instead of
    publishing a zero-byte 'Adzuna has no jobs'."""

    def __init__(self, path, append=False):
        self.path, self.mode, self.f, self.n = path, ("a" if append else "w"), \
            None, 0

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


class Fatal(Exception):
    """Credentials / quota — stop the run, do not mark partitions done."""


class Transient(Exception):
    """Host hiccup — NOT 'this partition has no jobs'."""


def api_get(s, url, params, budget):
    """One API call, validating the BODY.

    Returns a dict with 'results'.  Raises Fatal on auth/quota, Transient on
    anything that looks like a hiccup.  Adzuna's failure modes:
      HTML body            -> edge rejection (usually a missing credential)
      JSON with 'exception'-> the API refused; AUTH_FAIL / quota / bad param
      JSON with 'results'  -> data
    """
    if budget["calls"] >= MAX_CALLS:
        raise Fatal("call budget exhausted (%d) — raise ADZUNA_MAX_CALLS to "
                    "continue; state saved" % MAX_CALLS)
    last = ""
    for attempt in range(TRIES):
        budget["calls"] += 1
        try:
            r = s.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        body = (r.text or "").lstrip()
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After") or 30)
            log("    429 — sleeping %ds" % wait)
            time.sleep(wait)
            last = "HTTP 429"
            continue
        if not body or body[0] == "<":
            raise Fatal(
                "Adzuna answered HTTP %d with an HTML page, not JSON — that is "
                "their edge rejecting the request, almost always a missing or "
                "malformed app_id/app_key.\n  Check ADZUNA_APP_ID / "
                "ADZUNA_APP_KEY (%s)" % (r.status_code, SIGNUP))
        try:
            d = r.json()
        except ValueError:
            last = "non-json body (HTTP %d)" % r.status_code
            time.sleep(2 * (attempt + 1))
            continue
        if isinstance(d, dict) and d.get("exception"):
            exc = str(d.get("exception"))
            msg = "%s: %s" % (exc, d.get("display"))
            if "AUTH" in exc.upper() or r.status_code in (401, 403):
                raise Fatal("Adzuna refused the credentials — %s\n  Get a free "
                            "key at %s" % (msg, SIGNUP))
            if "LIMIT" in exc.upper() or "QUOTA" in exc.upper() \
                    or r.status_code == 429:
                raise Fatal("Adzuna quota hit — %s" % msg)
            raise Transient(msg)
        if not isinstance(d, dict) or "results" not in d:
            last = "unexpected shape: %s" % (list(d)[:6]
                                             if isinstance(d, dict) else type(d))
            time.sleep(2 * (attempt + 1))
            continue
        return d
    raise Transient(last or "unknown")


# ------------------------------------------------------------------ rows
def _area(loc, country):
    """location.area is coarse-to-fine: ["US","California","San Francisco
    County","San Francisco"].  area[0] is the country, area[1] the state/
    region, and the LAST element is the finest locality — which is the state
    itself on a state-level ad, hence the equality guard."""
    area = [a for a in (loc.get("area") or []) if a]
    disp = loc.get("display_name") or "; ".join(area)
    ctry = area[0] if area else country.upper()
    state = area[1] if len(area) > 1 else ""
    city = area[-1] if len(area) > 1 else ""
    if city == state:
        city = ""
    return disp, city, state, ctry


def build_row(j, country, apply_url=None):
    loc = j.get("location") or {}
    disp, city, state, ctry = _area(loc, country)
    title = j.get("title") or ""
    blob = "%s %s" % (title, disp)
    is_remote = bool(RX_REMOTE.search(blob)) and not RX_REMOTE_FP.search(blob)
    mode = "Remote" if is_remote else ("Hybrid" if RX_HYBRID.search(blob)
                                       else "")
    # salary_is_predicted means Adzuna's model INVENTED the number.  Emitting
    # it as if the employer published it would be a fabrication, so it is
    # dropped rather than passed through.
    predicted = str(j.get("salary_is_predicted") or "0") in ("1", "True",
                                                             "true")
    smin = "" if predicted else num(j.get("salary_min"))
    smax = "" if predicted else num(j.get("salary_max"))
    ctype = j.get("contract_type") or ""
    ctime = j.get("contract_time") or ""
    return cc.row(
        source=SOURCE, source_type=SOURCE_TYPE,
        company=(j.get("company") or {}).get("display_name") or "",
        title=title,
        employment_type=cc.classify_any(
            ("%s %s" % (ctype, ctime)).replace("_", " ").strip(), title),
        work_mode=mode, remote=is_remote,
        location=disp, city=city, state=state, country=ctry,
        salary_min=smin, salary_max=smax,
        salary_currency=CURRENCY.get(country, "") if (smin or smax) else "",
        # Adzuna normalises every advertised salary to an ANNUAL figure in the
        # country's local currency (there is no rate-interval field in the
        # schema), so the period is always Year when a figure survives.
        salary_period="Year" if (smin or smax) else "",
        posted=cc.ymd(iso=j.get("created")),
        apply_url=apply_url or j.get("redirect_url") or "",
        job_id=str(j.get("id") or j.get("adref") or ""),
        board="adzuna",
        board_url="https://www.adzuna.%s/" % TLD.get(country, "com"))


def resolve_apply(s, url, cache):
    """Follow an Adzuna /land/ad/ redirect to the employer's own posting.

    UNVERIFIED: no key was available to obtain a real redirect_url, so this
    has never seen a live Adzuna redirect.  It is opt-in for that reason.
    Only a final URL on a recognised ATS host is kept — anything else (the
    advertiser's own careers page, another aggregator, a 403 wall) leaves the
    Adzuna URL in place, which is at least honest about where the row came
    from.
    """
    if not url or url in cache:
        return cache.get(url, url)
    final = url
    try:
        # stream=True so only the headers are read — the destination URL is
        # all we want and some advertiser pages are megabytes.
        r = s.get(url, timeout=TIMEOUT, allow_redirects=True, stream=True)
        final = r.url or url
        r.close()
    except requests.RequestException:
        cache[url] = url
        return url
    host = (urlsplit(final).hostname or "").lower()
    keep = final if ATS_HOSTS.search(host) else url
    cache[url] = keep
    return keep


# ------------------------------------------------------------------ walk
def categories(s, country, creds, budget):
    try:
        d = api_get(s, CATS % country, dict(creds), budget)
    except (Fatal, Transient) as e:
        if isinstance(e, Fatal):
            raise
        log("  categories endpoint unavailable (%s) — using the static list"
            % e)
        return list(FALLBACK_CATEGORIES)
    tags = [c.get("tag") for c in (d.get("results") or []) if c.get("tag")]
    return tags or list(FALLBACK_CATEGORIES)


def walk_partition(s, country, params, creds, budget, out, seen, stats):
    """Page one (country, category) partition until it is genuinely done."""
    q = dict(creds)
    q.update(params)
    q["results_per_page"] = PER_PAGE
    if DAYS:
        q["max_days_old"] = DAYS
    q["sort_by"] = "date"
    prev_ids = None
    total = None
    page = 1
    wrote = 0
    while page <= MAX_PAGES:
        try:
            d = api_get(s, API % (country, page), q, budget)
        except Transient as e:
            if page == 1 and "sort_by" in q:
                # sort_by=date is documented but its accepted values are not
                # enumerated in the swagger; if it is rejected, drop it once
                # rather than losing the whole partition.
                log("    retrying without sort_by (%s)" % e)
                q.pop("sort_by")
                continue
            stats["transient"] += 1
            log("    ! %s p%d transient: %s (NOT recorded as empty)"
                % (params.get("category", country), page, e))
            return wrote, False
        if total is None:
            total = int(d.get("count") or 0)
        res = d.get("results") or []
        ids = [str(r.get("id") or "") for r in res]
        if not res:
            return wrote, True                       # genuinely exhausted
        if prev_ids is not None and ids == prev_ids:
            # The measured anti-pattern: a page param that is ignored, so the
            # same page comes back forever and "collects" thousands of rows
            # that dedupe to one page.
            log("    WARNING: page %d is identical to page %d — the page "
                "parameter is being ignored; stopping this partition"
                % (page, page - 1))
            stats["page_ignored"] += 1
            return wrote, True
        prev_ids = ids
        fresh = 0
        for j in res:
            jid = str(j.get("id") or "")
            url = j.get("redirect_url") or ""
            # Dedupe on the id AND on the apply_url: one ad legitimately
            # appears under several category partitions, and apply_url is the
            # key the pipeline dedupes on downstream, so two rows sharing one
            # apply_url would look like the re-collected-page failure.
            key, ukey = (country, jid), ("u", url)
            if not jid or key in seen or (url and ukey in seen):
                continue
            seen.add(key)
            if url:
                seen.add(ukey)
            fresh += 1
            out.write(build_row(j, country))
            wrote += 1
        if fresh == 0:
            log("    page %d added 0 new ids — stopping partition" % page)
            return wrote, True
        if len(res) < PER_PAGE:
            return wrote, True                       # short page == last page
        if total and page * PER_PAGE >= total:
            return wrote, True
        page += 1
        time.sleep(SLEEP)
    log("    hit the %d-page cap with count=%s — partition may be truncated "
        "(raise ADZUNA_MAX_PAGES or narrow with 'where')" % (MAX_PAGES, total))
    stats["capped"] += 1
    return wrote, True


def probe_without_key(s):
    """Prove the request shape is right even with no credentials.

    Sends a deliberately bogus app_id/app_key: a well-formed request reaches
    Adzuna's auth layer and comes back as JSON {"exception":"AUTH_FAIL"}.
    Anything else (HTML) would mean the URL itself is wrong.
    """
    try:
        r = s.get(API % ("us", 1),
                  params={"app_id": "probe", "app_key": "probe",
                          "results_per_page": 1}, timeout=TIMEOUT)
    except requests.RequestException as e:
        log("  probe: could not reach api.adzuna.com (%s)" % type(e).__name__)
        return
    body = (r.text or "").strip()
    try:
        d = r.json()
    except ValueError:
        log("  probe: HTTP %d, non-JSON body — %s" % (r.status_code,
                                                      body[:120]))
        return
    log("  probe: HTTP %d %s — request shape is VALID, only the credential is "
        "missing." % (r.status_code, json.dumps(d)[:120]))
    log("  schema reference (public, no key): %s" % SWAGGER)


def main():
    app_id = os.environ.get("ADZUNA_APP_ID") or ""
    app_key = os.environ.get("ADZUNA_APP_KEY") or ""
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json"})

    if not (app_id and app_key):
        log("Adzuna needs a free API key and none is set.")
        log("  export ADZUNA_APP_ID='...'")
        log("  export ADZUNA_APP_KEY='...'")
        log("  Sign up (free, self-serve): %s" % SIGNUP)
        log("  Then re-run:  python3 adzuna_fetch.py")
        log("Checking the endpoint anyway so the shape is proven now:")
        probe_without_key(s)
        s.close()
        log("No rows written (deliberately — an empty %s would read as "
            "'Adzuna has no jobs')." % OUT)
        return 2

    bad = [c for c in COUNTRIES if c not in VALID_COUNTRIES]
    if bad:
        raise SystemExit("unknown ADZUNA_COUNTRIES %s (valid: %s)"
                         % (bad, " ".join(sorted(VALID_COUNTRIES))))

    creds = {"app_id": app_id, "app_key": app_key}
    st = load_state()
    if st.get("date") != TODAY:
        st = {"date": TODAY, "calls": 0, "done": []}
    budget = {"calls": st.get("calls", 0)}
    done = set(tuple(x) for x in st.get("done", []))
    fresh = not APPEND and not done
    out = LazyWriter(OUT, append=not fresh)
    seen = set()
    stats = {"transient": 0, "capped": 0, "page_ignored": 0}
    total_rows = 0
    t0 = time.time()
    stopped = ""

    try:
        for country in COUNTRIES:
            cats = categories(s, country, creds, budget)
            log("%s: %d categories, budget %d/%d calls used"
                % (country, len(cats), budget["calls"], MAX_CALLS))
            # Partition axis is category, optionally crossed with ADZUNA_WHERE
            # (a comma list of place names).  The geo cross is the escape
            # hatch for the unmeasurable page-depth ceiling: when a bare
            # category run reports "hit the page cap", re-run with
            # ADZUNA_WHERE set to reach deeper inventory instead of pretending
            # `count` was reachable.
            for cat in cats:
                for where in (WHERE or [""]):
                    part = (country, cat, where)
                    if part in done:
                        continue
                    params = {"category": cat}
                    if where:
                        params["where"] = where
                    n, complete = walk_partition(
                        s, country, params, creds, budget, out, seen, stats)
                    total_rows += n
                    if complete:
                        done.add(part)
                    save_state({"date": TODAY, "calls": budget["calls"],
                                "done": [list(x) for x in done]})
                    log("  %-24s %-14s %5d rows (%d calls used)"
                        % (cat, where or "-", n, budget["calls"]))
    except Fatal as e:
        stopped = str(e)
    finally:
        st = {"date": TODAY, "calls": budget["calls"],
              "done": [list(x) for x in done]}
        save_state(st)

    # Close BEFORE the resolve pass — it re-reads the file, and buffered rows
    # that are still in the writer would be silently missing from the rewrite.
    out.close()
    if RESOLVE and total_rows:
        log("resolving redirect_url -> employer ATS (opt-in, unverified) ...")
        rewrite_apply_urls(s)
    s.close()
    log("adzuna: %d rows, %d unique ids, %d API calls, %.0fs"
        % (total_rows, sum(1 for k in seen if k[0] != "u"),
           budget["calls"], time.time() - t0))
    if stats["transient"]:
        log("  %d partitions ended on a TRANSIENT error — they are NOT marked "
            "done and a re-run retries them." % stats["transient"])
    if stats["capped"]:
        log("  %d partitions hit the page cap (possible truncation)."
            % stats["capped"])
    if stats["page_ignored"]:
        log("  %d partitions stopped because the page parameter looked "
            "ignored." % stats["page_ignored"])
    if stopped:
        log("STOPPED: %s" % stopped)
    log("DONE adzuna_fetch -> %s" % OUT)
    return 0 if total_rows and not stopped else 1


def rewrite_apply_urls(s):
    """Second pass over the written file, swapping Adzuna redirects for the
    employer ATS url where one can be resolved.  Opt-in; see docstring."""
    if not os.path.exists(OUT):
        return
    cache, n, hit = {}, 0, 0
    tmp = OUT + ".resolved"
    with open(OUT, encoding="utf-8") as f, \
            open(tmp, "w", encoding="utf-8") as g:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if n < RESOLVE_MAX and r.get("apply_url"):
                new = resolve_apply(s, r["apply_url"], cache)
                n += 1
                if new != r["apply_url"]:
                    r["apply_url"] = new
                    hit += 1
                time.sleep(0.3)
            g.write(json.dumps(r) + "\n")
    os.replace(tmp, OUT)
    log("  resolved %d redirects, %d landed on a known ATS host" % (n, hit))


if __name__ == "__main__":
    sys.exit(main())
