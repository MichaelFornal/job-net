#!/usr/bin/env python3
"""
oracle_fetch.py — Oracle HCM Cloud (Fusion "Candidate Experience") job boards.

Tenant list: tenants/oracle.csv (tenant, n_rows, example_url), mined out of the
pipeline's own corpus, so every host in it is a board that really existed.
The tenant id is the LEFTMOST host label of {tenant}.fa.{region}.oraclecloud.com
and the REGION VARIES PER TENANT (10 distinct regions in the current list:
ocs 59, us2 37, us6 16, em2 7, em3 6, em4 2, us1 2, ap1/ca2/em5 1 each), so both
the host and the site id are read out of example_url — never assembled from a
guessed region.  Getting the region wrong does not 404, it 504s (see DEAD
TENANTS below), which is exactly how you lose an afternoon.

ENDPOINTS (unauthenticated, verified live 2026-07-29)
  1. GET {host}/hcmRestApi/resources/latest/recruitingCESites?onlyData=true
     Every career site configured on the tenant: SiteNumber, SiteURLName,
     SiteName (the real employer name — "Albertsons Companies", "AutoZone",
     "Warby Parker"), StatusCode ORA_ACTIVE/ORA_INACTIVE.  One request, and it
     is what makes this pipe more than a re-scrape of the corpus:
       * 132 tenants expose 317 ACTIVE sites between them (88 have one, but one
         tenant has 36) — the corpus only ever knew about the single site the
         apply URL happened to point at.
       * for 7 of 132 tenants the corpus site is NOT in the active list at all
         (eqtm's example URL says `fmolhs-careers`; the tenant's only active
         site today is CX_3001).  Those stale ids still return rows, so the
         corpus site is fetched IN ADDITION to the active list, never instead.
       * it is the only cheap source of a company name.  Without it every row
         would be attributed to an opaque four-letter pod id.

  2. GET {host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions
         ?onlyData=true
         &expand=requisitionList.secondaryLocations
         &finder=findReqs;siteNumber={site},limit=200,offset={n},
                 sortBy=POSTING_DATES_DESC
     limit is hard-capped at 200 server-side (asking for 500 or 1000 returns
     200 rows), offset pages to the end of a 10k-posting board without
     complaint.

TWO WAYS THIS ENDPOINT LIES ABOUT AN EMPTY BOARD — both are HTTP 200 with a
syntactically perfect JSON envelope, and both cost a whole run if believed:

  (a) DROP THE `expand` PARAMETER AND `requisitionList` DISAPPEARS ENTIRELY.
      Not empty — absent.  The response still carries SearchId, Facets,
      TotalJobsCount, SiteNumber and ~30 other keys, so it parses, validates,
      and looks like a live board with no open roles.  A census of all 132
      tenants run without `expand` reported 132/132 "site-empty"; the identical
      census WITH `expand` reported 132/132 live and 81,221 postings.  So the
      absence of the key is treated here as a HARD ERROR (Transient), never as
      an empty board.

  (b) AN UNRECOGNISED siteNumber RETURNS THE TENANT-WIDE TotalJobsCount WITH AN
      EMPTY requisitionList.  siteNumber=ZZZNOPE on the eofd tenant answers 200,
      TotalJobsCount 6289 — the same number the real site returns — and zero
      rows.  TotalJobsCount is therefore useless as proof the site id is right
      and useless as a paging bound; it counts the TENANT, not the site.  This
      is why sites are enumerated (1) rather than trusted from the URL, and why
      paging stops on an empty page rather than on a count.

DEAD TENANTS — *.oraclecloud.com is Akamai wildcard DNS in front of per-pod
origins, so a retired or wrong-region host RESOLVES and the edge answers
**HTTP 504** with a ~278-byte HTML body containing `errors.edgesuite.net`.
A 504 reads as "origin busy, retry me", and retrying it is free money for
nobody.  The proof it means "no such pod" rather than "pod is busy":
  eofd.fa.us6.oraclecloud.com  -> 200, 6,289 postings   (its real region)
  eofd.fa.us2.oraclecloud.com  -> 504 + edgesuite body  (same LIVE tenant,
                                                         wrong region)
  zzzqqq.fa.us2.oraclecloud.com-> 504 + edgesuite body  (invented tenant)
A live tenant cannot be "busy" only on the hostname that does not exist, so
5xx + the edgesuite/`An error occurred while processing your request` signature
is recorded DEAD, not retried.  A 5xx WITHOUT that signature is a real origin
error and stays pending.  (Every one of the 132 tenants in the current list
answered 200 on 2026-07-29, so this branch is defence, not a live filter.)

OUTPUT  contract_norm/oracle.jsonl, one cc.row() per posting.
STATE   oracle_fetch_state.json — per tenant: done / dead / err:*.  Anything
        not "done"/"dead" is retried by the next run.  A tenant is written
        "done" only after ALL of its sites finished, so a retry cannot append a
        half-board twice.

Env: ORACLE_WORKERS, ORACLE_RPS (per host), ORACLE_BUDGET_MIN, ORACLE_LIMIT.
"""
import concurrent.futures as cf
import csv
import os
import re
import sys
import threading
import time

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "oracle.csv")
OUT = os.path.join(cc.NORM_DIR, "oracle.jsonl")
STATE = os.path.join(BASE, "oracle_fetch_state.json")

WORKERS = int(os.environ.get("ORACLE_WORKERS") or 24)
# Per-HOST budget.  Every tenant is its own hostname and (behind the Akamai
# edge) its own Fusion pod, so there is no shared origin to protect the way
# apply.workable.com is one host for 20k boards — the ceiling that matters is
# per tenant.  3 rps against a pod that answers a 200-posting page in ~1.5s is
# well inside what it serves the real career site.
RPS = float(os.environ.get("ORACLE_RPS") or 3.0)
PAGE = 200              # server-side hard cap; larger values are silently 200
# THE RESULT WINDOW IS 10,000 AND IT CLOSES SILENTLY.  offset=9800 on the
# 10,150-posting AutoZone board returns a full page; offset=10000 returns
# HTTP 200 with an EMPTY requisitionList *and* TotalJobsCount 0 — the exact
# shape of "this board has no jobs".  Paging is stopped at the wall on purpose
# and the site is recorded truncated, so the loss is visible in the state file
# instead of looking like a board that simply ended.  Only 1 of the 132 tenants
# is over the wall today (by ~150 postings, 1.5% of that board).
WINDOW = 10000
MAX_PAGES = WINDOW // PAGE
TIMEOUT = 90            # a 200-row page on a busy pod is genuinely slow
TRIES = 5
BUDGET_MIN = int(os.environ.get("ORACLE_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("ORACLE_LIMIT") or 0)     # first N tenants, testing

API = "/hcmRestApi/resources/latest"
SITES_URL = (API + "/recruitingCESites?onlyData=true&limit=200")
REQS_URL = (
    API + "/recruitingCEJobRequisitions?onlyData=true"
    "&expand=requisitionList.secondaryLocations"
    "&finder=findReqs;siteNumber={site},limit={limit},offset={offset},"
    "sortBy=POSTING_DATES_DESC")

# The Akamai "no origin behind this hostname" page.  See DEAD TENANTS above.
_EDGE_DEAD = re.compile(
    rb"errors\.edgesuite\.net|An error occurred while processing your request",
    re.I)

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {}
_pause_until = {}
_interval = {}
_state = cc.load_state(STATE)
_writer = None
_counts = {"rows": 0, "live": 0, "dead": 0, "empty": 0, "err": 0, "sites": 0}
_n429 = 0
_failed_now = set()


class Transient(Exception):
    """Retryable.  A tenant that raises this stays PENDING in the state file."""


class DeadTenant(Exception):
    """Confirmed gone: the hostname has no Fusion pod behind it."""


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


def slow_down(host):
    global _n429
    with _rate_lock:
        _pause_until[host] = time.time() + 20
        _interval[host] = min(_interval.get(host, 1.0 / RPS) * 1.5, 1.0)
        _n429 += 1


def gj(s, host, path):
    """Throttled GET -> parsed JSON, or DeadTenant / Transient.

    Deliberately never returns a sentinel dict: a caller that cannot tell an
    error from an empty payload is how a rate-limited board gets recorded as a
    completed empty one.  The three outcomes are distinct on purpose:
      return   -> the pod answered and this is its real answer
      DeadTenant -> the pod does not exist (see the module docstring)
      Transient  -> we did not reach it; the tenant stays pending
    """
    url = f"https://{host}{path}"
    last = None
    for attempt in range(TRIES):
        throttle(host)
        try:
            r = s.get(url, timeout=TIMEOUT)
            code = r.status_code
            if code == 200:
                try:
                    return r.json()
                except ValueError:
                    # 200 with a non-JSON body on the REST API is a parked or
                    # login-walled pod, not a transient blip; one retry in case
                    # it is an edge-cached error page, then dead.
                    if attempt >= 1:
                        raise DeadTenant("200 non-JSON body")
                    last = "bad-json"
            elif code in (500, 502, 503, 504) and _EDGE_DEAD.search(
                    r.content or b""):
                # THE trap for this platform.  Not a busy origin — no origin.
                raise DeadTenant(f"HTTP {code} edge-no-origin")
            elif code in (404, 410):
                # The REST resource itself is missing: the pod is up but is not
                # a recruiting pod (or the module is off).  Nothing to retry.
                raise DeadTenant(f"HTTP {code}")
            elif code in (401, 403):
                # Fusion answers 403 when the CE site is login-walled.  It is a
                # property of the tenant, not of our request rate — this pipe
                # has no credentials, so retrying cannot change it.
                raise DeadTenant(f"HTTP {code} auth-walled")
            elif code in (429, 418):
                slow_down(host)
                last = f"HTTP {code}"
            else:
                last = f"HTTP {code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient(f"{url}: {last}")


def emit(rows):
    global _writer
    with _lock:
        if _writer is None:
            _writer = cc.Writer(OUT)
        for r in rows:
            _writer.write(r)
        _counts["rows"] += len(rows)


# ---- normalization ---------------------------------------------------------
_WM = {"ORA_REMOTE": "Remote", "ORA_HYBRID": "Hybrid",
       "ORA_ON_SITE": "On-site"}


def split_loc(primary, iso2):
    """'Las Vegas, NV, United States' -> ('Las Vegas', 'NV').

    Deliberately shallow: the third segment is a country NAME while the
    `country` column carries the ISO-2 from PrimaryLocationCountry, so the tail
    is dropped rather than guessed at.  A two-part string only yields a state
    when it looks like a US state code, because 'Santiago, Chile' is a
    city/country pair with the same shape.
    """
    parts = [p.strip() for p in (primary or "").split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1]
    if len(parts) == 2:
        if iso2 == "US" and re.fullmatch(r"[A-Z]{2}", parts[1]):
            return parts[0], parts[1]
        return parts[0], ""
    return (parts[0] if parts else ""), ""


def site_path(site):
    """The path segment the public career site uses for this site.

    SiteURLName when the customer configured a vanity name (63 of the 317
    active sites have one — 'BDOEntryLevelCareers', 'WarbyParkerCareers'), else
    the SiteNumber.  This is what makes apply_url identical to the URL the
    corpus already holds for the same posting, which is the whole point of
    emitting a real apply_url: the canonical-ATS dedupe has to collapse the two
    copies instead of stacking them.
    """
    return site.get("SiteURLName") or site.get("SiteNumber") or ""


def to_rows(tenant, host, site, reqs):
    seg = site_path(site)
    company = (site.get("SiteName") or "").strip()
    # 'Candidate Experience site' is Oracle's factory default site name and
    # says nothing about the employer; the pod id is at least unambiguous.
    if not company or company.lower().startswith("candidate experience site"):
        company = tenant
    base = f"https://{host}/hcmUI/CandidateExperience/en/sites/{seg}"
    out = []
    for j in reqs:
        if not isinstance(j, dict):
            continue
        jid = str(j.get("Id") or "").strip()
        title = (j.get("Title") or "").strip()
        if not jid or not title:
            continue
        iso2 = (j.get("PrimaryLocationCountry") or "").strip()
        primary = (j.get("PrimaryLocation") or "").strip()
        secondary = [x.get("Name") for x in (j.get("secondaryLocations") or [])
                     if isinstance(x, dict) and x.get("Name")]
        locstr = "; ".join([p for p in [primary] if p] + secondary)
        city, state = split_loc(primary, iso2)
        # Structured facet first.  It is populated on ~38% of postings
        # (ORA_ON_SITE 375 / ORA_HYBRID 73 / ORA_REMOTE 24 in a 1,250-row
        # sample); where it is absent the ONLY fallback used is the literal
        # word "remote" in the location string.  Titles are not consulted —
        # "Remote Sensing Engineer" is an on-site job in a lab, and a wrong
        # remote flag is worse than no remote flag.
        code = (j.get("WorkplaceTypeCode") or "").strip()
        mode = _WM.get(code, "")
        if not mode and "remote" in locstr.lower():
            mode = "Remote"
        out.append(cc.row(
            source="oracle", source_type="ats", company=company,
            title=title,
            # No employment-type field exists anywhere in the search resource:
            # JobType / ContractType / WorkerType / JobSchedule came back null
            # on 1,250 of 1,250 sampled postings.  classify_any is mandatory —
            # cc.classify would return None for the full-time majority and drop
            # the board.
            employment_type=cc.classify_any(None, title),
            work_mode=mode, location=locstr, city=city, state=state,
            country=iso2, remote=(mode == "Remote"),
            posted=(j.get("PostedDate") or "")[:10],
            apply_url=f"{base}/job/{jid}",
            job_id=jid, board=tenant, board_url=f"{base}/jobs"))
    return out


# ---- per-tenant work -------------------------------------------------------
def fetch_site(s, tenant, host, site):
    """Page one career site to exhaustion.  Returns (rows, n_postings)."""
    seg = site.get("SiteNumber") or site.get("SiteURLName")
    rows, seen = [], set()
    truncated = False
    for page in range(MAX_PAGES):
        d = gj(s, host, REQS_URL.format(site=seg, limit=PAGE,
                                        offset=page * PAGE))
        items = d.get("items") if isinstance(d, dict) else None
        if not items:
            break
        head = items[0]
        if "requisitionList" not in head:
            # See trap (a): the key is ABSENT, not empty.  Treating this as an
            # empty board is what silently zeroed a 132-tenant census.
            raise Transient("requisitionList key absent — expand= dropped?")
        reqs = head.get("requisitionList") or []
        if not reqs:
            break
        fresh = [r for r in reqs
                 if isinstance(r, dict) and str(r.get("Id")) not in seen]
        seen.update(str(r.get("Id")) for r in fresh)
        rows.extend(to_rows(tenant, host, site, fresh))
        if len(reqs) < PAGE:
            break
        if not fresh:
            # A pod that keeps handing back the same page (seen on sorted
            # finders when the sort key is not unique) would otherwise spin
            # until MAX_PAGES.
            break
    else:
        truncated = True
    return rows, len(seen), truncated


def fetch_tenant(rec):
    tenant, host, corpus_site = rec["tenant"], rec["host"], rec["site"]
    s = cc.session()
    try:
        d = gj(s, host, SITES_URL)
        items = [x for x in (d.get("items") or []) if isinstance(x, dict)]
        sites = [x for x in items if x.get("StatusCode") == "ORA_ACTIVE"]
        known = {x.get("SiteNumber") for x in sites}
        known |= {x.get("SiteURLName") for x in sites}
        if corpus_site and corpus_site not in known:
            # Stale-but-serving site from the corpus apply URL (7 of 132).
            # Carry the SiteName from the inactive record if there is one, so
            # the company name survives.
            meta = next((x for x in items
                         if corpus_site in (x.get("SiteNumber"),
                                            x.get("SiteURLName"))), None)
            sites.append({"SiteNumber": corpus_site,
                          "SiteURLName": None,
                          "SiteName": (meta or {}).get("SiteName") or ""})
        if not sites:
            sites = [{"SiteNumber": corpus_site, "SiteURLName": None,
                      "SiteName": ""}]

        all_rows, ids, cut = [], set(), []
        for site in sites:
            rows, _n, truncated = fetch_site(s, tenant, host, site)
            if truncated:
                cut.append(site.get("SiteNumber"))
            for r in rows:
                # One requisition is frequently published to several of a
                # tenant's sites; keep the first (sites are ordered active
                # first) so a 36-site tenant does not emit 36 copies.
                if r["job_id"] in ids:
                    continue
                ids.add(r["job_id"])
                all_rows.append(r)
        return all_rows, len(sites), cut
    finally:
        s.close()


def _settled(tenant):
    """True once this tenant needs no further work.  Matched by PREFIX because
    a truncated board records `done truncated@10k:CX_1` — still done, but the
    loss is written down where a human will see it."""
    v = str(_state.get(tenant) or "")
    return v.startswith("done") or v.startswith("dead")


def work(rec):
    tenant = rec["tenant"]
    if _settled(tenant):
        return
    try:
        rows, nsites, cut = fetch_tenant(rec)
    except DeadTenant as e:
        with _lock:
            _state[tenant] = f"dead:{e}"
            _counts["dead"] += 1
        return
    except Exception as e:               # noqa: BLE001 - keep the pool alive
        with _lock:
            _state[tenant] = f"err:{type(e).__name__}"
            _counts["err"] += 1
            _failed_now.add(tenant)
        return
    emit(rows)
    with _lock:
        _state[tenant] = ("done" if not cut
                          else "done truncated@10k:" + ",".join(map(str, cut)))
        _counts["sites"] += nsites
        _counts["live" if rows else "empty"] += 1
        if cut:
            _counts["truncated"] = _counts.get("truncated", 0) + 1


def load_tenants():
    out = []
    with open(TENANTS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            url = (r.get("example_url") or "").strip()
            m = re.match(r"https?://([^/]+)/(.*)$", url)
            if not m:
                continue
            host = m.group(1)
            ms = re.search(r"/sites/([^/?#]+)", m.group(2))
            out.append({"tenant": (r.get("tenant") or "").strip(),
                        "host": host,
                        "site": ms.group(1) if ms else ""})
    return out


def main():
    recs = load_tenants()
    if LIMIT:
        recs = recs[:LIMIT]
    todo = [r for r in recs if not _settled(r["tenant"])]
    print(f"oracle: {len(recs)} tenants, {len(recs) - len(todo)} already "
          f"done/dead, {len(todo)} to fetch", flush=True)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(work, r) for r in todo]
        for _ in cf.as_completed(futs):
            done += 1
            if done % 10 == 0:
                with _lock:
                    cc.save_state(STATE, _state)
                print(f"  {done}/{len(todo)} tenants  {_counts}", flush=True)
            if deadline and time.time() > deadline:
                stopped = True
                for fu in futs:
                    fu.cancel()
                break
    cc.save_state(STATE, _state)
    if _writer is not None:
        _writer.close()
    if _n429:
        print(f"  rate-limit backoffs: {_n429}", flush=True)
    print(f"DONE oracle_fetch: {_counts}  -> {OUT}", flush=True)
    if stopped:
        print(f"BUDGET REACHED: {done}/{len(todo)} tenants; the rest are "
              f"still pending and are picked up by the next run.", flush=True)
        return 0
    if _failed_now:
        print(f"{len(_failed_now)} tenants failed transiently — re-run to "
              f"retry: {sorted(_failed_now)[:10]}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
