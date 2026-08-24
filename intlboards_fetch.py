#!/usr/bin/env python3
"""
intlboards_fetch.py — six international job boards, one shared jsonl
(contract_norm/intlboards.jsonl).  All unauthenticated, all verified with live
requests 2026-07-28/29.

  torre        marketplace   search.torre.co/opportunities/_search/   LatAm+global
  nofluffjobs  job_board     nofluffjobs.com/api/search/posting       CEE / Poland
  instahyre    job_board     instahyre.com/api/v1/job_search/         India
  getonbrd     job_board     getonbrd.com/api/v0/categories/*/jobs    LatAm
  thehub       job_board     thehub.io/api/jobs                       Nordics
  landingjobs  job_board     landing.jobs/api/v1/jobs                 EU / Portugal

------------------------------------------------------------------------------
PAGINATOR TRAPS measured on these hosts — every one of them answers HTTP 200
while quietly ignoring the parameter you asked for, so each walk below is built
on the ONE parameter that was proven to advance (page 2 ids disjoint from
page 1).  Do not "simplify" these back to the obvious offset/limit form.

  torre        `offset` IS SILENTLY IGNORED.  offset=0/20/1000/100000/280000 all
               return the byte-identical first page (verified: 20/20 id overlap
               at every offset).  A fetcher trusting it would "collect" 275k
               rows that dedupe to 20.  The real paginator is the opaque cursor
               `pagination.next` replayed as the QUERY param `?after=`.  Only
               `after` works — cursor/next/paginationId as query params, and
               after/cursor/next/pagination/searchAfter in the BODY, all return
               page 1 again.  `size` caps at 39; 40+ answers HTTP 400
               "Request size ... too large".
  nofluffjobs  needs `salaryCurrency` AND `salaryPeriod` or it 400s.  `region`
               is a NO-OP (pl/cz/de/uk/us/all/absent all report the same
               totalCount 18792).  `limit` resizes the page but does NOT cap it
               (limit=1000 returns ~6,700 rows); `page` advances cleanly.
  instahyre    `limit` is CLAMPED to 35 no matter what you send (limit=100 ->
               35 rows, meta echoes limit:35).  `offset` genuinely advances.
               `location=` / `remote=` filters are NO-OPS (total_count is
               unchanged by them).
  getonbrd     `total_pages` is per_page-relative, so the 247 figure recorded in
               the repo — measured at per_page=1 — was meaningless.  Re-measured
               at per_page=100 the programming category is 4 pages / ~340 jobs.
               per_page caps at 120.
  thehub       `limit` IS IGNORED (limit=100 still returns 15 docs).  `page`
               advances.  77 pages of 15 for 1,143 jobs.
  landingjobs  `page` IS SILENTLY IGNORED (page=1/2/3 all return the same 50
               rows, same first id).  `offset` is the only thing that moves.

REMOTE COVERAGE (this is what decides each source's worth for a US-remote
hunter; measured on the same dates):
  torre        70,733 of 275,142 (25.7%) via a real filter; 88% of those are
               place.locationType == "remote_anywhere" (no geo restriction at
               all) and 55% carry externalApplicationUrl pointing at the
               employer's own ATS.  BY FAR the most valuable of the six.
  nofluffjobs  ~19% fullyRemote, but remote-from-Poland — CEE contracts.
  instahyre    ~8.6% "Work From Home", India-only, no structured remote field.
  getonbrd     ~41% remote, but remote_modality is mostly "remote_local"
               (remote *within* Chile/Peru/Colombia), not US-eligible.
  thehub       45 of 1,143 (3.9%) isRemote.  Nordic.
  landingjobs  ~24% of a 65-job corpus.  EU.

ENV
  INTL_ONLY=torre,thehub     run only these sources
  TORRE_ALL=1                crawl all 275k opportunities, not just the remote
                             partition (~1.9h instead of ~21min)
  TORRE_MAX_ROWS=N           hard row cap for torre (default 80000)
  INTL_STATE_RESET=1         forget the torre resume cursor and start over
"""
import json
import os
import re
import time
from urllib.parse import urlparse

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
# INTL_OUT lets a long source (torre) run in its own process against its own
# file; two processes appending to one jsonl can interleave a flush mid-line.
# The orchestrator just cats the parts together.
OUT = os.environ.get("INTL_OUT") or os.path.join(cc.NORM_DIR,
                                                 "intlboards.jsonl")
STATE = os.path.join(cc.NORM_DIR, ".intlboards_state.json")

TORRE_ALL = os.environ.get("TORRE_ALL") == "1"
TORRE_MAX_ROWS = int(os.environ.get("TORRE_MAX_ROWS") or 80000)
RETRY_AFTER_CAP = int(os.environ.get("RETRY_AFTER_CAP") or 900)


# ---------------------------------------------------------------- http helpers
def _sleep_429(r, attempt):
    """Honour Retry-After when present, else exponential-ish backoff.

    The cap has to be generous: instahyre answers 429 with Retry-After ~724s
    (a ~12 minute cooldown, applied to the whole API including offset=0 once
    you trip it).  An earlier 120s cap meant the walk "retried" four times
    inside one cooldown, exhausted its attempts and quit at 60% of the board.
    """
    wait = 5 * (attempt + 1)
    ra = (r.headers or {}).get("Retry-After") if r is not None else None
    if ra:
        try:
            wait = max(wait, min(RETRY_AFTER_CAP, int(float(ra)) + 5))
        except (TypeError, ValueError):
            pass
    print(f"    . sleeping {wait}s on 429", flush=True)
    time.sleep(wait)


def req(s, method, url, tries=4, timeout=45, **kw):
    """One request -> parsed JSON, or None.  A transport/HTTP failure is LOUD.

    Returning {} on failure the way cc.get_json does makes a 429 or a DNS blip
    indistinguishable from "this board is empty today", which is exactly how a
    healthy source gets recorded as 0 rows.  None means "ask again / stop",
    never "there is no more data".
    """
    r = None
    for i in range(tries):
        try:
            r = s.request(method, url, timeout=timeout, **kw)
            if r.status_code == 200:
                # RULE 2: never trust the status code.  Several of these hosts
                # answer 200 with an HTML shell for any unrecognised path.
                ct = (r.headers.get("content-type") or "").lower()
                if "json" not in ct:
                    head = r.text[:80].lstrip().lower()
                    if head.startswith("<!doctype") or head.startswith("<html"):
                        print(f"    ! {url} -> HTML shell, not JSON", flush=True)
                        return None
                try:
                    return r.json()
                except ValueError:
                    print(f"    ! {url} -> 200 but body is not JSON", flush=True)
                    return None
            if r.status_code == 429:
                print(f"    . 429 on {url}, backing off", flush=True)
                _sleep_429(r, i)
                continue
            if r.status_code in (400, 401, 403, 404):
                print(f"    ! {url} -> HTTP {r.status_code} "
                      f"{r.text[:120]}", flush=True)
                return None
            # Anything else (5xx, 503 from a throttler that forgot to say 429)
            # is retried — but say so, or a source dies in total silence.
            print(f"    . HTTP {r.status_code} on {url}, retry {i + 1}"
                  f"/{tries}", flush=True)
        except requests.RequestException as e:
            print(f"    . {type(e).__name__} on {url}", flush=True)
        time.sleep(1.5 * (i + 1))
    print(f"    ! {url} -> gave up after {tries} tries", flush=True)
    return None


def gj(s, url, **kw):
    return req(s, "GET", url, **kw)


def pj(s, url, **kw):
    return req(s, "POST", url, **kw)


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
    except (TypeError, ValueError):
        return ""


def per(p):
    """Normalize a periodicity token to the pipeline's salary_period."""
    p = (p or "").lower()
    for k, v in (("hour", "Hour"), ("day", "Day"), ("week", "Week"),
                 ("month", "Month"), ("year", "Year"), ("annual", "Year")):
        if k in p:
            return v
    return ""


# ============================================================= 1. TORRE =======
TORRE_URL = "https://search.torre.co/opportunities/_search/"
TORRE_SIZE = 39           # 40+ -> HTTP 400 "Request size ... too large"
TORRE_REMOTE_BODY = {"and": [{"remote": {"term": True}}]}


def torre_row(x):
    orgs = x.get("organizations") or []
    org = orgs[0] if orgs else {}
    place = x.get("place") or {}
    ltype = place.get("locationType") or ""
    is_remote = bool(x.get("remote") or place.get("remote"))

    # Structured work_mode straight off locationType — no keyword guessing.
    if ltype.startswith("remote") or is_remote:
        wm = "Remote"
    elif "hybrid" in ltype:
        wm = "Hybrid"
    elif ltype:
        wm = "On-site"
    else:
        wm = ""

    locs = [str(v) for v in (x.get("locations") or []) if v]
    if locs:
        location = "; ".join(locs)
    elif ltype == "remote_anywhere":
        location = "Remote (anywhere)"
    elif ltype == "remote_timezones":
        tz = x.get("timezones") or []
        location = "Remote (" + ("; ".join(str(t) for t in tz) or
                                 "timezone restricted") + ")"
    else:
        location = "Remote" if is_remote else ""

    comp = ((x.get("compensation") or {}).get("data")) or {}

    # type: full-time-employment | flexible-jobs | internships
    # opportunity: employee | freelance | ...   commitment: full-time|part-time
    raw = " ".join(str(x.get(k) or "").replace("-", " ")
                   for k in ("opportunity", "type", "commitment"))
    # "flexible-jobs" alone is not a contingent signal; strip the -jobs suffix
    # noise but keep freelance/contract words the API does use.
    et = cc.classify_any(raw, x.get("objective"))

    jid = x.get("id") or ""
    slug = x.get("slug") or ""
    torre_page = f"https://torre.ai/post/{jid}" + (f"-{slug}" if slug else "")
    # RULE: prefer the employer's own ATS link over the aggregator's page.
    apply_url = x.get("externalApplicationUrl") or torre_page

    return cc.row(
        source="torre", source_type="marketplace",
        company=org.get("name") or "", title=x.get("objective"),
        employment_type=et, work_mode=wm, remote=is_remote,
        location=location,
        country="; ".join(locs) if locs else "",
        salary_min=num(comp.get("minAmount")),
        salary_max=num(comp.get("maxAmount")),
        salary_currency=(comp.get("currency") or "").upper(),
        salary_period=per(comp.get("periodicity")),
        posted=cc.ymd(iso=x.get("created")),
        apply_url=apply_url, job_id=jid, board="torre",
        board_url="https://torre.ai/jobs")


def do_torre(s, out, st):
    body = {} if TORRE_ALL else TORRE_REMOTE_BODY
    key = "torre_all" if TORRE_ALL else "torre_remote"
    ck = st.get(key) or {}
    cur = ck.get("cursor")
    n = int(ck.get("rows") or 0)
    if cur:
        print(f"  torre: resuming from saved cursor at {n} rows", flush=True)

    seen = set()
    total = None
    empties = 0
    t0 = time.time()
    while n < TORRE_MAX_ROWS:
        params = {"size": TORRE_SIZE}
        if cur:
            params["after"] = cur       # the ONLY paginator that advances
        d = pj(s, TORRE_URL, params=params, json=body)
        if d is None:
            empties += 1
            if empties >= 4:
                print("  torre: giving up after 4 failed pages", flush=True)
                break
            time.sleep(10 * empties)
            continue                    # SAME cursor, deliberately
        empties = 0
        if total is None:
            total = d.get("total")
            print(f"  torre: total={total} "
                  f"({'ALL' if TORRE_ALL else 'remote partition'})", flush=True)
        res = d.get("results") or []
        if not res:
            break
        fresh = 0
        for x in res:
            jid = x.get("id")
            if not jid or jid in seen:
                continue
            seen.add(jid)
            out.write(torre_row(x))
            n += 1
            fresh += 1
        nxt = (d.get("pagination") or {}).get("next")
        if not nxt or nxt == cur:
            print("  torre: cursor exhausted", flush=True)
            cur = None
            break
        cur = nxt
        st[key] = {"cursor": cur, "rows": n}
        if n % 3900 < TORRE_SIZE:
            cc.save_state(STATE, st)
            rate = n / max(1e-9, time.time() - t0)
            print(f"    torre {n}/{total} rows  {rate:.0f}/s", flush=True)
        if not fresh:
            # cursor advanced but every id repeated -> stop rather than spin
            print("  torre: page produced no new ids, stopping", flush=True)
            break
        time.sleep(0.25)
    st[key] = {"cursor": cur, "rows": n}
    cc.save_state(STATE, st)
    done = "complete" if not cur else f"resumable (cursor saved)"
    print(f"  torre: {n} rows of total {total} — {done}", flush=True)


# ======================================================== 2. NOFLUFFJOBS ======
NFJ_URL = "https://nofluffjobs.com/api/search/posting"
# salaryCurrency + salaryPeriod are REQUIRED (400 without).  region is a no-op.
NFJ_PARAMS = {"salaryCurrency": "PLN", "salaryPeriod": "month", "limit": 1000}
# salary.type is the real employment signal on this board: b2b = a Polish B2B
# contractor engagement, uop/permanent = employment contract, uz = mandate.
NFJ_TYPE = {
    "b2b": "contract b2b", "uz": "temporary contract",
    "uop": "full-time permanent", "permanent": "full-time permanent",
    "internship": "internship", "mandate": "temporary contract",
}


def nfj_remote_ids(s):
    """Authoritative fully-remote id set, via the board's own remote filter.

    DO NOT use `location.fullyRemote` (or a places[] entry whose city is
    "Remote") as the remote flag.  Measured 2026-07-29: that field is true for
    5,901 of 6,696 rows on page 1 — 88% of the board — because it means "remote
    work is an OPTION for this role", not "this role is remote".  The board's
    own filter (rawSearch="remote", equivalently criteriaSearch.city=["remote"])
    reports totalCount 1,039 — 5.5%.  Trusting fullyRemote flagged 15,612 of
    17,821 rows Remote, a ~15x over-count that would have poisoned every
    remote-targeted cut downstream.
    """
    ids = set()
    page = 1
    pages = None
    while page <= (pages or 1):
        d = pj(s, NFJ_URL, params={**NFJ_PARAMS, "page": page},
               json={"rawSearch": "remote"})
        if d is None:
            break
        if pages is None:
            pages = int(d.get("totalPages") or 1)
            print(f"    nofluffjobs remote partition: "
                  f"{d.get('totalCount')} postings", flush=True)
        posts = d.get("postings") or []
        if not posts:
            break
        ids.update(p["id"] for p in posts if p.get("id"))
        page += 1
        time.sleep(0.8)
    return ids


def do_nofluffjobs(s, out):
    n = 0
    seen = set()
    page = 1
    pages = None
    remote_ids = nfj_remote_ids(s)
    print(f"    nofluffjobs: {len(remote_ids)} remote ids collected",
          flush=True)
    while page <= (pages or 40):
        d = pj(s, NFJ_URL, params={**NFJ_PARAMS, "page": page},
               json={"rawSearch": ""})
        if d is None:
            break
        if pages is None:
            pages = int(d.get("totalPages") or 1)
            print(f"  nofluffjobs: totalCount={d.get('totalCount')} "
                  f"pages={pages} @limit={NFJ_PARAMS['limit']}", flush=True)
        posts = d.get("postings") or []
        if not posts:
            break
        for p in posts:
            pid = p.get("id")
            if not pid or pid in seen:
                continue
            seen.add(pid)
            loc = p.get("location") or {}
            places = loc.get("places") or []
            pl = places[0] if places else {}
            ctry = (pl.get("country") or {}).get("name") or ""
            city = pl.get("city") or ""
            # Remote comes from the board's own filter, NOT from
            # location.fullyRemote — see nfj_remote_ids() for why.
            fully = pid in remote_ids
            hybrid = bool((loc.get("hybridDesc") or "").strip())
            # Everything else is left BLANK rather than guessed: a non-remote
            # row here may still be hybrid or on-site and the API does not say.
            wm = "Remote" if fully else ("Hybrid" if hybrid else "")
            sal = p.get("salary") or {}
            st_ = (sal.get("type") or "").lower()
            et = cc.classify_any(NFJ_TYPE.get(st_, st_), p.get("title"))
            slug = p.get("url") or pid
            out.write(cc.row(
                source="nofluffjobs", source_type="job_board",
                company=p.get("name"), title=p.get("title"),
                employment_type=et, work_mode=wm, remote=fully,
                location=("Remote" if fully else
                          ", ".join(x for x in (city, ctry) if x)),
                city=city, country=ctry,
                salary_min=num(sal.get("from")), salary_max=num(sal.get("to")),
                salary_currency=(sal.get("currency") or "").upper(),
                salary_period="Month" if sal.get("from") else "",
                posted=cc.ymd(ts=p.get("posted")),
                apply_url=f"https://nofluffjobs.com/job/{slug}",
                job_id=pid, board="nofluffjobs",
                board_url="https://nofluffjobs.com"))
            n += 1
        page += 1
        time.sleep(1.0)
    print(f"  nofluffjobs: {n}", flush=True)


# ========================================================== 3. INSTAHYRE ======
IH_URL = "https://www.instahyre.com/api/v1/job_search/"
IH_FN_URL = "https://www.instahyre.com/api/v1/job_function/"
IH_STEP = 35              # limit= is clamped to 35 server-side; offset advances
# HARD DEEP-PAGING CEILING: offset 9,965 answers 200, offset 9,966 answers
# HTTP 500 — i.e. offset+limit must stay <= 10,000, the stock Elasticsearch
# max_result_window.  A flat offset walk therefore tops out at 10k of the
# ~13.9k corpus and then fails with a 500 that looks like a dead source.
# `job_functions=<id>` genuinely partitions (id 10 -> 3,948, id 1 -> 1,862),
# every partition sits well under the ceiling, so the sweep below is the only
# way to reach the whole board.  `location=` does NOT partition — it is a
# no-op that echoes the full 13.9k count.
IH_MAX_OFFSET = 9965
# instahyre also throttles hard: a flat-out walk got ~8,350 rows in and then
# 429'd EVERY request, including offset=0, with Retry-After ~724s.
IH_PACE = float(os.environ.get("IH_PACE") or 1.3)


def ih_seen_ids():
    """job ids already in the output jsonl, so a re-run tops up rather than
    duplicating.  The partition sweep revisits jobs an earlier flat walk
    already wrote, and the Writer only ever appends."""
    seen = set()
    if not os.path.exists(OUT):
        return seen
    with open(OUT, encoding="utf-8") as f:
        for line in f:
            if '"instahyre"' not in line:
                continue
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if d.get("source") == "instahyre" and d.get("job_id") not in (
                    None, ""):
                seen.add(str(d["job_id"]))
    return seen


def do_instahyre(s, out, st):
    ck = st.get("instahyre") or {}
    done = set(ck.get("done") or [])
    n = int(ck.get("rows") or 0)
    seen = ih_seen_ids()
    print(f"  instahyre: {len(seen)} ids already on disk, "
          f"{len(done)} partitions done", flush=True)

    d = gj(s, IH_FN_URL, params={"limit": 300})
    fns = []
    for o in (d or {}).get("objects", []):
        ru = o.get("resource_uri") or ""
        fid = str(o.get("id") or ru.rstrip("/").split("/")[-1])
        if fid:
            fns.append((fid, o.get("name") or ""))
    if not fns:
        print("  instahyre: could not list job_function ids, aborting",
              flush=True)
        return
    print(f"  instahyre: sweeping {len(fns)} job_function partitions",
          flush=True)

    stop = False
    for fid, fname in fns:
        if stop:
            break
        if fid in done:
            continue
        off = 0
        total = None
        misses = 0
        while off <= IH_MAX_OFFSET:
            d = gj(s, IH_URL, params={"job_type": 0, "limit": IH_STEP,
                                      "offset": off, "job_functions": fid},
                   tries=5)
            if d is None:
                misses += 1
                if misses >= 3:
                    print(f"  instahyre: throttled out in partition {fid} "
                          f"({fname}) at offset {off}; state saved, re-run "
                          f"to resume", flush=True)
                    stop = True
                    break
                time.sleep(90 * misses)
                continue
            misses = 0
            meta = d.get("meta") or {}
            if total is None:
                total = int(meta.get("total_count") or 0)
            objs = d.get("objects") or []
            if not objs:
                break
            for o in objs:
                jid = o.get("id")
                if jid is None or str(jid) in seen:
                    continue
                seen.add(str(jid))
                emp = o.get("employer") or {}
                locs = o.get("locations") or ""
                # No structured remote field on this API (location=/remote=
                # filters are no-ops).  "Work From Home" IS one of instahyre's
                # enumerated location values, so it is a structured token, not
                # a keyword guess.
                parts = [x.strip() for x in str(locs).split(",") if x.strip()]
                wfh = any(p.lower() == "work from home" for p in parts)
                cities = [p for p in parts
                          if p.lower() != "work from home"]
                out.write(cc.row(
                    source="instahyre", source_type="job_board",
                    company=emp.get("company_name") or "",
                    title=o.get("title") or o.get("candidate_title"),
                    employment_type=cc.classify_any(None, o.get("title")),
                    work_mode="Remote" if wfh else "", remote=wfh,
                    location=locs or "", city="; ".join(cities),
                    country="India",
                    apply_url=o.get("public_url") or "", job_id=jid,
                    board="instahyre",
                    board_url="https://www.instahyre.com/jobs/"))
                n += 1
            off += IH_STEP
            if total and off >= min(total, IH_MAX_OFFSET + 1):
                break
            time.sleep(IH_PACE)
        if not stop:
            done.add(fid)
            st["instahyre"] = {"done": sorted(done), "rows": n}
            cc.save_state(STATE, st)
            print(f"    instahyre partition {fid} ({fname}) done — "
                  f"{n} rows, {len(seen)} unique ids", flush=True)
    st["instahyre"] = {"done": sorted(done), "rows": n}
    cc.save_state(STATE, st)
    print(f"  instahyre: {n} new rows this run; {len(seen)} unique ids total; "
          f"{len(done)}/{len(fns)} partitions complete", flush=True)


# =========================================================== 4. GETONBRD ======
GOB = "https://www.getonbrd.com/api/v0"
GOB_PER = 100             # per_page caps at 120; total_pages is per_page-relative
GOB_MODALITY = {"1": "full-time", "2": "part-time", "3": "freelance",
                "4": "internship"}


def gob_company(s, cid, cache):
    """Job records carry only a company id; resolve name once per id."""
    cid = str(cid)
    if cid in cache:
        return cache[cid]
    d = gj(s, f"{GOB}/companies/{cid}")
    name = ""
    if d:
        name = ((d.get("data") or {}).get("attributes") or {}).get("name") or ""
    cache[cid] = name
    time.sleep(0.2)
    return name


def do_getonbrd(s, out):
    d = gj(s, f"{GOB}/categories")
    cats = [c["id"] for c in (d or {}).get("data", [])]
    if not cats:
        print("  getonbrd: no categories, aborting", flush=True)
        return
    print(f"  getonbrd: {len(cats)} categories", flush=True)
    n = 0
    seen = set()
    cache = {}
    zones = {}
    for cat in cats:
        page = 1
        pages = None
        while page <= (pages or 1):
            d = gj(s, f"{GOB}/categories/{cat}/jobs",
                   params={"per_page": GOB_PER, "page": page})
            if d is None:
                break
            if pages is None:
                pages = int((d.get("meta") or {}).get("total_pages") or 1)
            rows = d.get("data") or []
            if not rows:
                break
            for j in rows:
                jid = j.get("id")
                if not jid or jid in seen:
                    continue
                seen.add(jid)
                a = j.get("attributes") or {}
                cid = ((a.get("company") or {}).get("data") or {}).get("id")
                comp = gob_company(s, cid, cache) if cid else ""
                is_remote = bool(a.get("remote"))
                rmod = a.get("remote_modality") or ""
                zone = a.get("remote_zone")
                if is_remote:
                    zones[rmod] = zones.get(rmod, 0) + 1
                wm = ("Hybrid" if "hybrid" in rmod
                      else ("Remote" if is_remote else "On-site"))
                mid = str(((a.get("modality") or {}).get("data") or {})
                          .get("id") or "")
                ctys = [str(c) for c in (a.get("countries") or []) if c]
                out.write(cc.row(
                    source="getonbrd", source_type="job_board",
                    company=comp, title=a.get("title"),
                    employment_type=cc.classify_any(GOB_MODALITY.get(mid),
                                                    a.get("title")),
                    work_mode=wm, remote=is_remote,
                    location="; ".join(ctys) or (zone or ""),
                    country="; ".join(c for c in ctys if c != "Remote"),
                    salary_min=num(a.get("min_salary")),
                    salary_max=num(a.get("max_salary")),
                    salary_currency="USD" if a.get("min_salary") else "",
                    salary_period="Month" if a.get("min_salary") else "",
                    posted=cc.ymd(ts=a.get("published_at")),
                    apply_url=f"https://www.getonbrd.com/jobs/{jid}",
                    job_id=jid, board="getonbrd",
                    board_url="https://www.getonbrd.com/jobs"))
                n += 1
            page += 1
            time.sleep(0.4)
    print(f"  getonbrd: {n} (remote_modality mix: {zones})", flush=True)


# ============================================================= 5. THE HUB =====
HUB_URL = "https://thehub.io/api/jobs"
# scraped live from thehub.io/jobs — the API exposes only these ObjectIds and
# there is no public lookup endpoint for them (all four guessed paths 404).
HUB_POS = {
    "5b8e46b3853f039706b6ea70": "full-time",
    "5b8e46b3853f039706b6ea71": "part-time",
    "5b8e46b3853f039706b6ea72": "student job",
    "5b8e46b3853f039706b6ea73": "internship",
    "5b8e46b3853f039706b6ea74": "cofounder",
    "5b8e46b3853f039706b6ea75": "freelance",
}


def _rng(*ds):
    for d in ds:
        if isinstance(d, dict) and d:
            lo = d.get("min") or d.get("from") or d.get("low")
            hi = d.get("max") or d.get("to") or d.get("high")
            if lo or hi:
                return num(lo), num(hi), (d.get("currency") or "").upper()
    return "", "", ""


def do_thehub(s, out):
    n = 0
    seen = set()
    page = 1
    pages = None
    while page <= (pages or 1):
        # limit= is IGNORED here (always 15 docs); page is the real paginator.
        d = gj(s, HUB_URL, params={"page": page})
        if d is None:
            break
        if pages is None:
            pages = int(d.get("pages") or 1)
            print(f"  thehub: total={d.get('total')} pages={pages} "
                  f"(limit is ignored, 15/page)", flush=True)
        docs = d.get("docs") or []
        if not docs:
            break
        for j in docs:
            jid = j.get("id")
            if not jid or jid in seen:
                continue
            seen.add(jid)
            co = j.get("company") or {}
            loc = j.get("location") or {}
            pts = [HUB_POS.get(str(t), "") for t in (j.get("jobPositionTypes")
                                                     or [])]
            is_remote = bool(j.get("isRemote"))
            lo, hi, curr = _rng(j.get("salaryRange"),
                                j.get("monthlySalaryRange"),
                                j.get("hourlyRateRange"))
            period = ("Year" if (j.get("salaryRange") or {}) else
                      "Month" if (j.get("monthlySalaryRange") or {}) else
                      "Hour" if (j.get("hourlyRateRange") or {}) else "")
            # `link` is the employer's own ATS posting (greenhouse/lever/...)
            # for scraped rows; absoluteJobUrl is thehub's redirect page.
            out.write(cc.row(
                source="thehub", source_type="job_board",
                company=co.get("name") or "", title=j.get("title"),
                employment_type=cc.classify_any(" ".join(p for p in pts if p),
                                                j.get("title")),
                work_mode="Remote" if is_remote else "", remote=is_remote,
                location=loc.get("address") or loc.get("locality") or
                ("Remote" if is_remote else ""),
                city=loc.get("locality") or "",
                country=loc.get("country") or j.get("countryCode") or "",
                salary_min=lo, salary_max=hi, salary_currency=curr,
                salary_period=period if (lo or hi) else "",
                posted=cc.ymd(iso=j.get("publishedAt") or j.get("createdAt")),
                apply_url=j.get("link") or j.get("absoluteJobUrl") or "",
                job_id=jid, board="thehub",
                board_url="https://thehub.io/jobs"))
            n += 1
        page += 1
        time.sleep(0.5)
    print(f"  thehub: {n}", flush=True)


# ======================================================== 6. LANDING.JOBS =====
LJ_URL = "https://landing.jobs/api/v1/jobs"
LJ_STEP = 50
_LJ_SLUG = re.compile(r"/at/([^/]+)/")


def do_landingjobs(s, out):
    n = 0
    seen = set()
    off = 0
    misses = 0
    while off < 20000:
        # `page` is IGNORED on this API (page=1/2/3 -> identical rows);
        # `offset` is the only parameter that moves the window.
        d = gj(s, LJ_URL, params={"offset": off})
        if d is None:
            misses += 1
            if misses >= 3:
                break
            time.sleep(8 * misses)
            continue
        misses = 0
        if not isinstance(d, list) or not d:
            break
        fresh = 0
        for j in d:
            jid = j.get("id")
            if jid is None or jid in seen:
                continue
            seen.add(jid)
            fresh += 1
            url = j.get("url") or ""
            m = _LJ_SLUG.search(url)
            comp = (m.group(1).replace("-", " ").title() if m else "")
            locs = j.get("locations") or []
            cities = [x.get("city") for x in locs if x.get("city")]
            ctys = [x.get("country_code") for x in locs
                    if x.get("country_code")]
            is_remote = bool(j.get("remote"))
            out.write(cc.row(
                source="landingjobs", source_type="job_board",
                company=comp, title=j.get("title"),
                employment_type=cc.classify_any(j.get("type"), j.get("title")),
                work_mode="Remote" if is_remote else "", remote=is_remote,
                location="; ".join(cities) or ("Remote" if is_remote else ""),
                city="; ".join(cities), country="; ".join(ctys),
                salary_min=num(j.get("gross_salary_low")),
                salary_max=num(j.get("gross_salary_high")),
                salary_currency=(j.get("currency_code") or "").upper(),
                salary_period="Year" if j.get("gross_salary_low") else "",
                posted=cc.ymd(iso=j.get("published_at")),
                apply_url=url, job_id=jid, board="landingjobs",
                board_url="https://landing.jobs/jobs"))
            n += 1
        if len(d) < LJ_STEP or not fresh:
            break
        off += LJ_STEP
        time.sleep(0.5)
    print(f"  landingjobs: {n}", flush=True)


# ================================================================= driver =====
SOURCES = {
    "torre": do_torre,
    "nofluffjobs": do_nofluffjobs,
    "instahyre": do_instahyre,
    "getonbrd": do_getonbrd,
    "thehub": do_thehub,
    "landingjobs": do_landingjobs,
}


def main():
    only = [x for x in os.environ.get("INTL_ONLY", "").split(",") if x]
    for name in only:
        if name not in SOURCES:
            raise SystemExit(f"unknown source {name!r} (choose from "
                             f"{', '.join(SOURCES)})")
    st = {} if os.environ.get("INTL_STATE_RESET") == "1" \
        else cc.load_state(STATE)
    out = cc.Writer(OUT)
    s = cc.session()
    s.headers.update({"Content-Type": "application/json"})
    for name, fn in SOURCES.items():
        if only and name not in only:
            continue
        try:
            if name in ("torre", "instahyre"):   # resumable, take the state
                fn(s, out, st)
            else:
                fn(s, out)
        except Exception as e:  # noqa: BLE001 — one dead board is not fatal
            print(f"  {name}: ERROR {type(e).__name__}: {e}", flush=True)
    out.close()
    s.close()
    print("DONE intlboards_fetch", flush=True)


if __name__ == "__main__":
    main()
