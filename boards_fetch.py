#!/usr/bin/env python3
"""
boards_fetch.py — harvest roles from open marketplace, remote-board, and RSS
sources (all unauthenticated, verified 2026-07-19).  Each source writes into
one shared contract_norm/boards.jsonl.

Sources:
  freelancer     marketplace   projects/0.1/projects/active (all freelance)
  braintrust     marketplace   app.usebraintrust.com/api/jobs (contract net)
  remotive       remote_board  remotive.com/api/remote-jobs   (job_type)
  remoteok       remote_board  remoteok.com/api               (tags)
  himalayas      remote_board  himalayas.app/jobs/api         (employmentType)
  jobicy         remote_board  jobicy.com/api/v2/remote-jobs  (jobType)
  workingnomads  remote_board  workingnomads.com/api          (title-only)
  arbeitnow      remote_board  arbeitnow.com/api/job-board-api(job_types)
  weworkremotely remote_board  weworkremotely.com/*.rss       (title-only)
  problogger     remote_board  problogger.com/jobs/feed       (freelance)

BOARDS_KEEP_ALL=1 keeps FULL-TIME roles too, the same way ATS_KEEP_ALL does for
ats_fetch.py and PHENOM_KEEP_ALL for phenom_fetch.py.  Without it every one of
these boards is filtered down to its contingent slice — which is why the eight
remote boards together contributed only 256 rows to all_jobs_master.csv while
their APIs carry tens of thousands of postings, nearly all of them remote.
run_all.py passes the flag; a bare run stays contract-only for the legacy
Contract_Gigs workbooks.
"""
import json
import os
import time
from urllib.parse import quote

import defusedxml.ElementTree as ET  # hardened against XXE / billion-laughs

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
KEEP_ALL = os.environ.get("BOARDS_KEEP_ALL") == "1"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
OUT = os.path.join(cc.NORM_DIR,
                   "boards_all.jsonl" if KEEP_ALL else "boards.jsonl")
# Depth caps.  Contract-only mode keeps ~1 row in 20, so a deep crawl bought
# almost nothing and the old caps were sized for that; KEEP_ALL keeps every
# row, so depth is now the only thing bounding the haul.
# 100000 -> 250000 for KEEP_ALL: the board OVERTOOK the cap.  himalayas
# reported totalCount=100431 on 2026-08-19, so the ceiling was already below
# the corpus and every run silently dropped the tail.  A cap that tracks a
# growing board needs headroom, not a round number that happens to match it.
HIMALAYAS_MAX_OFFSET = int(os.environ.get("HIMALAYAS_MAX_OFFSET")
                           or (250000 if KEEP_ALL else 12000))
# Resume knob: the writer APPENDS and himalayas keeps no per-offset state, so
# picking up where a capped run stopped needs an explicit start rather than a
# re-run (which would duplicate every row already collected).
HIMALAYAS_START_OFFSET = int(os.environ.get("HIMALAYAS_START_OFFSET") or 0)
# How many CONSECUTIVE blocked answers (406/429/5xx/transport) end the walk.
# Deliberately far above the 5-empty-page rule: an empty page is evidence about
# the feed, a blocked one is evidence about us.  12 tries with the capped
# back-off below is ~20 minutes of patience at one offset before giving up.
HIMALAYAS_BLOCK_TRIES = int(os.environ.get("HIMALAYAS_BLOCK_TRIES") or 12)
# Below this fraction of a board's own totalCount, the run is INCOMPLETE and
# says so in its last line -- run_all surfaces that line as the step summary.
HIMALAYAS_MIN_COVERAGE = float(os.environ.get("HIMALAYAS_MIN_COVERAGE") or 0.9)

# Boards that ended short of their own advertised total.  A silently-truncated
# board looked exactly like a healthy one before 2026-08-25.
SHORTFALL = []
ARBEITNOW_MAX_PAGE = int(os.environ.get("ARBEITNOW_MAX_PAGE")
                         or (400 if KEEP_ALL else 60))


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
    except (TypeError, ValueError):
        return ""


def jsn(s, url, err_out=None, **kw):
    """cc.get_json, but a transport failure is LOUD instead of empty.

    Pass err_out={} to also learn *why* the answer was empty.  Printing the
    reason was never enough: a caller that pages a feed has to tell "the board
    is throttling me" from "the board has no more jobs", and both arrive here
    as {}.  himalayas ended its 2026-08-24 walk at 18% of the board because it
    could not.

    cc.get_json hands back {"__error__": ...} / {"__status__": 404}, and every
    caller here reads it as d.get("jobs") or [] — so a timeout, a 403 or a
    DNS blip is indistinguishable from "this board has no jobs today" and the
    source silently reports 0.  That is exactly how the 2026-07-28 keep-all run
    recorded jobicy/workingnomads/arbeitnow/problogger as 0 while all four APIs
    were in fact healthy when re-probed minutes later.
    """
    d = cc.get_json(s, url, **kw)
    if err_out is not None:
        err_out.clear()
    if isinstance(d, dict) and ("__error__" in d or "__status__" in d):
        why = d.get("__error__") or f"HTTP {d.get('__status__')}"
        print(f"    ! {url} -> {why}", flush=True)
        if err_out is not None:
            err_out.update(status=d.get("__status__"),
                           error=d.get("__error__"), why=why)
        return {}
    return d


def do_freelancer(s, out):
    n = 0
    off = 0
    while off < 8000:
        d = jsn(
            s, "https://www.freelancer.com/api/projects/0.1/projects/active/"
               f"?limit=100&offset={off}&compact=true")
        res = (d.get("result") or {})
        projs = res.get("projects") or []
        if not projs:
            break
        for p in projs:
            b = p.get("budget") or {}
            cur = (p.get("currency") or {}).get("code") or ""
            hourly = p.get("type") == "hourly"
            out.write(cc.row(
                source="freelancer", source_type="marketplace",
                company="(Freelancer.com client)", title=p.get("title"),
                employment_type="Freelance", work_mode="Remote", remote=True,
                location="Remote / Online",
                salary_min=num(b.get("minimum")), salary_max=num(b.get("maximum")),
                salary_currency=cur,
                salary_period="Hour" if hourly else "Project",
                posted=cc.ymd(ts=p.get("time_submitted")),
                apply_url="https://www.freelancer.com/projects/"
                          + (p.get("seo_url") or ""),
                job_id=p.get("id"), board="freelancer",
                board_url="https://www.freelancer.com/jobs"))
            n += 1
        off += 100
        if off >= (res.get("total_count") or 0):
            break
    print(f"  freelancer: {n}", flush=True)


def do_braintrust(s, out):
    n = 0
    page = 1
    while page <= 60:
        d = jsn(s, f"https://app.usebraintrust.com/api/jobs/?page={page}")
        results = d.get("results") or []
        if not results:
            break
        for j in results:
            loc = j.get("locations") or j.get("location") or ""
            if isinstance(loc, list):
                loc = "; ".join(str(x) for x in loc)
            out.write(cc.row(
                source="braintrust", source_type="marketplace",
                company=(j.get("employer") or {}).get("name") or "",
                title=j.get("title"), employment_type="Contract",
                work_mode="Remote", remote=True,
                location=loc or "Remote",
                salary_min=num(j.get("budget_minimum_usd")),
                salary_max=num(j.get("budget_maximum_usd")),
                salary_currency="USD",
                salary_period="Hour" if j.get("payment_type") == "hourly"
                else "",
                apply_url="https://app.usebraintrust.com/jobs/"
                          f"{j.get('id')}/",
                job_id=j.get("id"), board="braintrust",
                board_url="https://app.usebraintrust.com/jobs/"))
            n += 1
        if not d.get("next"):
            break
        page += 1
    print(f"  braintrust: {n}", flush=True)


def do_remotive(s, out):
    d = jsn(s, "https://remotive.com/api/remote-jobs")
    n = 0
    for j in (d.get("jobs") or []):
        et = CLASSIFY((j.get("job_type") or "").replace("_", " "),
                      j.get("title"))
        if not et:
            continue
        out.write(cc.row(
            source="remotive", source_type="remote_board",
            company=j.get("company_name"), title=j.get("title"),
            employment_type=et, work_mode="Remote", remote=True,
            location=j.get("candidate_required_location") or "Remote",
            salary_min="", salary_max="",
            posted=cc.ymd(iso=j.get("publication_date")),
            apply_url=j.get("url"), job_id=j.get("id"), board="remotive",
            board_url="https://remotive.com/remote-jobs"))
        n += 1
    print(f"  remotive: {n}", flush=True)


# remoteok.com/api caps at 100 rows and silently IGNORES ?offset= and ?limit=
# (measured 2026-07-28: byte-identical 450,789-byte body for every variant).
# ?tags=<tag> is the only thing that returns a different 100, so the tag sweep
# below is the entire pagination story.  Yields measured per tag on the same
# date; the union came to 669 unique ids against 100 for the bare feed.
RO_TAGS = ["dev", "python", "product", "data", "non tech", "crypto",
           "javascript", "engineer", "sales", "marketing", "senior",
           "finance", "design", "support"]


def do_remoteok(s, out):
    n = 0
    seen = set()
    for tag in [None] + RO_TAGS:
        url = "https://remoteok.com/api"
        if tag:
            url += "?tags=" + quote(tag)
        d = jsn(s, url)
        for j in (d if isinstance(d, list) else []):
            # Element [0] of the array is NOT a job — it is remoteok's legal
            # notice object.  Keying on position skips it.
            if not isinstance(j, dict) or not j.get("position"):
                continue
            jid = str(j.get("id") or j.get("slug") or j.get("url") or "")
            if jid in seen:
                continue
            seen.add(jid)
            tags = " ".join(j.get("tags") or [])
            et = CLASSIFY(tags, j.get("position"))
            if not et:
                continue
            out.write(cc.row(
                source="remoteok", source_type="remote_board",
                company=j.get("company"), title=j.get("position"),
                employment_type=et, work_mode="Remote", remote=True,
                location=j.get("location") or "Remote",
                salary_min=num(j.get("salary_min")),
                salary_max=num(j.get("salary_max")),
                salary_currency="USD",
                posted=cc.ymd(iso=j.get("date")), apply_url=j.get("url"),
                job_id=j.get("id"), board="remoteok",
                board_url="https://remoteok.com"))
            n += 1
        time.sleep(1.0)
    print(f"  remoteok: {n} (union of {len(RO_TAGS) + 1} tag feeds)",
          flush=True)


def do_himalayas(s, out):
    # limit= is silently IGNORED (limit=50/100/500/1000 all answer with
    # limit:20 and 20 rows), so offset in steps of 20 is the only paginator and
    # the full board is ~4,750 requests.  totalCount is the real ceiling —
    # 95,055 measured 2026-07-28, against the 38,440 an earlier run stopped at
    # because it treated ONE short page as the end of the feed.  A short page
    # mid-feed is normal here (expired postings are filtered out of a page
    # after the count is computed), so only an EMPTY page ends the walk, and
    # even then only after a retry.
    # himalayas.app 429s at roughly 3,500 rows in when paged flat out, so the
    # walk is paced AND retries the SAME offset on an empty answer.  Retrying
    # the same offset is the part that matters: jsn() cannot distinguish "429"
    # from "no more jobs" in its return value, so advancing the offset on an
    # empty page would silently drop 20 live postings per rate-limit hit.
    n = 0
    off = HIMALAYAS_START_OFFSET
    total = None
    tries = 0          # consecutive genuinely-empty pages (HTTP 200, jobs: [])
    blocked = 0        # consecutive throttled/errored answers
    throttled = 0      # total, for the summary
    stopped = ""
    err = {}
    while off < HIMALAYAS_MAX_OFFSET:
        d = jsn(s, "https://himalayas.app/jobs/api"
                   f"?limit=20&offset={off}", err_out=err)
        if total is None and d.get("totalCount"):
            total = int(d["totalCount"])
            print(f"    himalayas totalCount={total}", flush=True)
        if err:
            # A THROTTLE IS NOT THE END OF THE FEED.  Until 2026-08-25 a 406 or
            # 429 fell through to the empty-page counter below, so five blocked
            # answers in a row ended the walk and the run still printed DONE and
            # exited 0.  On 2026-08-24 that stopped himalayas at offset 19,200 of
            # totalCount 103,859 -- 67,209 rows, which was the ENTIRE boards_all
            # delta that run, and boards_all is truncate-and-rewrite so there was
            # no carry-forward to soften it.  Blocked answers now get their own
            # counter, a much longer leash and a capped linear back-off.
            blocked += 1
            throttled += 1
            if blocked >= HIMALAYAS_BLOCK_TRIES:
                stopped = f"blocked ({err.get('why')})"
                print(f"    himalayas: giving up at offset {off} after "
                      f"{blocked} blocked answers ({err.get('why')})",
                      flush=True)
                break
            time.sleep(min(15 * blocked, 120))
            continue                   # SAME offset, deliberately
        blocked = 0
        jobs = d.get("jobs") or []
        if not jobs:
            tries += 1
            if tries >= 5:
                stopped = f"{tries} empty answers"
                print(f"    himalayas: giving up at offset {off} after "
                      f"{tries} empty answers", flush=True)
                break
            time.sleep(15 * tries)     # 15/30/45/60s — a 429 needs real time
            continue                   # SAME offset, deliberately
        tries = 0
        for j in jobs:
            et = CLASSIFY(j.get("employmentType"), j.get("title"))
            if not et:
                continue
            locs = j.get("locationRestrictions") or []
            slug = j.get("guid") or j.get("slug") or ""
            out.write(cc.row(
                source="himalayas", source_type="remote_board",
                company=j.get("companyName"), title=j.get("title"),
                employment_type=et, work_mode="Remote", remote=True,
                location="; ".join(locs) if locs else "Remote",
                salary_min=num(j.get("minSalary")),
                salary_max=num(j.get("maxSalary")),
                salary_currency=(j.get("currency") or "").upper(),
                salary_period=(j.get("salaryPeriod") or "").title(),
                posted=cc.ymd(ts=j.get("pubDate") or j.get("publishedAt")),
                apply_url=j.get("applicationLink") or j.get("url")
                or (f"https://himalayas.app/jobs/{slug}" if slug else ""),
                job_id=slug, board="himalayas",
                board_url="https://himalayas.app/jobs"))
            n += 1
        off += 20
        if total and off >= total:
            break
        time.sleep(0.4)
    msg = (f"  himalayas: {n} (scanned to offset {off} of "
           f"totalCount {total})")
    if throttled:
        msg += f"; {throttled} throttled answer(s)"
    print(msg, flush=True)
    # totalCount counts postings before CLASSIFY filters them, so `n` is
    # legitimately smaller -- coverage is judged on OFFSET reached, not rows.
    if total and off < total * HIMALAYAS_MIN_COVERAGE:
        SHORTFALL.append(f"himalayas {off}/{total} "
                         f"({100.0 * off / total:.0f}%)"
                         + (f" [{stopped}]" if stopped else ""))


# jobicy has NO offset/page parameter (?offset= and ?page= return a BLANK
# body, not an error, not JSON) and count= silently clamps to 100 — count=500
# answers with 100 rows and echoes {'count': 100} in appliedFilters, so a
# fetcher that trusts the parameter believes it has the whole board.  geo and
# industry are the only partitions, and these slug lists are the ones that
# actually answered 200 when swept on 2026-07-28; the invalid ones ('anywhere',
# 'worldwide', 'design', 'asia', 'india', 'latin-america') return HTTP 400.
JOBICY_GEOS = [
    "apac", "australia", "china", "hong-kong", "japan", "new-zealand",
    "philippines", "singapore", "south-korea", "thailand", "vietnam", "emea",
    "europe", "austria", "belgium", "bulgaria", "croatia", "cyprus", "czechia",
    "denmark", "germany", "sweden", "switzerland", "uk", "ukraine", "israel",
    "turkiye", "united-arab-emirates", "latam", "argentina", "brazil",
    "costa-rica", "mexico", "canada", "usa"]
JOBICY_INDUSTRIES = [
    "supporting", "marketing", "engineering", "business", "legal",
    "data-science", "technical-support", "admin", "healthcare",
    "cybersecurity", "education", "project-management", "hr", "copywriting",
    "seo"]


def do_jobicy(s, out):
    n = 0
    seen = set()
    queries = ["https://jobicy.com/api/v2/remote-jobs?count=100"]
    queries += [f"https://jobicy.com/api/v2/remote-jobs?count=100&geo={g}"
                for g in JOBICY_GEOS]
    queries += [f"https://jobicy.com/api/v2/remote-jobs?count=100&industry={i}"
                for i in JOBICY_INDUSTRIES]
    for url in queries:
        d = jsn(s, url)
        for j in (d.get("jobs") or []):
            jid = str(j.get("id") or j.get("url") or "")
            if jid in seen:
                continue
            seen.add(jid)
            jt = j.get("jobType")
            jt = " ".join(jt) if isinstance(jt, list) else (jt or "")
            et = CLASSIFY(jt.replace("-", " "), j.get("jobTitle"))
            if not et:
                continue
            out.write(cc.row(
                source="jobicy", source_type="remote_board",
                company=j.get("companyName"), title=j.get("jobTitle"),
                employment_type=et, work_mode="Remote", remote=True,
                location=j.get("jobGeo") or "Remote",
                salary_min=num(j.get("annualSalaryMin")),
                salary_max=num(j.get("annualSalaryMax")),
                salary_currency=(j.get("salaryCurrency") or "").upper(),
                salary_period="Year",
                posted=cc.ymd(iso=j.get("pubDate")), apply_url=j.get("url"),
                job_id=j.get("id"), board="jobicy",
                board_url="https://jobicy.com"))
            n += 1
        time.sleep(0.8)
    print(f"  jobicy: {n} (union of {len(queries)} geo/industry feeds)",
          flush=True)


def do_workingnomads(s, out):
    d = jsn(s, "https://www.workingnomads.com/api/exposed_jobs/")
    n = 0
    for j in (d if isinstance(d, list) else []):
        et = CLASSIFY((j.get("category_name") or "") + " "
                         + (j.get("tags") or ""), j.get("title"))
        if not et:
            continue
        out.write(cc.row(
            source="workingnomads", source_type="remote_board",
            company=j.get("company_name"), title=j.get("title"),
            employment_type=et, work_mode="Remote", remote=True,
            location=j.get("location") or "Remote",
            posted=cc.ymd(iso=j.get("pub_date")), apply_url=j.get("url"),
            job_id=j.get("url"), board="workingnomads",
            board_url="https://www.workingnomads.com"))
        n += 1
    print(f"  workingnomads: {n}", flush=True)


def do_arbeitnow(s, out):
    # arbeitnow 429s from about page 11 when paged flat out (measured
    # 2026-07-28), and an unpaced loop reads that as "no more data" and stops
    # ~1k rows in.  One retry after a pause recovers it; the pace between
    # pages keeps it from happening in the first place.
    n = 0
    page = 1
    while page <= ARBEITNOW_MAX_PAGE:
        url = ("https://www.arbeitnow.com/api/job-board-api"
               f"?page={page}")
        d = jsn(s, url)
        data = d.get("data") or []
        if not data:
            time.sleep(20)
            d = jsn(s, url)
            data = d.get("data") or []
        if not data:
            break
        for j in data:
            jt = " ".join(j.get("job_types") or [])
            et = CLASSIFY(jt.replace("_", " "), j.get("title"))
            if not et:
                continue
            out.write(cc.row(
                source="arbeitnow", source_type="remote_board",
                company=j.get("company_name"), title=j.get("title"),
                employment_type=et,
                work_mode="Remote" if j.get("remote") else "",
                remote=bool(j.get("remote")),
                location=j.get("location") or "",
                posted=cc.ymd(ts=j.get("created_at")),
                apply_url=j.get("url"), job_id=j.get("slug"),
                board="arbeitnow", board_url="https://www.arbeitnow.com"))
            n += 1
        links = d.get("links") or {}
        if not links.get("next"):
            break
        page += 1
        time.sleep(0.7)
    print(f"  arbeitnow: {n}", flush=True)


def _rss_items(s, url):
    r = s.get(url, timeout=25)
    root = ET.fromstring(r.content)
    return root.findall(".//item")


# The documented main feed is hard-capped at 100 items.  The 11 per-category
# feeds are not, and their union came to 752 unique links when measured
# 2026-07-28 — 7.5x the main feed.  They overlap heavily (naive summation gives
# 793), so the walk dedupes on <link>.  Slugs scraped live from the homepage's
# href="/categories/..." links; that is the complete set.
WWR_FEEDS = ["https://weworkremotely.com/remote-jobs.rss"] + [
    f"https://weworkremotely.com/categories/{slug}.rss" for slug in (
        "remote-programming-jobs", "remote-back-end-programming-jobs",
        "remote-front-end-programming-jobs",
        "remote-full-stack-programming-jobs", "remote-devops-sysadmin-jobs",
        "remote-design-jobs", "remote-product-jobs",
        "remote-customer-support-jobs", "remote-sales-and-marketing-jobs",
        "remote-management-and-finance-jobs", "all-other-remote-jobs")]


def do_wwr(s, out):
    n = 0
    items = []
    for url in WWR_FEEDS:
        try:
            items += _rss_items(s, url)
        except Exception as e:  # noqa: BLE001 — one dead category is not fatal
            print(f"    ! {url} -> {type(e).__name__}", flush=True)
        time.sleep(0.5)
    seen = set()
    for it in items:
        link = (it.findtext("link") or "").strip()
        if link in seen:
            continue
        seen.add(link)
        title = (it.findtext("title") or "").strip()
        region = (it.findtext("region") or "").strip()
        # There is no company element; company is prefixed into <title> before
        # the colon ("Toptal: SharePoint & Microsoft 365 Consultant").
        company, role = (title.split(":", 1) + [""])[:2] if ":" in title \
            else ("", title)
        et = CLASSIFY(None, role or title)
        if not et:
            continue
        out.write(cc.row(
            source="weworkremotely", source_type="remote_board",
            company=company.strip(), title=(role or title).strip(),
            employment_type=et, work_mode="Remote", remote=True,
            location=region or "Remote",
            posted=cc.ymd(iso=it.findtext("pubDate")),
            apply_url=it.findtext("link"), job_id=it.findtext("link"),
            board="weworkremotely",
            board_url="https://weworkremotely.com"))
        n += 1
    print(f"  weworkremotely: {n}", flush=True)


def do_problogger(s, out):
    n = 0
    try:
        items = _rss_items(s, "https://problogger.com/jobs/feed/")
    except Exception:
        print("  problogger: fetch error", flush=True)
        return
    for it in items:
        title = (it.findtext("title") or "").strip()
        et = CLASSIFY(None, title) or "Freelance"  # board is freelance
        out.write(cc.row(
            source="problogger", source_type="remote_board",
            company="", title=title, employment_type=et,
            work_mode="Remote", remote=True, location="Remote",
            posted=cc.ymd(iso=it.findtext("pubDate")),
            apply_url=it.findtext("link"), job_id=it.findtext("link"),
            board="problogger", board_url="https://problogger.com/jobs/"))
        n += 1
    print(f"  problogger: {n}", flush=True)


SOURCES = {
    "freelancer": do_freelancer, "braintrust": do_braintrust,
    "remotive": do_remotive, "remoteok": do_remoteok,
    "himalayas": do_himalayas, "jobicy": do_jobicy,
    "workingnomads": do_workingnomads, "arbeitnow": do_arbeitnow,
    "weworkremotely": do_wwr, "problogger": do_problogger,
}


def main():
    # BOARDS_ONLY=a,b,c re-runs just those sources.  The writer APPENDS, so
    # re-running one source on top of an existing file would leave TWO copies
    # of it -- which is the trap in the recovery path for a throttled board:
    # you re-fetch himalayas to repair it and silently double what survived.
    # Strip exactly the sources about to be rewritten, keep everything else.
    only = [x for x in os.environ.get("BOARDS_ONLY", "").split(",") if x]
    for name in only:
        if name not in SOURCES:
            raise SystemExit(f"unknown source {name!r} (choose from "
                             f"{', '.join(SOURCES)})")
    if only and os.path.exists(OUT):
        kept = dropped = 0
        tmp = OUT + ".tmp"
        with open(OUT, encoding="utf-8", errors="replace") as f, \
                open(tmp, "w", encoding="utf-8") as g:
            for line in f:
                if not line.strip():
                    continue
                try:
                    src = json.loads(line).get("source")
                except ValueError:
                    continue
                if src in only:
                    dropped += 1
                    continue
                g.write(line)
                kept += 1
        os.replace(tmp, OUT)
        print(f"  BOARDS_ONLY={','.join(only)}: dropped {dropped} old row(s) "
              f"from those sources, kept {kept} from the rest", flush=True)
    out = cc.Writer(OUT)
    s = cc.session()
    for name, fn in SOURCES.items():
        if only and name not in only:
            continue
        try:
            fn(s, out)
        except Exception as e:  # noqa: BLE001
            print(f"  {fn.__name__}: ERROR {type(e).__name__}: {e}",
                  flush=True)
    out.close()
    s.close()
    # run_all reports the LAST non-=== line of this log as the step's summary,
    # so the health signal has to live there or nobody sees it.
    if SHORTFALL:
        print("DONE boards_fetch — INCOMPLETE: " + "; ".join(SHORTFALL),
              flush=True)
    else:
        print("DONE boards_fetch", flush=True)


if __name__ == "__main__":
    main()
