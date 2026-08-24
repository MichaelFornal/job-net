#!/usr/bin/env python3
"""
web3career_fetch.py — harvest web3.career into contract_norm/web3career.jsonl.

SHAPE
  GET https://web3.career/?page=N   (page 1 is https://web3.career/ — ?page=1
  301s).  Each page carries 15 real listing rows plus a fixed block of ~5
  sponsored postings, and one schema.org JobPosting JSON-LD per posting.

  40,339 jobs advertised ("has 40,339 jobs" in the page chrome), 15 per page ->
  ~2,690 pages.  Pagination genuinely advances: page 2 is 2026-07-28 Zscaler,
  page 800 is 2025-08 Coins.ph/Pond/visa, and pages 2500/2688/2690 share zero
  ids with one another.  Verified deepest real page: 2689.

TWO PHASES, THREE FILES — and why
  The listing is unmetered; the apply-url click-out is aggressively rate
  limited (see TRAPS).  Interleaving them means the rate limit throttles the
  whole crawl to a crawl, so they are separated:

    phase 1  listing  -> web3career_listing.jsonl   (append-only raw cache:
                         job id, href, the JobPosting JSON-LD, estimated flag)
    phase 2  resolve  -> web3career_apply_cache.json (job id -> real ATS url)
    phase 3  emit     -> contract_norm/web3career.jsonl, REGENERATED from the
                         two caches every run via a tmp file + os.replace

  Because the output is a pure function of the caches it is rewritten, not
  appended.  That is what lets a later run upgrade an already-emitted row's
  apply_url from the web3.career page to the employer's real ATS link — an
  append-only output would freeze the aggregator URL forever and the row could
  never dedupe against the greenhouse/lever copy the pipeline already holds.

APPLY URL — the reason this file is longer than a listing scraper
  web3.career deliberately hides the employer's ATS link.  The job page shows
  only /i/<token>, where token = base64(job_id) REVERSED.  Fetching /i/<token>:
    * with NO Referer -> 302 back to web3.career's own job page (a bot trap: a
      naive resolver records the aggregator URL as if it were the apply link);
    * with a Referer  -> 200 HTML whose only <script> assigns a 67 KB string to
      a 250-char random identifier.  Stripping the repeated base64-of-the-
      identifier padding leaves ~70 chars which, REVERSED, are the path
      "/z/<sig><b64 job id><b64 stamp>".  GET that path with redirects off and
      the Location header is the real ATS URL, e.g.
      https://job-boards.greenhouse.io/zscaler/jobs/5141020007?source=web3.career
  The '/z/' marker sits ~830 chars into that 68 KB body, so the response is
  streamed and truncated instead of downloaded whole.
  Not every posting has one: web3.career hosts the application itself for a
  large share of the board, and for those /z/ answers 302-back-to-web3.career
  or 404.  Those are cached as "" and the row keeps the web3.career URL.

TRAPS MEASURED HERE (2026-07-29)
  * /jobs.rss, /feed, /rss and /api/jobs are all hard 404s (887-byte pink
    gradient page).  There is no feed; the listing JSON-LD is the only machine
    surface.
  * /i/ IS RATE LIMITED and the listing is NOT.  6 threads flat out -> 12 of 45
    answered 429; a follow-up burst at 6 rps -> 56 of 60 answered 429, with NO
    Retry-After header, while /?page=N kept answering 200 throughout.  Hence
    the phase split, a separate slow pacer for the resolver, and an adaptive
    interval.  A 429 is NEVER cached as "this job has no apply url" — the id is
    left out of the cache and the next run retries it.
  * jobLocationType is "TELECOMMUTE" and applicantLocationRequirements is
    {"name": "Anywhere"} on EVERY posting — including a Zurich office role and
    a New York office role.  The structured remote field is a CONSTANT and is
    worthless as a remote signal.  jobLocation.address.addressLocality is the
    honest one: "Anywhere" means remote, a city name means a city.
  * baseSalary is usually web3.career's own ESTIMATE, flagged in the listing row
    by <span class="estimated_star">.  Salary is emitted only for rows without
    that marker, so the pipeline never inherits a guessed range.
  * ~5 JSON-LD blocks per page are sponsored postings that repeat on EVERY page
    (page 2 and page 800 share exactly those 5).  DO NOT pair JSON-LD to rows
    by position.  On an inner page the 15 real rows line up with ld[0:15] and
    the sponsored 5 trail at the end — but on PAGE 1 the sponsored rows are
    rendered at the TOP of the table while their JSON-LD still sits at the END,
    so the two lists are offset by exactly five and every row on page 1 gets
    another job's title, salary, date AND apply url.  This is silent: the rows
    look completely plausible.  Pairing is therefore done on the SLUG —
    slugify(ld.title) + "-" + slugify(ld.company) reproduces the row href
    ("FP&A Manager" @ Bcbgroup -> /fp-a-manager-bcbgroup/150890) — and a row
    whose slug matches no JSON-LD is DROPPED rather than guessed at.
  * Beyond the last real page (2690+) the site serves a FIXED 6-row fallback
    instead of an empty page — page 2690 and page 3000 are identical in their
    id set.  "Zero NEW ids on this page" is therefore the end-of-feed test, not
    "empty page".

ENV
  W3_MAX_SECONDS   total wall-clock budget for one run (default 1500)
  W3_LIST_SECONDS  share of it spent on phase 1 (default 600)
  W3_MAX_PAGES     max listing pages this run (default 0 = unbounded)
  W3_LIST_WORKERS  concurrent listing fetchers (default 5)
  W3_LIST_RPS      listing request budget per second (default 6)
  W3_WORKERS       concurrent apply-url resolvers (default 2)
  W3_RPS           resolver request budget per second (default 3)
  W3_RESOLVE       1 (default) resolve real ATS urls, 0 = skip phase 2
"""
import base64
import json
import os
import re
import sys
import threading
import time
import concurrent.futures as cf
from html import unescape

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "web3career.jsonl")
LISTING_JSONL = os.path.join(BASE, "web3career_listing.jsonl")
STATE_JSON = os.path.join(BASE, "web3career_fetch_state.json")
CACHE_JSON = os.path.join(BASE, "web3career_apply_cache.json")

ROOT = "https://web3.career"
MAX_SECONDS = int(os.environ.get("W3_MAX_SECONDS") or 1500)
LIST_SECONDS = int(os.environ.get("W3_LIST_SECONDS") or 600)
MAX_PAGES = int(os.environ.get("W3_MAX_PAGES") or 0)
LIST_WORKERS = int(os.environ.get("W3_LIST_WORKERS") or 3)
LIST_RPS = float(os.environ.get("W3_LIST_RPS") or 3)
WORKERS = int(os.environ.get("W3_WORKERS") or 2)
RPS = float(os.environ.get("W3_RPS") or 3)
RESOLVE = os.environ.get("W3_RESOLVE", "1") != "0"
TIMEOUT = 30
TRIES = 3
HARD_PAGE_CEILING = 4000        # guard against an infinite paginator
STREAM_CAP = 16384              # /i/ payload marker sits ~830 chars in
PAGE_BATCH = 30                 # listing pages fetched per parallel batch

LD_RX = re.compile(r'<script type="application/ld\+json"[^>]*>(.*?)</script>',
                   re.S)
ROW_RX = re.compile(
    r"<tr [^>]*data-jobid=(\d+)[^>]*onclick=\"table(?:Turbo)?RowClick\("
    r"event, '([^']+)'\)\"")
# the obfuscated payload: <long-random-ident>="<junk>/z/<padding>"
PAYLOAD_RX = re.compile(r'(\w{40,})="([^"]{40,}?/z/)')


class Pacer:
    """Independent token pacer.  The listing and the click-out endpoint have
    completely different limits, so they get completely different pacers."""

    def __init__(self, rps, max_interval=2.0):
        self.lock = threading.Lock()
        self.floor = 1.0 / rps
        self.interval = self.floor
        self.max_interval = max_interval
        self.next_t = 0.0
        self.pause_until = 0.0
        self.n429 = 0

    def wait(self):
        while True:
            with self.lock:
                now = time.time()
                if now < self.pause_until:
                    w = self.pause_until - now
                else:
                    w = self.next_t - now
                    if w <= 0:
                        self.next_t = max(self.next_t + self.interval, now)
                        return
            time.sleep(min(w, 3) if w > 0 else 0)

    def hit_429(self):
        with self.lock:
            self.n429 += 1
            self.pause_until = max(self.pause_until, time.time() + 3)
            self.interval = min(self.interval * 1.3, self.max_interval)

    def slow(self, seconds=3):
        with self.lock:
            self.pause_until = max(self.pause_until, time.time() + seconds)

    def ok(self):
        with self.lock:
            self.interval = max(self.interval * 0.97, self.floor)


LIST_PACE = Pacer(LIST_RPS, max_interval=1.0)
APPLY_PACE = Pacer(RPS, max_interval=2.0)


def head_text(r, cap=STREAM_CAP):
    """First `cap` decoded chars of a streamed response, then hang up."""
    buf, n = [], 0
    try:
        for chunk in r.iter_content(chunk_size=4096, decode_unicode=True):
            if not chunk:
                continue
            if isinstance(chunk, bytes):
                chunk = chunk.decode("utf-8", "replace")
            buf.append(chunk)
            n += len(chunk)
            if n >= cap:
                break
    finally:
        r.close()
    return "".join(buf)


def get(s, url, pace, stream_cap=0, **kw):
    """(response, status) with status 'ok' | '404' | 'transient'.

    A 429/5xx is retried and reported as transient, NEVER as 404/empty — the
    caller must be able to tell "the host pushed back" from "no such job"."""
    last = "unknown"
    for attempt in range(TRIES):
        pace.wait()
        try:
            if stream_cap:
                kw["stream"] = True
            r = s.get(url, timeout=TIMEOUT, **kw)
            if r.status_code == 404:
                r.close()
                return r, "404"
            if r.status_code == 429:
                r.close()
                pace.hit_429()
                last = "HTTP 429"
            elif r.status_code >= 500:
                r.close()
                pace.slow(5)
                last = "HTTP %d" % r.status_code
            elif r.status_code in (200, 301, 302):
                r.head = head_text(r, stream_cap) if stream_cap else ""
                pace.ok()
                return r, "ok"
            else:
                r.close()
                last = "HTTP %d" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))
    return last, "transient"


def token_for(job_id):
    """web3.career's /i/ token: base64 of the numeric job id, then reversed."""
    return base64.b64encode(str(job_id).encode()).decode()[::-1]


def resolve_apply(s, job_id, href):
    """(url, status) with status 'ok' | 'none' | 'transient'."""
    r, st = get(s, "%s/i/%s" % (ROOT, token_for(job_id)), APPLY_PACE,
                headers={"Referer": ROOT + href}, allow_redirects=False,
                stream_cap=STREAM_CAP)
    if st == "transient":
        return "", "transient"
    if st == "404":
        return "", "none"
    # 302 here = the Referer was not accepted and the site bounced us back to
    # its own job page.  Unresolved — never record that as the apply url.
    if r.status_code in (301, 302):
        return "", "none"
    m = PAYLOAD_RX.search(r.head)
    if not m:
        return "", "none"
    path = m.group(2)[::-1]
    if not path.startswith("/z/"):
        return "", "none"
    r2, st2 = get(s, ROOT + path, APPLY_PACE, allow_redirects=False,
                  headers={"Referer": ROOT + href})
    if st2 == "transient":
        return "", "transient"
    loc = r2.headers.get("location") or ""
    if not loc.startswith("http") or loc.startswith(ROOT):
        return "", "none"          # web3.career hosts this application itself
    return loc, "ok"


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
    except (TypeError, ValueError):
        return ""


def page_url(n):
    return ROOT + "/" if n <= 1 else "%s/?page=%d" % (ROOT, n)


_SLUG_RX = re.compile(r"[^a-z0-9]+")
_unmatched = [0]        # rows dropped because no JSON-LD slug matched


def slugify(s):
    return _SLUG_RX.sub("-", unescape(str(s or "")).lower()).strip("-")


def parse_page(html):
    """[(job_id, href, ld, estimated_salary_bool)] for the real rows only.

    Rows and JSON-LD are matched on the slug, NEVER on position — see the
    page-1 trap in the module docstring."""
    rows = ROW_RX.findall(html)
    lds = []
    for blob in LD_RX.findall(html):
        if '"JobPosting"' not in blob:
            continue
        try:
            d = json.loads(blob)
        except ValueError:
            continue
        if isinstance(d, dict) and d.get("@type") == "JobPosting":
            d.pop("description", None)      # 3-8 KB each; never used
            lds.append(d)
    by_slug = {}
    for d in lds:
        org = d.get("hiringOrganization") or {}
        comp = (org.get("name") or "") if isinstance(org, dict) else ""
        t = slugify(d.get("title") or "")
        # the company half of a web3.career slug drops its separators
        # ("OP Labs" -> oplabs, "Sui Foundation" -> suifoundation) while the
        # title half keeps them, so register both spellings
        for c in (slugify(comp), _SLUG_RX.sub("", unescape(comp).lower())):
            if c:
                by_slug.setdefault("%s-%s" % (t, c), d)

    # estimated-salary marker keyed by job id rather than by position, so a
    # stray nested <tr> cannot shift the whole list by one
    est = {}
    for c in html.split("<tr ")[1:]:
        m = re.search(r"data-jobid=(\d+)", c.split(">", 1)[0])
        if m:
            est[m.group(1)] = "estimated_star" in c

    out = []
    for jid, href in rows:
        slug = href.strip("/").rsplit("/", 1)[0].rsplit("/", 1)[-1]
        d = by_slug.get(slug)
        if d is None:
            # long titles get a truncated slug in the href; fall back to the
            # unique JSON-LD whose key starts with the row slug
            cands = [v for k, v in by_slug.items()
                     if k.startswith(slug) or slug.startswith(k)]
            d = cands[0] if len(cands) == 1 else None
        if d is None:
            _unmatched[0] += 1          # drop rather than mispair
            continue
        out.append((jid, href, d, est.get(jid, True)))
    return out


def build_row(job_id, href, ld, estimated, apply_url):
    title = (ld.get("title") or "").strip()
    org = ld.get("hiringOrganization") or {}
    company = (org.get("name") or "").strip() if isinstance(org, dict) else ""

    jl = ld.get("jobLocation")
    addr = (jl.get("address") or {}) if isinstance(jl, dict) else {}
    city = (addr.get("addressLocality") or "").strip()
    country = (addr.get("addressCountry") or "").strip()
    state = (addr.get("addressRegion") or "").strip()
    remote = city.lower() in ("anywhere", "remote", "")
    if remote:
        city = ""
        if country.lower() in ("anywhere", "remote", ""):
            location, country = "Remote", ""
        else:
            location = "Remote, " + country
        work_mode = "Remote"
    else:
        location = ", ".join(x for x in (city, state, country) if x)
        work_mode = cc.work_mode(text=location)

    smin = smax = cur = per = ""
    bs = ld.get("baseSalary")
    if isinstance(bs, dict) and not estimated:
        val = bs.get("value") or {}
        if isinstance(val, dict):
            smin = num(val.get("minValue"))
            smax = num(val.get("maxValue"))
            per = (val.get("unitText") or "").title()
        cur = (bs.get("currency") or "").upper()
        if not (smin or smax):
            cur = per = ""

    # "2026-07-28 20:13:11 +0100" — not ISO-8601, so hand cc.ymd the date part.
    posted = cc.ymd(iso=str(ld.get("datePosted") or "")[:10])

    return cc.row(
        source="web3career", source_type="niche_board",
        company=company, title=title,
        # classify_any, never classify: classify() returns None for "Full-time"
        # and would silently drop ~95% of this board.
        employment_type=cc.classify_any(ld.get("employmentType"), title),
        work_mode=work_mode, location=location, city=city, state=state,
        country=country, remote=remote,
        salary_min=smin, salary_max=smax, salary_currency=cur,
        salary_period=per, posted=posted,
        apply_url=apply_url or (ROOT + href),
        job_id=job_id, board="web3career", board_url=ROOT)


# --------------------------------------------------------------------------
def load_listing():
    """job_id -> (href, ld, estimated) from the append-only raw cache."""
    out = {}
    if not os.path.exists(LISTING_JSONL):
        return out
    with open(LISTING_JSONL, encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                d = json.loads(ln)
            except ValueError:
                continue
            out[d["id"]] = (d["href"], d["ld"], d.get("est", True))
    return out


def phase_listing(s, listing, state, t0):
    """Walk /?page=N and append every unseen posting to the raw cache."""
    page = int(state.get("next_page") or 1)
    n_new = n_pages = 0
    empty_streak = 0
    local = threading.local()

    def sess():
        if not hasattr(local, "s"):
            local.s = cc.session()
            local.s.headers.update(
                {"Accept": "text/html,application/xhtml+xml"})
        return local.s

    def fetch(p):
        # The listing is rate limited too, just far more generously than /i/:
        # 5 workers at 6 rps tripped it at page 84 on 2026-07-29.  get() already
        # retries 3x through the pacer; two extra rounds with a real pause on
        # top of that is what separates "the host pushed back for a moment"
        # from "stop the crawl here", and only the latter ends phase 1.
        for pause in (0, 25, 60):
            if pause:
                time.sleep(pause)
            r, st = get(sess(), page_url(p), LIST_PACE)
            if st == "ok":
                return p, parse_page(r.text)
        return p, None

    with open(LISTING_JSONL, "a", encoding="utf-8") as raw:
        with cf.ThreadPoolExecutor(max_workers=LIST_WORKERS) as ex:
            while page <= HARD_PAGE_CEILING:
                if time.time() - t0 > LIST_SECONDS:
                    print("  phase 1: listing budget reached at page %d"
                          % page, flush=True)
                    break
                if MAX_PAGES and n_pages >= MAX_PAGES:
                    print("  phase 1: page cap reached at page %d" % page,
                          flush=True)
                    break
                batch = list(range(page, page + PAGE_BATCH))
                results = dict(ex.map(fetch, batch))
                stop_at = None
                for p in batch:
                    items = results.get(p)
                    if items is None:
                        # transient: stop the batch HERE so the re-run resumes
                        # at p.  A blip must never be read as end-of-feed.
                        print("    ! page %d transient — stopping so the "
                              "re-run resumes here" % p, flush=True)
                        stop_at = p
                        break
                    fresh = [it for it in items if it[0] not in listing]
                    if not fresh:
                        empty_streak += 1
                        if empty_streak >= 3:
                            print("    end of feed at page %d (%d rows, 0 new "
                                  "ids)" % (p, len(items)), flush=True)
                            stop_at = p + 1
                            break
                        continue
                    empty_streak = 0
                    for jid, href, ld, est in fresh:
                        listing[jid] = (href, ld, est)
                        raw.write(json.dumps(
                            {"id": jid, "href": href, "ld": ld, "est": est},
                            ensure_ascii=False) + "\n")
                        n_new += 1
                    n_pages += 1
                raw.flush()
                if stop_at is not None:
                    page = stop_at
                    break
                page += PAGE_BATCH
                state["next_page"] = page
                cc.save_state(STATE_JSON, state)
                print("    phase 1: page %d | %d cached postings | %.0fs"
                      % (page - 1, len(listing), time.time() - t0), flush=True)
    state["next_page"] = page
    cc.save_state(STATE_JSON, state)
    return n_new, page


def phase_resolve(listing, cache, t0):
    """Fill the apply-url cache for as many unresolved postings as the budget
    allows, NEWEST FIRST (the listing cache is in newest-first page order)."""
    todo = [jid for jid in listing if jid not in cache]
    if not todo:
        return 0, 0
    local = threading.local()

    def sess():
        if not hasattr(local, "s"):
            local.s = cc.session()
            local.s.headers.update(
                {"Accept": "text/html,application/xhtml+xml"})
        return local.s

    done = [0]
    lock = threading.Lock()
    stop = threading.Event()

    def work(jid):
        if stop.is_set():
            return
        href = listing[jid][0]
        url, st = resolve_apply(sess(), jid, href)
        if st == "transient":
            return                      # left OUT of the cache -> retried
        with lock:
            cache[jid] = url
            done[0] += 1
            if done[0] % 200 == 0:
                cc.save_state(CACHE_JSON, cache)
                print("    phase 2: %d/%d resolved | %d 429s | pace %.2fs | "
                      "%.0fs" % (done[0], len(todo), APPLY_PACE.n429,
                                 APPLY_PACE.interval, time.time() - t0),
                      flush=True)
        if time.time() - t0 > MAX_SECONDS:
            stop.set()

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, todo))
    cc.save_state(CACHE_JSON, cache)
    return done[0], len(todo)


def emit(listing, cache):
    """Regenerate contract_norm/web3career.jsonl from the caches (tmp+replace).
    Rewriting rather than appending is what lets a later run upgrade a row's
    apply_url from the web3.career page to the employer's real ATS link."""
    tmp = OUT + ".tmp"
    n = n_ats = 0
    with open(tmp, "w", encoding="utf-8") as f:
        for jid, (href, ld, est) in listing.items():
            rec = build_row(jid, href, ld, est, cache.get(jid, ""))
            f.write(json.dumps(rec) + "\n")
            n += 1
            if not rec["apply_url"].startswith(ROOT):
                n_ats += 1
    os.replace(tmp, OUT)
    return n, n_ats


def main():
    t0 = time.time()
    state = cc.load_state(STATE_JSON)
    cache = cc.load_state(CACHE_JSON)
    listing = load_listing()

    s = cc.session()
    s.headers.update({"Accept": "text/html,application/xhtml+xml"})

    # sanity probe: never record an empty board because of a bad response
    probe, st = get(s, page_url(1), LIST_PACE)
    if st != "ok" or "tableTurboRowClick" not in getattr(probe, "text", ""):
        raise SystemExit("web3.career listing did not answer with job rows "
                         "(%s) — NOT recording an empty board" % st)
    m = re.search(r"has ([\d,]+) jobs", probe.text)
    advertised = int(m.group(1).replace(",", "")) if m else 0
    print("  web3.career advertises %s jobs; %d postings cached, %d apply urls "
          "resolved, resuming at page %s"
          % (advertised or "?", len(listing), len(cache),
             state.get("next_page") or 1), flush=True)

    n_new, page = phase_listing(s, listing, state, t0)
    print("  phase 1 done: +%d postings (%d cached total), next_page=%d, "
          "%d rows dropped as slug-unmatched, %.0fs"
          % (n_new, len(listing), page, _unmatched[0], time.time() - t0),
          flush=True)

    n_res = n_todo = 0
    if RESOLVE:
        n_res, n_todo = phase_resolve(listing, cache, t0)
        print("  phase 2 done: +%d of %d unresolved apply urls, %d 429s, %.0fs"
              % (n_res, n_todo, APPLY_PACE.n429, time.time() - t0), flush=True)

    rows, ats = emit(listing, cache)
    pct = 100.0 * len(listing) / advertised if advertised else 0.0
    cc.save_state(STATE_JSON, {
        "next_page": page, "last_run": time.strftime("%Y-%m-%d %H:%M:%S"),
        "advertised_total": advertised, "postings_cached": len(listing),
        "apply_urls_cached": len(cache), "rows_emitted": rows,
        "rows_with_ats_apply_url": ats, "rate_limited_429": APPLY_PACE.n429,
        "coverage_pct": round(pct, 1),
        "elapsed_sec": round(time.time() - t0, 1)})

    print("\nweb3career: %d rows in %s" % (rows, OUT), flush=True)
    print("  %d of %d advertised postings cached (%.1f%%)"
          % (len(listing), advertised or 0, pct), flush=True)
    print("  apply urls: %d point at the employer's real ATS, %d still point "
          "at the web3.career page" % (ats, rows - ats), flush=True)
    print("  re-run to continue: phase 1 resumes at page %d, phase 2 keeps "
          "filling the apply cache and the output is regenerated in place"
          % page, flush=True)
    print("DONE web3career_fetch (%.0fs)" % (time.time() - t0), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
