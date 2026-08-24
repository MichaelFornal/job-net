#!/usr/bin/env python3
"""
nicheboards_fetch.py — nine small vertical / niche job boards, one JSONL.

All nine write into contract_norm/nicheboards.jsonl, each with its own
`source` value and source_type="niche_board".

  cryptojobslist      cryptojobslist.com        Next.js __NEXT_DATA__, ?page=N
  cryptocurrencyjobs  cryptocurrencyjobs.co     /index.xml RSS + detail page
  vuejobs             vuejobs.com               app.vuejobs.com/feed/posts + detail
  dribbble            dribbble.com/jobs.rss     RSS
  authenticjobs       authenticjobs.com         WP-Job-Manager AJAX + detail page
  pythonorg           python.org/jobs           HTML listing (RSS only shows 20)
  larajobs            larajobs.com/feed         RSS with a custom job: namespace
  elixirjobs          elixirjobs.net/rss        RSS
  mindtheproduct      jobs.mindtheproduct.com   Next.js __NEXT_DATA__

Total live inventory is under 1,000 rows, so the whole run finishes in minutes
and every source runs to completion.  One source failing never kills another:
each handler is wrapped, and its outcome is recorded in
nicheboards_fetch_state.json as "ok" / "error: ..." so a transient failure is
never mistaken for "this board has no jobs".

TRAPS MEASURED ON THESE HOSTS (2026-07-29) — each is defended in the handler
that hit it:

 1. cryptojobslist.com is behind Cloudflare.  /api/jobs, /jobs.rss and /feed
    all answer 403 with a "Just a moment..." interstitial, and so do the
    per-job DETAIL pages.  Only the plain document https://cryptojobslist.com/
    ?page=N answers 200 with the real payload.  Discriminator: a live answer
    contains <script id="__NEXT_DATA__">; the interstitial is ~6 KB of HTML
    with no such tag.  Status alone is useless — never trust it here.

 2. jobs.mindtheproduct.com serves ONE statically generated page for EVERY
    path.  /jobs, /robots.txt, /api/jobs?page=2, /jobs/zzz-not-a-real-path all
    return the byte-identical 253,471-byte document whose __NEXT_DATA__ always
    says page=1.  The page/pageSize params are silently ignored — the textbook
    "paginator that returns page 1 forever".  So only the 20 SSR'd rows of the
    advertised 27 are reachable; the handler asserts page==1 and reports the
    shortfall instead of looping.

 3. authenticjobs.com's RSS is a PERMANENT ARCHIVE, not live inventory.
    ?post_type=job_listing&paged=N keeps returning 10 items down to paged=139
    (~1,390 postings back to early 2021), and those old job pages render
    <li class="listing-expired">Applications have closed</li>.  The LIVE set is
    33 postings, reachable only through the WP-Job-Manager AJAX endpoint
    POST /jm-ajax/get_listings/ (action=job_manager_get_listings), which
    applies the expiry filter.  The RSS is used only as a fallback and is then
    capped to the first few pages.  Separately: the advertised /feed/ without
    post_type returns 10 BLOG POSTS, and paged=1 returns 0 items.

 4. cryptocurrencyjobs.co /feed/ returns HTTP 404 with a 33 KB styled HTML
    body; the working path is /index.xml.  elixirjobs.net /feed and /jobs.rss
    likewise 404 with a 10 KB HTML body; the working path is /rss.

 5. www.python.org/jobs/ is served gzip-encoded regardless of negotiation; a
    client that does not decompress sees zero job links and reads exactly like
    a JS shell.  requests handles this, curl needs --compressed.  The RSS feed
    exposes only the 20 newest of 36, so the HTML listing is the source here.

APPLY URLS.  The pipeline dedupes on a canonical ATS key parsed out of
apply_url, so an aggregator redirect is worth much less than the employer's
own ATS link.  Three of these boards expose the real one and the handlers pay
the extra request to get it:

  vuejobs             detail page carries <a href="...?utm_source=vuejobs.com">
                      -> jobs.ashbyhq.com / job-boards.greenhouse.io /
                         jobs.smartrecruiters.com / *.teamtailor.com ...
  cryptocurrencyjobs  detail page's onclick='trackOutboundLink("Apply: ...")'
                      anchor -> apply.workable.com / jobs.lever.co /
                         job-boards.greenhouse.io / *.bamboohr.com ...
  authenticjobs       detail page's #apply-to-job block carries the employer URL
  mindtheproduct      structured `applicationUrl` field, no extra request

Resolved apply URLs are cached in nicheboards_apply_cache.json keyed by the
listing URL, so an interrupted run resumes almost instantly and a re-run costs
one request per NEW posting.  Only successes are cached — a failed lookup is
retried next time rather than frozen in as "no apply url".

Env:
  NICHE_ONLY=a,b     run only these sources (implies append, no truncate)
  NICHE_RESUME=1     append and skip sources already marked ok today
  (default)          truncate contract_norm/nicheboards.jsonl, run all nine
"""
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import defusedxml.ElementTree as ET  # hardened against XXE / billion-laughs
import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "nicheboards.jsonl")
STATE = os.path.join(BASE, "nicheboards_fetch_state.json")
APPLY_CACHE = os.path.join(BASE, "nicheboards_apply_cache.json")
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

PACE = float(os.environ.get("NICHE_PACE") or 0.6)   # seconds between requests
DC = "{http://purl.org/dc/elements/1.1/}"
LARA = "{https://larajobs.com}"

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR",
}


class Fail(Exception):
    """A transport/validation failure — NOT 'this board has no jobs'."""


# --------------------------------------------------------------------------
# http
# --------------------------------------------------------------------------
class Http:
    """Paced session with 429/5xx backoff and a browser UA.

    A bare python-urllib UA gets 403 from cryptojobslist, dribbble and
    larajobs, so cc.session()'s Chrome UA is mandatory here.  cc.get_json is
    not used because every source in this file except one answers HTML or XML.
    """

    def __init__(self):
        self.s = cc.session()
        self.s.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._next = 0.0

    def _wait(self):
        d = self._next - time.time()
        if d > 0:
            time.sleep(d)
        self._next = time.time() + PACE

    def get(self, url, tries=4, timeout=40, **kw):
        return self._req("GET", url, tries, timeout, **kw)

    def post(self, url, tries=4, timeout=90, **kw):
        return self._req("POST", url, tries, timeout, **kw)

    def _req(self, method, url, tries, timeout, **kw):
        last = None
        for i in range(tries):
            self._wait()
            try:
                r = self.s.request(method, url, timeout=timeout, **kw)
            except requests.RequestException as e:
                last = f"{type(e).__name__}: {e}"
                time.sleep(2 * (i + 1))
                continue
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                # honour Retry-After when the host bothers to send one
                ra = r.headers.get("Retry-After")
                wait = int(ra) if (ra or "").isdigit() else 20 * (i + 1)
                last = "HTTP 429"
                time.sleep(min(wait, 120))
                continue
            if r.status_code in (404, 410) and i == 0:
                # a hard 404 is an answer, not a transport blip — but hand the
                # response back so the caller can inspect the BODY (several of
                # these hosts 404 with a full styled HTML page).
                return r
            last = f"HTTP {r.status_code}"
            time.sleep(2 * (i + 1))
        raise Fail(f"{url} -> {last}")

    def close(self):
        self.s.close()


NEXT_DATA = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.S)


def next_data(r, url):
    """__NEXT_DATA__ payload, or Fail.

    This is the Cloudflare discriminator for cryptojobslist: the "Just a
    moment..." interstitial is a 200/403 HTML document with no __NEXT_DATA__
    tag at all, so "no tag" means blocked, never "no jobs".
    """
    m = NEXT_DATA.search(r.text)
    if not m:
        head = re.sub(r"\s+", " ", r.text[:120])
        raise Fail(f"{url}: HTTP {r.status_code}, {len(r.content)}B, no "
                   f"__NEXT_DATA__ (blocked/shell?) :: {head}")
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        raise Fail(f"{url}: __NEXT_DATA__ not JSON: {e}") from e


def items(r, url, expect=1):
    """RSS <item> list, or Fail.  Validates the BODY, not the status code."""
    if r.status_code != 200:
        head = re.sub(r"\s+", " ", r.text[:120])
        raise Fail(f"{url}: HTTP {r.status_code} ({len(r.content)}B) :: {head}")
    try:
        root = ET.fromstring(r.content)
    except Exception as e:  # noqa: BLE001 — any XML error is a Fail
        head = re.sub(r"\s+", " ", r.text[:120])
        raise Fail(f"{url}: not XML ({type(e).__name__}) :: {head}") from e
    out = root.findall(".//item")
    if len(out) < expect:
        raise Fail(f"{url}: {len(out)} items, expected >= {expect}")
    return out


# --------------------------------------------------------------------------
# small parsers
# --------------------------------------------------------------------------
def txt(s):
    """Strip tags + unescape entities + squeeze whitespace."""
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def slugify(t):
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-",
                                     (t or "").lower())).strip("-")


# Referral / analytics params these boards bolt onto the employer's ATS link.
# They must come off before the row is written: all_jobs_build parses a
# canonical ATS key out of apply_url, and the SAME greenhouse posting reached
# through two boards would otherwise carry two different URLs.  The job's own
# identifying params (ashby_jid, gh_jid, ...) are NOT in this list.
_TRACK = {"utm_source", "utm_medium", "utm_campaign", "utm_content",
          "utm_term", "ref", "source", "gh_src", "sl_source", "igbtracker",
          "trackingid", "refid", "alternatechannel", "src"}


def clean_url(u):
    if not u or not u.startswith("http"):
        return u or ""
    try:
        p = urlsplit(u)
    except ValueError:
        return u
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if k.lower() not in _TRACK]
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q), p.fragment))


_JUNK = re.compile(r"<script|onerror\s*=|javascript:|<img\s+src\s*=", re.I)


def is_junk(*fields):
    """True for injection probes posted as job titles on self-serve boards.

    These strings end up in an XLSX and in generated HTML downstream, so they
    are dropped at the source rather than escaped everywhere later.
    """
    return any(_JUNK.search(str(f or "")) for f in fields)


def geo(loc):
    """(city, state, country) — only when unambiguous, else blanks.

    Deliberately conservative: 'Lancaster, PA' resolves, 'Remote (US
    Preferred, Open to International)' does not.
    """
    s = (loc or "").strip()
    m = re.match(r"^([A-Za-z .'\-]+),\s*([A-Za-z]{2})$", s)
    if m and m.group(2).upper() in US_STATES:
        return m.group(1).strip(), m.group(2).upper(), "United States"
    m = re.match(r"^([A-Za-z .'\-]+),\s*(United States|USA|US|Canada|"
                 r"United Kingdom|UK|Germany|France|Spain|Netherlands|India|"
                 r"Australia|Ireland|Poland|Portugal|Brazil|Mexico|Singapore|"
                 r"Japan|Israel|Sweden|Switzerland|Italy)$", s, re.I)
    if m:
        return m.group(1).strip(), "", m.group(2)
    return "", "", ""


_MONEY = re.compile(r"(?:\$|USD\s*)?([\d][\d,\.]*)\s*([kKmM])?")


def money(tok):
    m = _MONEY.match(tok.strip())
    if not m:
        return ""
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return ""
    mult = {"k": 1e3, "m": 1e6}.get((m.group(2) or "").lower(), 1)
    v *= mult
    if mult == 1 and v < 1000:      # "$120 - $160" is an hourly rate; keep it
        return round(v)
    return round(v)


def salary_range(s):
    """'$110k - $130k + Equity' -> (110000, 130000, 'USD')."""
    s = (s or "").strip()
    if not s:
        return "", "", ""
    cur = "USD" if ("$" in s or "usd" in s.lower()) else (
        "EUR" if "€" in s else ("GBP" if "£" in s else ""))
    parts = re.split(r"\s*(?:-|–|—|to)\s*", s, maxsplit=1)
    lo = money(parts[0]) if parts else ""
    hi = money(parts[1]) if len(parts) > 1 else ""
    return lo, hi, cur


def num(v):
    try:
        f = float(v)
        return round(f) if f else ""
    except (TypeError, ValueError):
        return ""


# --------------------------------------------------------------------------
# apply-url cache (only successes are cached)
# --------------------------------------------------------------------------
_cache = {}
_cache_dirty = [0]


def cache_load():
    _cache.update(cc.load_state(APPLY_CACHE))


def cache_save(force=False):
    if _cache_dirty[0] and (force or _cache_dirty[0] % 25 == 0):
        cc.save_state(APPLY_CACHE, _cache)


def resolve_apply(h, key, url, rx, ctx=None):
    """Fetch a detail page once and pull the outbound employer apply link.

    Returns (apply_url, status) where status is "cache" / "hit" / "miss" /
    "err".  A miss (page loaded, no outbound link — e.g. the posting applies by
    email) is cached as "" so it is not re-fetched; an ERROR is not cached.
    """
    if key in _cache:
        return clean_url(_cache[key] or ""), "cache"
    try:
        r = h.get(url, tries=2, timeout=30)
    except Fail:
        return "", "err"
    if r.status_code != 200 or len(r.content) < 500:
        return "", "err"
    body = r.text
    m = rx.search(body)
    got = ""
    if m:
        got = html.unescape(m.group(1))
    elif ctx:
        m = ctx.search(body)
        if m:
            got = html.unescape(m.group(1))
    got = clean_url(got)
    _cache[key] = got
    _cache_dirty[0] += 1
    cache_save()
    return got, ("hit" if got else "miss")


# --------------------------------------------------------------------------
# 1. cryptojobslist.com
# --------------------------------------------------------------------------
# Only these tags are employment types; the rest are skills/seniority/chains.
# Feeding the raw tag list to classify_any would turn a "smart-contract" tag
# into employment_type="Contract".
CJL_TYPE_TAGS = {"full-time", "part-time", "contract", "freelance",
                 "internship", "intern", "temporary"}


def do_cryptojobslist(h, out):
    n = 0
    seen = set()
    page = 1
    total = None
    pages = None
    while page <= (pages or 40):
        url = f"https://cryptojobslist.com/?page={page}"
        d = next_data(h.get(url), url)
        pp = d.get("props", {}).get("pageProps", {})
        meta = pp.get("meta") or {}
        if total is None:
            total = meta.get("totalCount")
            pages = meta.get("totalPages") or 40
            print(f"    cryptojobslist totalCount={total} totalPages={pages}",
                  flush=True)
        jobs = pp.get("jobs") or []
        if not jobs:
            break
        fresh = 0
        for j in jobs:
            jid = str(j.get("id") or j.get("seoSlug") or "")
            if not jid or jid in seen:
                continue
            seen.add(jid)
            fresh += 1
            if j.get("filled") or j.get("isActive") is False:
                continue
            tags = [str(t).lower() for t in (j.get("tags") or [])]
            raw = " ".join(t for t in tags if t in CJL_TYPE_TAGS)
            est = (j.get("estimatedSalary") or {}).get("value") or {}
            loc = j.get("jobLocation") or ""
            rem = bool(j.get("remote"))
            city, st, ctry = geo(loc)
            slug = j.get("seoSlug") or ""
            out.write(cc.row(
                source="cryptojobslist", source_type="niche_board",
                company=j.get("companyName"), title=j.get("jobTitle"),
                employment_type=cc.classify_any(raw, j.get("jobTitle")),
                work_mode="Remote" if rem else "", remote=rem,
                location=loc or ("Remote" if rem else ""),
                city=city, state=st, country=ctry,
                salary_min=num(est.get("minValue")),
                salary_max=num(est.get("maxValue")),
                salary_currency=(j.get("estimatedSalary") or {}).get(
                    "currency") or "",
                salary_period=(est.get("unitText") or "").title(),
                posted=cc.ymd(iso=j.get("publishedAt")),
                apply_url=(f"https://cryptojobslist.com/jobs/{slug}"
                           if slug else url),
                job_id=jid, board="cryptojobslist",
                board_url="https://cryptojobslist.com"))
            n += 1
        # A page whose ids we have all seen means the ?page= param stopped
        # advancing — bail rather than spin (the 300k-rows-that-dedupe-to-98
        # failure mode).
        if fresh == 0:
            print("    cryptojobslist: page param stopped advancing at "
                  f"page={page}", flush=True)
            break
        page += 1
    print(f"  cryptojobslist: {n} rows / {len(seen)} ids "
          f"(advertised {total})", flush=True)
    return n


# --------------------------------------------------------------------------
# 2. cryptocurrencyjobs.co
# --------------------------------------------------------------------------
# The outbound "Apply now" anchor.  Anchoring on the tracking attribute rather
# than on a ref= querystring is the whole difference between 11 and 62 real ATS
# links: the referral param is not one param but four seen in the wild
# (?ref=, ?source=, ?gh_src=, and none at all), and 51 of 62 postings were
# silently scored "email only" while their greenhouse/bamboohr link sat right
# there.  Postings that really do apply by email have a relative
# /cdn-cgi/l/email-protection# href, which https?:// correctly refuses.
CCJ_APPLY = re.compile(
    r'href="(https?://[^"]+)"\s+onclick=\'trackOutboundLink\("Apply:')
CCJ_APPLY_ALT = re.compile(
    r'href="(https?://[^"]*(?:ref|source|gh_src)=cryptocurrencyjobs\.co[^"]*)"')
CCJ_TYPE = re.compile(
    r"This is an?\s+([a-z\- ]+?)\s+(?:position|role|job)", re.I)
CCJ_LOC = re.compile(
    r"can be done\s+(remotely[^.]*|from[^.]*)\.|"
    r"is located in\s+([^.]+)\.|"
    r"position\s+(?:that\s+)?(?:is\s+)?(?:based\s+)?in\s+([^.]+)\.", re.I)


def do_cryptocurrencyjobs(h, out):
    # /feed/ is a 404 with a 33KB styled HTML body; /index.xml is the feed.
    url = "https://cryptocurrencyjobs.co/index.xml"
    its = items(h.get(url), url, expect=5)
    n = hit = miss = err = 0
    for it in its:
        link = (it.findtext("link") or it.findtext("guid") or "").strip()
        if not link:
            continue
        title = txt(it.findtext("title"))
        # "<Role> at <Company>" is the only place the company appears.
        role, _, comp = title.rpartition(" at ")
        if not role:
            role, comp = title, ""
        desc = txt(it.findtext("description"))
        mt = CCJ_TYPE.search(desc)
        ml = CCJ_LOC.search(desc)
        loc = ""
        if ml:
            loc = txt(ml.group(1) or ml.group(2) or ml.group(3) or "")
        rem = "remote" in (loc + " " + desc).lower()
        apply_url, status = resolve_apply(
            h, link, link, CCJ_APPLY, ctx=CCJ_APPLY_ALT)
        # count the OUTCOME, not the fetch: a cached row is still a hit
        hit += bool(apply_url)
        miss += not apply_url and status != "err"
        err += status == "err"
        city, st, ctry = geo(loc)
        out.write(cc.row(
            source="cryptocurrencyjobs", source_type="niche_board",
            company=comp.strip(), title=role.strip(),
            employment_type=cc.classify_any(mt.group(1) if mt else "", role),
            work_mode="Remote" if rem else "", remote=rem,
            location=loc or ("Remote" if rem else ""),
            city=city, state=st, country=ctry,
            posted=cc.ymd(iso=_rfc(it.findtext("pubDate"))),
            apply_url=apply_url or link, job_id=link,
            board="cryptocurrencyjobs",
            board_url="https://cryptocurrencyjobs.co"))
        n += 1
    cache_save(force=True)
    print(f"  cryptocurrencyjobs: {n} (real ATS apply url: {hit}, "
          f"email-only: {miss}, lookup failed: {err})", flush=True)
    return n


# --------------------------------------------------------------------------
# 3. vuejobs.com
# --------------------------------------------------------------------------
VJ_APPLY = re.compile(
    r'href="(https?://[^"]*(?:utm_source=vuejobs\.com|ref=vuejobs\.com)[^"]*)"')
VJ_EMP = re.compile(r"<strong>Employer:</strong>\s*(.+?)</p>", re.S)
VJ_LOC = re.compile(r"<strong>Location:</strong>\s*(.+?)</p>", re.S)


def do_vuejobs(h, out):
    # vuejobs.com/feed 302s to app.vuejobs.com/feed/posts — hardcode the target
    # to skip the hop.  One request returns all ~279 items.
    url = "https://app.vuejobs.com/feed/posts"
    its = items(h.get(url, timeout=60), url, expect=20)
    n = hit = miss = err = 0
    for it in its:
        link = (it.findtext("guid") or it.findtext("link") or "").strip()
        link = link.split("?")[0]          # drop ?utm_source=rssfeed
        if not link:
            continue
        raw = it.findtext("description") or ""
        me, ml = VJ_EMP.search(raw), VJ_LOC.search(raw)
        comp = txt(me.group(1)) if me else ""
        loc = txt(ml.group(1)) if ml else ""
        title = txt(it.findtext("title"))
        rem = "remote" in (loc + " " + title).lower()
        apply_url, status = resolve_apply(h, link, link, VJ_APPLY)
        # count the OUTCOME, not the fetch: a cached row is still a hit
        hit += bool(apply_url)
        miss += not apply_url and status != "err"
        err += status == "err"
        city, st, ctry = geo(loc)
        out.write(cc.row(
            source="vuejobs", source_type="niche_board",
            company=comp, title=title,
            employment_type=cc.classify_any("", title),
            work_mode="Remote" if rem else "", remote=rem,
            location=loc, city=city, state=st, country=ctry,
            posted=cc.ymd(iso=_rfc(it.findtext("pubDate"))),
            apply_url=apply_url or link, job_id=link, board="vuejobs",
            board_url="https://vuejobs.com/jobs"))
        n += 1
    cache_save(force=True)
    print(f"  vuejobs: {n} (real ATS apply url: {hit}, none found: {miss}, "
          f"lookup failed: {err})", flush=True)
    return n


# --------------------------------------------------------------------------
# 4. dribbble.com/jobs
# --------------------------------------------------------------------------
# Rigid title template: "<Company> is hiring for a position of <Title> in <Loc>"
DR_TITLE = re.compile(r"^(.*?) is hiring for a position of (.*?) in (.*)$")


def do_dribbble(h, out):
    url = "https://dribbble.com/jobs.rss"
    its = items(h.get(url), url, expect=10)
    n = 0
    for it in its:
        raw = txt(it.findtext("title"))
        m = DR_TITLE.match(raw)
        creator = txt(it.findtext(DC + "creator"))
        if m:
            comp, title, loc = m.group(1), m.group(2), m.group(3)
        else:
            comp, title, loc = creator, raw, ""
        comp = creator or comp     # dc:creator independently confirms company
        link = (it.findtext("link") or "").split("?")[0]
        rem = "remote" in loc.lower()
        city, st, ctry = geo(loc)
        out.write(cc.row(
            source="dribbble", source_type="niche_board",
            company=comp, title=title,
            employment_type=cc.classify_any("", title),
            work_mode="Remote" if rem else "", remote=rem,
            location=loc, city=city, state=st, country=ctry,
            # STALE FEED: pubDates run from 2022 to today, so this date is the
            # only thing keeping dead listings out of the fresh cuts downstream.
            posted=cc.ymd(iso=_rfc(it.findtext("pubDate"))),
            apply_url=link, job_id=link, board="dribbble",
            board_url="https://dribbble.com/jobs"))
        n += 1
    print(f"  dribbble: {n}", flush=True)
    return n


# --------------------------------------------------------------------------
# 5. authenticjobs.com
# --------------------------------------------------------------------------
AJ_LI = re.compile(r'<li class="(post-\d+ job_listing[^"]*)"(.*?)</li>\s*</ul>',
                   re.S)
AJ_HREF = re.compile(r'<a href="(https://authenticjobs\.com/job/[^"]+)"')
AJ_H2 = re.compile(r"<h2>(.*?)</h2>", re.S)
AJ_COMP = re.compile(r'<div class="company">\s*<strong>(.*?)</strong>', re.S)
AJ_LOC = re.compile(r'<div class="location">\s*(.*?)\s*</div>', re.S)
AJ_TYPE = re.compile(r'<li class="job-type ([a-z\- ]+)"', re.S)
AJ_DATE = re.compile(r'<time datetime="([0-9\-]+)"')
# the employer's own URL, inside the detail page's #apply-to-job block
AJ_APPLY = re.compile(
    r'id="apply-to-job".{0,2000}?<div class="application_details">.*?'
    r'href="(https?://[^"]+)"', re.S)


def do_authenticjobs(h, out):
    # WP-Job-Manager AJAX = the LIVE set (expiry filter applied).  The RSS is
    # an archive back to 2021 whose old pages render "Applications have
    # closed", so it is only a fallback and is capped when used.
    rows = []
    try:
        r = h.post("https://authenticjobs.com/jm-ajax/get_listings/",
                   data={"action": "job_manager_get_listings",
                         "per_page": 500, "page": 1,
                         "orderby": "date", "order": "DESC"},
                   headers={"X-Requested-With": "XMLHttpRequest",
                            "Referer": "https://authenticjobs.com/"})
        d = r.json()
        if not d.get("found_jobs"):
            raise Fail("jm-ajax found_jobs=false")
        body = d.get("html") or ""
        for m in AJ_LI.finditer(body):
            cls, blk = m.group(1), m.group(2)
            hm = AJ_HREF.search(blk)
            if not hm:
                continue
            t = AJ_H2.search(blk)
            c = AJ_COMP.search(blk)
            lo = AJ_LOC.search(blk)
            ty = AJ_TYPE.search(blk)
            da = AJ_DATE.search(blk)
            rows.append({
                "url": hm.group(1), "title": txt(t.group(1)) if t else "",
                "company": txt(c.group(1)) if c else "",
                "location": txt(lo.group(1)) if lo else "",
                "type": (ty.group(1) if ty else "").replace("-", " "),
                "posted": da.group(1) if da else "",
                "id": re.sub(r"\D", "", cls.split()[0]),
            })
    except (Fail, ValueError, KeyError) as e:
        print(f"    ! authenticjobs jm-ajax failed ({e}); "
              f"falling back to the RSS archive (capped at 3 pages)",
              flush=True)
        for p in range(1, 4):
            u = "https://authenticjobs.com/feed/?post_type=job_listing"
            if p > 1:                      # paged=1 returns 0 items
                u += f"&paged={p}"
            for it in items(h.get(u), u, expect=1):
                link = (it.findtext("link") or "").strip()
                rows.append({
                    "url": link, "title": txt(it.findtext("title")),
                    "company": "", "location": "", "type": "",
                    "posted": _rfc(it.findtext("pubDate")) or "",
                    "id": re.sub(r"\D", "", link.rstrip("/").split("/")[-2]
                                 if "/job/" in link else ""),
                })
    n = hit = miss = err = 0
    for j in rows:
        comp = j["company"]
        if not comp:
            # /job/36541/hope-international-associate-digital-designer/
            m = re.match(r"https://authenticjobs\.com/job/\d+/(.+?)/?$",
                         j["url"])
            if m and j["title"]:
                s = m.group(1)
                ts = slugify(j["title"])
                comp = s[:-len(ts)].strip("-") if s.endswith(ts) else s
                comp = comp.replace("-", " ").title()
        apply_url, status = resolve_apply(h, j["url"], j["url"], AJ_APPLY)
        # count the OUTCOME, not the fetch: a cached row is still a hit
        hit += bool(apply_url)
        miss += not apply_url and status != "err"
        err += status == "err"
        loc = j["location"]
        rem = "remote" in (loc + " " + j["title"]).lower()
        city, st, ctry = geo(loc)
        out.write(cc.row(
            source="authenticjobs", source_type="niche_board",
            company=comp, title=j["title"],
            employment_type=cc.classify_any(j["type"], j["title"]),
            work_mode="Remote" if rem else "", remote=rem,
            location=loc, city=city, state=st, country=ctry,
            posted=cc.ymd(iso=j["posted"]) if j["posted"] else "",
            apply_url=apply_url or j["url"], job_id=j["id"] or j["url"],
            board="authenticjobs", board_url="https://authenticjobs.com"))
        n += 1
    cache_save(force=True)
    print(f"  authenticjobs: {n} live (employer apply url: {hit}, "
          f"none found: {miss}, lookup failed: {err})", flush=True)
    return n


# --------------------------------------------------------------------------
# 6. python.org/jobs
# --------------------------------------------------------------------------
PY_LI = re.compile(
    r'<h2 class="listing-company">(.*?)</h2>(.*?)(?=<li>|</ul>)', re.S)
PY_A = re.compile(r'<a href="/jobs/(\d+)/">(.*?)</a>\s*<br/?>\s*(.*?)\s*</span>',
                  re.S)
PY_LOC = re.compile(r'<span class="listing-location"><a[^>]*>(.*?)</a>', re.S)
PY_TIME = re.compile(r'<time datetime="([0-9\-]+)')
PY_COUNT = re.compile(r"([\d,]+)\s+jobs?\b")


def do_pythonorg(h, out):
    # The RSS shows only the 20 newest of 36; the HTML listing has all of them
    # over 2 pages.  The body is gzip-encoded unconditionally — requests
    # decompresses, a raw socket / plain curl does not and sees zero links.
    n = 0
    seen = set()
    total = None
    page = 1
    while page <= 20:
        url = f"https://www.python.org/jobs/?page={page}"
        r = h.get(url)
        if r.status_code != 200:
            break
        body = r.text
        if 'class="listing-company"' not in body:
            if page == 1:
                raise Fail(f"{url}: no listing markup in {len(r.content)}B "
                           f"(gzip not decoded, or layout changed)")
            break
        if total is None:
            m = PY_COUNT.search(body)
            total = m.group(1) if m else "?"
        fresh = 0
        for m in PY_LI.finditer(body):
            head, tail = m.group(1), m.group(2)
            a = PY_A.search(head)
            if not a:
                continue
            jid, title, comp = a.group(1), txt(a.group(2)), txt(a.group(3))
            if jid in seen:
                continue
            seen.add(jid)
            fresh += 1
            lm = PY_LOC.search(head)
            loc = txt(lm.group(1)) if lm else ""
            tm = PY_TIME.search(tail)
            rem = bool(re.search(r"remote|telecommut", (loc + " " + title),
                                 re.I))
            city, st, ctry = geo(loc)
            out.write(cc.row(
                source="pythonorg", source_type="niche_board",
                company=comp, title=title,
                employment_type=cc.classify_any("", title),
                work_mode="Remote" if rem else "", remote=rem,
                location=loc, city=city, state=st, country=ctry,
                posted=cc.ymd(iso=tm.group(1)) if tm else "",
                apply_url=f"https://www.python.org/jobs/{jid}/",
                job_id=jid, board="pythonorg",
                board_url="https://www.python.org/jobs/"))
            n += 1
        if fresh == 0:
            break
        page += 1
    print(f"  pythonorg: {n} (site says {total} jobs)", flush=True)
    return n


# --------------------------------------------------------------------------
# 7. larajobs.com
# --------------------------------------------------------------------------
def do_larajobs(h, out):
    # Richest schema of the batch: a custom job: namespace with dedicated
    # company / location / job_type / salary elements.  Hard 10-item ceiling
    # with no paging, so this only accretes if polled daily.
    url = "https://larajobs.com/feed"
    its = items(h.get(url), url, expect=3)
    n = 0
    for it in its:
        link = (it.findtext("link") or it.findtext("guid") or "").strip()
        loc = txt(it.findtext(LARA + "location"))
        comp = txt(it.findtext(LARA + "company")) or txt(it.findtext(
            DC + "creator"))
        jt = txt(it.findtext(LARA + "job_type")).replace("_", " ")
        lo, hi, cur = salary_range(txt(it.findtext(LARA + "salary")))
        title = txt(it.findtext("title"))
        rem = "remote" in loc.lower()
        city, st, ctry = geo(loc)
        out.write(cc.row(
            source="larajobs", source_type="niche_board",
            company=comp, title=title,
            employment_type=cc.classify_any(jt, title),
            work_mode="Remote" if rem else "", remote=rem,
            location=loc, city=city, state=st, country=ctry,
            salary_min=lo, salary_max=hi, salary_currency=cur,
            salary_period="Year" if (lo or 0) and lo >= 10000 else "",
            posted=cc.ymd(iso=_rfc(it.findtext("pubDate"))),
            apply_url=link, job_id=link, board="larajobs",
            board_url="https://larajobs.com"))
        n += 1
    print(f"  larajobs: {n}", flush=True)
    return n


# --------------------------------------------------------------------------
# 8. elixirjobs.net
# --------------------------------------------------------------------------
EJ_PLACE = re.compile(r"Job place:\s*([^<\n]+)", re.I)
EJ_TYPE = re.compile(r"Job type:\s*([^<\n]+)", re.I)
EJ_LOC = re.compile(r"Job location:\s*([^<\n]+)", re.I)


def do_elixirjobs(h, out):
    # /feed and /jobs.rss 404 with a 10KB styled HTML body; /rss is the feed.
    url = "https://elixirjobs.net/rss"
    its = items(h.get(url), url, expect=3)
    n = 0
    for it in its:
        link = (it.findtext("link") or it.findtext("guid") or "").strip()
        title = txt(it.findtext("title"))
        desc = it.findtext("description") or ""
        mp = EJ_PLACE.search(desc)
        mt = EJ_TYPE.search(desc)
        ml = EJ_LOC.search(desc)
        place = txt(mp.group(1)) if mp else ""
        jt = txt(mt.group(1)) if mt else ""
        loc = txt(ml.group(1)) if ml else place
        # Company is NOT a field anywhere in the feed.  The offer slug is
        # "<company-slug>-<title-slug>-<8 hex>", so stripping the slugified
        # title and the hash off the tail recovers it exactly.
        slug = link.rstrip("/").rsplit("/", 1)[-1]
        slug = re.sub(r"-[0-9a-f]{8}$", "", slug)
        ts = slugify(title)
        comp = slug[:-len(ts)].strip("-") if ts and slug.endswith(ts) else slug
        rem = place.strip().lower() == "remote" or "remote" in loc.lower()
        city, st, ctry = geo(loc)
        out.write(cc.row(
            source="elixirjobs", source_type="niche_board",
            company=comp.replace("-", " ").title(), title=title,
            employment_type=cc.classify_any(jt, title),
            work_mode="Remote" if rem else ("Hybrid" if place.strip().lower()
                                            == "both" else ""),
            remote=rem, location=loc, city=city, state=st, country=ctry,
            posted=cc.ymd(iso=_rfc(it.findtext("pubDate"))),
            apply_url=link, job_id=link, board="elixirjobs",
            board_url="https://elixirjobs.net"))
        n += 1
    print(f"  elixirjobs: {n}", flush=True)
    return n


# --------------------------------------------------------------------------
# 9. jobs.mindtheproduct.com
# --------------------------------------------------------------------------
def do_mindtheproduct(h, out):
    url = "https://jobs.mindtheproduct.com/jobs"
    d = next_data(h.get(url), url)
    ij = (d.get("props", {}).get("pageProps", {}) or {}).get("initialJobs")
    if not isinstance(ij, dict) or not ij.get("jobs"):
        raise Fail(f"{url}: no initialJobs.jobs in __NEXT_DATA__")
    jobs = ij["jobs"]
    total = ij.get("total")
    # The page/pageSize params are IGNORED — every path on this host returns
    # the same static document with page=1 (verified: /jobs?page=2,
    # /api/jobs?page=2, /_next/data/<buildId>/jobs.json?page=2 and even
    # /robots.txt all return the identical 253,471-byte body).  Looping would
    # re-collect page 1 forever, so the walk asserts and stops.
    if ij.get("page") != 1 or len(jobs) < total:
        print(f"    mindtheproduct: {len(jobs)} of {total} reachable "
              f"(page param ignored by the host; the rest need a client-side "
              f"API that is not exposed server-side)", flush=True)
    n = junk = 0
    for j in jobs:
        # applicationUrl is a REAL employer link (ashby / pinpoint / workday /
        # company careers page), which is exactly what the canonical-ATS
        # dedupe wants; contactEmail is the fallback for email-only postings.
        apply_url = clean_url(j.get("applicationUrl") or "")
        if not apply_url and j.get("contactEmail"):
            apply_url = "mailto:" + j["contactEmail"]
        # This board is self-serve and unmoderated: it carries seeded demo rows
        # ("DataViz Inc") and at least one live XSS probe as a job title.  A
        # posting with NO apply url and NO contact email cannot be applied to,
        # which is the objective discriminator for both.
        if not apply_url or is_junk(j.get("title"), j.get("company")):
            junk += 1
            continue
        # Structured `remote` wins, but a posting whose location is literally
        # "Remote" with remote:false is a poster typo, not a signal.
        rem = bool(j.get("remote")) or (j.get("location") or "").strip().lower(
        ) in ("remote", "remote, worldwide", "anywhere")
        loc = j.get("location") or ""
        city, st, ctry = geo(loc)
        slug = j.get("slug") or ""
        out.write(cc.row(
            source="mindtheproduct", source_type="niche_board",
            company=j.get("company"), title=j.get("title"),
            employment_type=cc.classify_any(j.get("type"), j.get("title")),
            work_mode="Remote" if rem else "", remote=rem,
            location=loc or ("Remote" if rem else ""),
            city=city, state=st, country=ctry,
            salary_min=num(j.get("salaryMin")),
            salary_max=num(j.get("salaryMax")),
            salary_currency=(j.get("salaryCurrency") or "").upper(),
            salary_period="Year" if num(j.get("salaryMin")) else "",
            posted=cc.ymd(iso=j.get("publishedAt")),
            apply_url=apply_url, job_id=j.get("id") or slug,
            board="mindtheproduct",
            board_url="https://jobs.mindtheproduct.com/jobs"))
        n += 1
    print(f"  mindtheproduct: {n} (board total {total}, {junk} unapplyable "
          f"demo/spam rows dropped)", flush=True)
    return n


# --------------------------------------------------------------------------
def _rfc(s):
    """RFC-822 pubDate -> ISO, so cc.ymd can read it."""
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y"):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    return ""


SOURCES = {
    "cryptojobslist": do_cryptojobslist,
    "cryptocurrencyjobs": do_cryptocurrencyjobs,
    "vuejobs": do_vuejobs,
    "dribbble": do_dribbble,
    "authenticjobs": do_authenticjobs,
    "pythonorg": do_pythonorg,
    "larajobs": do_larajobs,
    "elixirjobs": do_elixirjobs,
    "mindtheproduct": do_mindtheproduct,
}


def main():
    only = [x for x in os.environ.get("NICHE_ONLY", "").split(",") if x]
    for name in only:
        if name not in SOURCES:
            raise SystemExit(f"unknown source {name!r} (choose from "
                             f"{', '.join(SOURCES)})")
    # RESUME IS THE DEFAULT.  It used to be the opposite: a bare run deleted
    # OUT and re-crawled all nine sources, on the theory that truncating stops
    # a retry duplicating rows.  That theory assumes every source still
    # answers.  On 2026-08-14 cryptojobslist began returning HTTP 403 and
    # vuejobs 404, and a bare run replaced 907 rows with 223 — a permanent
    # loss, because nothing here keeps a .prev.  The state file had already
    # recorded `cryptojobslist -> error ... HTTP 403`; nothing read it.
    #
    # Worse under supervision: this exits `1 if failed`, so at run_all's
    # FETCH_TRIES=40 a permanently-403 source meant forty sequential
    # os.remove()s.  Duplicate rows are collapsed downstream by
    # bh_build_combined on (source, board, job_id) and by norm_dedupe; a
    # deleted row is gone.  Prefer the recoverable failure.
    fresh = os.environ.get("NICHE_FRESH") == "1"
    resume = not fresh          # NICHE_RESUME=1 kept as a no-op alias
    st = cc.load_state(STATE) if (resume or only) else {}
    if not only and fresh and os.path.exists(OUT):
        # Even here, never os.remove() the live file: write beside it and
        # os.replace at the end, so a crash mid-run cannot leave a short file
        # at the canonical path.
        os.replace(OUT, OUT + ".prev.jsonl")
    cache_load()

    h = Http()
    out = cc.Writer(OUT)
    failed = []
    for name, fn in SOURCES.items():
        if only and name not in only:
            continue
        prev = st.get(name) or {}
        if resume and prev.get("status") == "ok" and prev.get("day") == TODAY:
            print(f"  {name}: skip (done {prev.get('rows')} rows today)",
                  flush=True)
            continue
        t0 = time.time()
        try:
            n = fn(h, out)
            st[name] = {"status": "ok", "rows": n, "day": TODAY,
                        "sec": round(time.time() - t0, 1)}
        except Exception as e:  # noqa: BLE001 — one dead board is not fatal
            # Recorded as an ERROR, never as "0 jobs": all_jobs_build must not
            # read a 403 or a timeout as a board that emptied out.
            msg = f"{type(e).__name__}: {e}"[:300]
            st[name] = {"status": "error", "rows": 0, "day": TODAY,
                        "error": msg, "sec": round(time.time() - t0, 1)}
            failed.append(name)
            print(f"  {name}: ERROR {msg}", flush=True)
        cc.save_state(STATE, st)
    out.close()
    cache_save(force=True)
    cc.save_state(STATE, st)
    h.close()

    total = sum(v.get("rows", 0) for v in st.values()
                if v.get("day") == TODAY)
    print(f"DONE nicheboards_fetch: {total} rows -> {OUT}"
          + (f" | FAILED: {', '.join(failed)}" if failed else ""), flush=True)
    # Non-zero exit makes a partial run visible to the supervisor instead of
    # letting it pass as a clean success.
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
