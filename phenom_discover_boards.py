#!/usr/bin/env python3
"""
phenom_discover_boards.py — mine + PROVE the Phenom People career-site tenants
that already appear in all_jobs_master.csv, and write the board list
phenom_fetch.py runs on.

  python3 phenom_discover_boards.py                 # mine, then prove
  python3 phenom_discover_boards.py --mine          # rebuild the candidate pool
  python3 phenom_discover_boards.py --prove         # probe pending candidates
  PHENOM_DISC_LIMIT=25 python3 phenom_discover_boards.py --prove   # smoke test

WHY A DISCOVERY PASS AT ALL.  phenom_fetch.py shipped with six hard-coded
Allegis staffing hosts.  Phenom is not a staffing platform, it is a
career-site platform: the corpus carries ~200 hosts whose apply URLs have the
Phenom path shape, and every one of them is an EMPLOYER-DIRECT board — exactly
the rows the aggregator-heavy end of the master is short of.

WHY A PATH FINGERPRINT IS NOT PROOF.  The obvious tell is the URL shape
  https://careers.pnc.com/global/en/job/PNC1GLOBALR226286/data-scientist
but "/{cc}/{lang}/job/" also matches gh.wd1.myworkdayjobs.com/gh/job/... , and
three of the five hosts a human eyeballed as Phenom are not:

  jobs.bd.com          -> Radancy/TalentBrew  (tbcdn.talentbrew.com, Kestrel)
  jobs.walgreens.com   -> Radancy/TalentBrew
  jobs.iqvia.com       -> neither

measured 2026-08-20.  So the fingerprint is only used to WIDEN the candidate
pool.  What earns a host a place in tenants/phenom.csv is a live response from
the platform's own unauthenticated widgets API:

  POST https://{host}/widgets
  {"ddoKey":"refineSearch", ...}
  -> {"refineSearch":{"status":200,"hits":N,"totalHits":T,"data":{"jobs":[...]}}}

That is the same call the fetcher makes, so "proved" here means "harvestable
there" — never a guess about branding.

A 200 IS NOT PROOF EITHER.  A Phenom career site is a single-page app behind
wildcard routing: every one of these answers HTTP 200 with an HTML shell on a
path that does not exist ...

  jobs.bd.com/us/en/widgets            200  text/html   (not Phenom at all)
  careers.unitedhealthgroup.com/api/widgets  200 text/html

... so the verdict is taken from the PAYLOAD SHAPE (a dict under
"refineSearch" carrying status/totalHits/data), never from the status code.

Outputs
  phenom_candidates.csv   every candidate + its verdict, for auditing
  tenants/phenom.csv      the proved boards, the fetcher's input
  phenom_discover_state.json  resumable; transient failures are NOT recorded,
                          so an unreachable host stays PENDING and is retried.
"""
import argparse
import collections
import csv
import json
import os
import html
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
MASTER = os.environ.get("PHENOM_MASTER",
                        os.path.join(BASE, "all_jobs_master.csv"))
CAND_CSV = os.path.join(BASE, "phenom_candidates.csv")
TENANT_CSV = os.path.join(BASE, "tenants", "phenom.csv")
STATE = os.path.join(BASE, "phenom_discover_state.json")

RPS = float(os.environ.get("PHENOM_DISC_RPS", "12"))
WORKERS = int(os.environ.get("PHENOM_DISC_WORKERS", "40"))
LIMIT = int(os.environ.get("PHENOM_DISC_LIMIT", "0"))
# Split connect/read.  A discovery sweep is 3.7k DIFFERENT hosts, most of
# which are not Phenom and a good number of which answer nothing at all; the
# run is bound by how long a dead host is allowed to hang, not by rate.
# Measured 2026-08-20: 12 workers x 20s x 3 tries walked ~20 hosts/min, an
# ETA of ~3h for the sweep.  The requests here are one POST per host, so the
# aggregate rate is high while the PER-HOST rate stays at one hit.
TIMEOUT = (float(os.environ.get("PHENOM_DISC_CONNECT", "6")),
           float(os.environ.get("PHENOM_DISC_TIMEOUT", "12")))
TRIES = int(os.environ.get("PHENOM_DISC_TRIES", "2"))

# ---- candidate mining ------------------------------------------------------
# Apexes that are somebody else's platform.  A host under one of these is
# never a Phenom career site, and probing 3k of them is wasted requests.
KNOWN_APEX = (
    "myworkdayjobs.com", "workdayjobs.com", "icims.com", "greenhouse.io",
    "lever.co", "ashbyhq.com", "smartrecruiters.com", "taleo.net",
    "oraclecloud.com", "eightfold.ai", "getro.com", "workable.com",
    "breezy.hr", "bamboohr.com", "recruitee.com", "jobvite.com", "comeet.com",
    "personio.de", "personio.com", "rippling.com", "gupy.io", "csod.com",
    "successfactors.com", "sapsf.com", "brassring.com", "linkedin.com",
    "indeed.com", "glassdoor.com", "ziprecruiter.com", "monster.com",
    "dice.com", "careerbuilder.com", "simplyhired.com", "usajobs.gov",
    "governmentjobs.com", "careers-page.com", "notion.site",
    "teamtailor.com", "pinpointhq.com", "applytojob.com", "paylocity.com",
    "adp.com", "dayforcehcm.com", "ultipro.com", "paycomonline.net",
    "jazzhr.com", "clearcompany.com", "hireology.com", "zohorecruit.com",
    "zoho.com", "phenompeople.com", "avature.net", "radancy.com", "tal.net",
    "jobs.net", "peoplefluent.com", "silkroad.com", "hiringthing.com",
    "crelate.com", "bullhorn.com", "jobscore.com", "wellfound.com",
    "builtin.com", "themuse.com", "careerjet.com", "adzuna.com",
    "jobgether.com", "himalayas.app", "remoteok.com", "weworkremotely.com",
    "ycombinator.com", "dover.com", "join.com", "keka.com",
    "welcometothejungle.com", "manatal.com", "neogov.com", "ukgpro.com",
    "totaljobs.com", "reed.co.uk", "seek.com.au", "naukri.com",
    "jobs2web.com", "careerplug.com", "paycor.com", "trakstar.com",
    "applicantpro.com", "smartrecruiters.de", "jobs.abbott.smartrecruiters",
)
# The Phenom detail-URL shape: an optional country segment, a language
# segment, then /job/ or /jobs/.  Deliberately loose — it only widens the
# pool, the widgets probe is what decides.
SHAPE = re.compile(
    r"^/(?:[a-z]{2}(?:[_-][a-z]{2})?|global|search|sitemap)"
    r"/[a-z]{2}(?:[_-][a-z]{2})?/jobs?/", re.I)
# Host labels that plausibly front a career site.
NAMEY = re.compile(r"(^|[.\-])(careers?|jobs?|talent|apply|hiring|work|"
                   r"recruit|employment|opportunities)([.\-]|$)", re.I)

# Boards the fetcher shipped with, plus the two whose hostnames MOVED (see the
# note in phenom_fetch.py).  Seeded so a run with no master still has input.
SEED = [
    ("careers.teksystems.com", "TEKsystems", "agency"),
    ("careers.actalentservices.com", "Actalent", "agency"),
    ("careers.astoncarter.com", "Aston Carter", "agency"),
    ("jobs.aerotek.com", "Aerotek", "agency"),
    ("careers.mlaglobal.com", "MLA Global", "agency"),
]


def host_of(url):
    u = (url or "").strip()
    if not u or "://" not in u:
        return "", ""
    try:
        sp = urlsplit(u)
    except ValueError:
        return "", ""
    return (sp.hostname or "").lower(), sp.path or ""


def _skip(host):
    return any(host == k or host.endswith("." + k) for k in KNOWN_APEX)


def mine(master=MASTER):
    """Candidate hosts + their modal company name, out of the master."""
    if not os.path.exists(master):
        print("mine: no master at %s — seeds only" % master)
        return {}
    rows = collections.Counter()
    comps = collections.defaultdict(collections.Counter)
    shaped = set()
    csv.field_size_limit(10 ** 7)
    with open(master, newline="", encoding="utf-8", errors="replace") as f:
        rd = csv.reader(f)
        idx = {c: n for n, c in enumerate(next(rd))}
        ia, ij, ic = idx["apply_url"], idx["job_url"], idx["company"]
        n = 0
        for row in rd:
            n += 1
            if n % 500000 == 0:
                print("  mined %d rows, %d candidate hosts" % (n, len(rows)),
                      flush=True)
            company = row[ic].strip() if ic < len(row) else ""
            for i in (ia, ij):
                if i >= len(row):
                    continue
                h, path = host_of(row[i])
                if not h or _skip(h):
                    continue
                is_shape = bool(SHAPE.match(path))
                if not is_shape and not NAMEY.search(h):
                    continue
                if is_shape:
                    shaped.add(h)
                rows[h] += 1
                if company:
                    comps[h][company] += 1
    print("  mined %d rows total -> %d candidate hosts (%d path-shaped)"
          % (n, len(rows), len(shaped)))
    out = {}
    for h, c in rows.items():
        out[h] = {"host": h, "company": pick_company(h, comps[h]),
                  "rows": c, "shaped": int(h in shaped)}
    return out


_LABEL = re.compile(r"[^a-z0-9]+")
GENERIC_LABEL = {"www", "careers", "career", "jobs", "job", "talent", "apply",
                 "hiring", "work", "recruiting", "recruit", "employment",
                 "search", "my", "us", "global", "en"}


def board_id(host):
    """A short, stable id for the board — the state-file key and the `board`
    column.  Strip the generic front labels ('www.jobs.abbott' -> abbott,
    'careers.pnc.com' -> pnc) rather than taking parts[0], which would name a
    hundred different boards "careers"."""
    parts = [p for p in host.lower().split(".") if p]
    while len(parts) > 1 and parts[0] in GENERIC_LABEL:
        parts.pop(0)
    # Drop the TLD ('abbott' has none left to drop; 'pnc.com' -> pnc).
    if len(parts) > 1:
        parts = parts[:-1]
    return _LABEL.sub("", "".join(parts[:1])) or _LABEL.sub("", host)


# Trailing words a career site appends to its own name.  "Snowflake Careers"
# is the employer "Snowflake"; the suffix is site furniture, not company name.
_SITE_SUFFIX = re.compile(
    r"\s*(?:[-|–—:]\s*)?\b(careers?|jobs?|job\s+search|talent|"
    r"career\s+site|opportunities|hiring)\b\s*$", re.I)


def _clean_site_name(v):
    v = re.sub(r"\s+", " ", (v or "")).strip().strip("|-–—: ")
    for _ in range(2):                      # "Foo Careers Jobs" -> "Foo"
        v = _SITE_SUFFIX.sub("", v).strip()
    return v if 1 < len(v) <= 80 else ""


def site_name(host, sess=None, timeout=20):
    """The employer name the BOARD publishes about itself, or "".

    This is the authoritative source and the reason it is worth 127 extra
    requests.  The Phenom search payload carries no employer field at all, so
    the previous approach took the modal `company` over that host's rows in
    all_jobs_master.csv -- and those rows came from aggregators that labelled
    them with whatever brand they felt like.  Measured 2026-08-20, that put a
    FALSE employer on real postings:

        careers.usbank.com    -> "Elavon"                        (a subsidiary)
        jobs.ascension.org    -> "Cedar Park Regional Medical Center"
        careers.marsh.com     -> "Mercer France"                 (a sibling)
        careers.opentext.com  -> "Webroot"
        careers.academy.com   -> "Colearn"
        careers.snowflake.com -> "TensorStax"

    and the board's company is stamped on EVERY row it produces, so each bad
    pick mislabels a whole board.  Dominance does not rescue it: usbank shows
    100% because the master holds exactly one row for that host.

    og:site_name gets all six right (U.S. Bank / Ascension / Marsh McLennan /
    OpenText / Academy Sports / Snowflake).
    """
    own = sess is None
    s = sess or requests.Session()
    try:
        r = s.get("https://%s/" % host, timeout=timeout, allow_redirects=True,
                  headers={"User-Agent": cc.UA})
        if r.status_code != 200 or not r.text:
            return ""
        body = r.text[:400000]
    except requests.RequestException:
        return ""
    finally:
        if own:
            s.close()
    for pat in (r"<meta[^>]+property=[\"\']og:site_name[\"\'][^>]+"
                r"content=[\"\']([^\"\']+)",
                r"<meta[^>]+name=[\"\']application-name[\"\'][^>]+"
                r"content=[\"\']([^\"\']+)",
                r"<title[^>]*>(.*?)</title>"):
        m = re.search(pat, body, re.I | re.S)
        if not m:
            continue
        v = _clean_site_name(html.unescape(m.group(1)))
        # A <title> is often a whole sentence ("Join the Snowflake Team | ...");
        # take its most name-like segment rather than the sentence.
        if v and pat.startswith("<title") and len(v.split()) > 4:
            parts = [_clean_site_name(x) for x in re.split(r"[|–—]", v)]
            parts = [x for x in parts if x and len(x.split()) <= 4]
            v = min(parts, key=len) if parts else ""
        if v:
            return v
    return ""


def pick_company(host, counter, probed=""):
    """The employer name for a board.

    Order of trust, and it matters:
      1. what the BOARD says about itself (site_name) -- the employer's own claim
      2. the modal company over that host's rows in the master, but ONLY when
         it corroborates the host, so a subsidiary cannot hijack the board
      3. nothing.  A blank company is honest; a confident wrong one is not, and
         it propagates to every row of the board.
    """
    if probed:
        return probed
    top = counter.most_common(6) if counter else []
    key = board_id(host)
    if key:
        for name, _ in top:
            flat = _LABEL.sub("", name.lower())
            if flat and (flat == key or flat.startswith(key)
                         or key.startswith(flat)):
                return name
    # Deliberately NOT top[0][0].  See site_name() for the six boards that
    # fallback mislabelled and why "most common" is not evidence here.
    return ""


# ---- throttle (the ats_fetch.py speed_up/slow_down pair) -------------------
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
        _pause_until[0] = time.time() + 10
        _interval[0] = min(_interval[0] * 1.5, 0.5)


def speed_up():
    """Decay the penalty.  A one-way ratchet was measured HALVING throughput
    elsewhere in this repo (39 -> 21 boards/min); recovery is slower than the
    backoff so a host that is genuinely refusing still wins the argument."""
    with _rate_lock:
        _ok_streak[0] += 1
        if _ok_streak[0] < 30:
            return
        _ok_streak[0] = 0
        floor = 1.0 / RPS
        if _interval[0] > floor:
            _interval[0] = max(_interval[0] * 0.9, floor)


class Transient(Exception):
    """Unreachable, not absent.  The host stays PENDING and is retried."""


class NotPhenom(Exception):
    """The host answered, and what it answered is provably not the Phenom
    widgets API.  Carries the discriminator so the audit CSV says WHY."""
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def body(size=1):
    return {"ddoKey": "refineSearch", "from": 0, "size": size, "jobs": True,
            "country": "us", "lang": "en_us", "pageName": "search-results",
            "siteType": "external", "keywords": "", "selected_fields": {}}


def is_refine(d):
    """The payload-shape test.  A Phenom widgets response is a dict with a
    dict under 'refineSearch' carrying the search envelope."""
    if not isinstance(d, dict):
        return False
    rs = d.get("refineSearch")
    return isinstance(rs, dict) and (
        "totalHits" in rs or "hits" in rs or "data" in rs)


def probe(host):
    """-> (totalHits, site_path, url_mode).  Raises NotPhenom / Transient."""
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                      "Content-Type": "application/json"})
    try:
        last = None
        for attempt in range(TRIES):
            throttle()
            try:
                r = s.post("https://%s/widgets" % host, json=body(),
                           timeout=TIMEOUT)
            except requests.exceptions.SSLError as e:
                raise NotPhenom("tls-%s" % type(e).__name__)
            except requests.exceptions.ConnectionError as e:
                # NXDOMAIN is the host ASSERTING it does not exist; every other
                # connection error is a network blip and must stay pending.
                if "NameResolutionError" in str(e) or "nodename" in str(e):
                    raise NotPhenom("dns-nxdomain")
                last = type(e).__name__
                time.sleep(1.0 * (attempt + 1))
                continue
            except requests.RequestException as e:
                last = type(e).__name__
                time.sleep(1.0 * (attempt + 1))
                continue

            code = r.status_code
            ct = (r.headers.get("content-type") or "").lower()
            if code == 200:
                if "json" not in ct:
                    # The SPA wildcard: 200 + an HTML shell on any path.
                    raise NotPhenom("200-html-shell")
                try:
                    d = r.json()
                except ValueError:
                    raise NotPhenom("200-unparseable")
                if not is_refine(d):
                    raise NotPhenom("200-not-refinesearch")
                speed_up()
                rs = d["refineSearch"]
                jobs = ((rs.get("data") or {}).get("jobs")) or []
                sp, mode = resolve_site_path(s, host,
                                             jobs[0] if jobs else None)
                return rs.get("totalHits"), sp, mode
            if code in (404, 410, 405, 501):
                # No widgets route on this host.  For THIS family that is the
                # same as not existing: nothing to harvest either way.
                raise NotPhenom("http-%d-no-widgets" % code)
            if code in (400, 401):
                raise NotPhenom("http-%d-rejected" % code)
            if code in (403, 502, 504, 408, 500):
                # Transient, but NOT a reason to slow the sweep down.  Every
                # candidate here is a DIFFERENT origin, so a WAF refusing us at
                # careers.x.com says nothing about jobs.y.com — and with one
                # shared bucket, letting each of those pause the run for 10s
                # cost the sweep half its throughput (66 -> 36 hosts/min,
                # measured 2026-08-20).  It still stays transient rather than
                # "not Phenom": calling a WAF block a verdict would drop a live
                # board forever.
                last = "HTTP %d" % code
            elif code in (429, 503):
                # Real rate limiting.  Phenom career sites share one platform
                # edge, so this one IS global.
                slow_down()
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
            time.sleep(1.0 * (attempt + 1))
        raise Transient("%s: %s" % (host, last))
    finally:
        s.close()


_SITE_PATH = re.compile(r"^/([a-z]{2,8}/[a-z]{2})(?:/|$)", re.I)
_LOCALE = re.compile(r"^([a-z]{2})[_-]([a-z]{2,8})$", re.I)


def root_path(s, host):
    """The locale prefix a GET on / redirects to.  A HINT, not the answer."""
    try:
        throttle()
        r = s.get("https://%s/" % host, timeout=TIMEOUT,
                  allow_redirects=False, headers={"Accept": "text/html"})
        loc = r.headers.get("Location") or ""
        if "://" in loc:
            loc = urlsplit(loc).path
        m = _SITE_PATH.match(loc)
        if m:
            return m.group(1).lower()
    except requests.RequestException:
        pass
    return ""


def resolve_site_path(s, host, job):
    """The locale prefix this board's JOB PAGES actually live under.

    This is not cosmetic — it is how the fetcher builds a real posting URL
    (rule 4), and it cannot be guessed or read off the home page.  Measured
    2026-08-20, every cheap heuristic is wrong on some board:

      careers.pnc.com    root -> /global/en   jobs are locale en_US, and
                         /us/en/job/... 303s to the home page.  So the JOB's
                         own locale is wrong here.
      careers.dhl.com    root -> /amer/en     but every job is en_GLOBAL and
                         only /global/en/job/... renders.  So the ROOT
                         redirect is wrong here.
      careers.allianz.com root -> /global/en  and /global/en/job/... answers
                         410 Gone for a posting the API just returned.  Wrong
                         again, in the third direction.

    So the prefix is decided the only way that cannot lie: fetch a posting the
    API just handed us under each candidate and keep the one that renders.
    """
    hint = root_path(s, host)
    cands = []
    if job:
        m = _LOCALE.match((job.get("locale") or "").strip())
        if m:
            cands.append("%s/%s" % (m.group(2).lower(), m.group(1).lower()))
    for c in (hint, "us/en", "global/en", "en/en"):
        if c and c not in cands:
            cands.append(c)
    jid = (job or {}).get("jobSeqNo") or (job or {}).get("jobId") if job \
        else None
    if not jid:
        # An employer with zero open reqs: nothing to test against.  The hint
        # is all there is, and the fetcher will emit no rows anyway.
        return hint or "us/en", "phenom"
    for c in cands:
        try:
            throttle()
            r = s.get("https://%s/%s/job/%s" % (host, c, jid), timeout=TIMEOUT,
                      allow_redirects=False, headers={"Accept": "text/html"})
        except requests.RequestException:
            continue
        # 200 only.  303 means "wrong prefix, here is the home page" and 410
        # means "right prefix would have worked, this req is gone" — but a 410
        # on the FIRST job of the board is how careers.allianz.com presents a
        # wrong prefix, so it is not accepted either.
        if r.status_code == 200 and len(r.text or "") > 5000:
            return c, "phenom"
    # No prefix renders a posting.  This is a real configuration, not a bug:
    # jobs.trinity-health.org serves the widgets API but 404s its own
    # /us/en/job/... AND /us/en/search-results — the HTML site is somewhere
    # else.  Its payload hands back a genuine Workday deep link
    # (trinityhealth.wd1.myworkdayjobs.com/Jobs/job/.../..._00637465/apply),
    # which all_jobs_build already canonicalises, so the fetcher is told to
    # prefer applyUrl on THIS board rather than minting a URL that 404s.
    return hint or "us/en", "ats"


def load_candidates():
    if not os.path.exists(CAND_CSV):
        return {}
    with open(CAND_CSV, newline="", encoding="utf-8") as f:
        return {r["host"]: r for r in csv.DictReader(f)}


CAND_COLS = ["host", "company", "kind", "rows", "shaped", "verdict",
             "total", "site_path", "url_mode", "checked"]


def save_candidates(cands):
    tmp = CAND_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CAND_COLS)
        w.writeheader()
        for h in sorted(cands):
            row = {c: cands[h].get(c, "") for c in CAND_COLS}
            w.writerow(row)
    os.replace(tmp, CAND_CSV)


def probe_company_names(proved, workers=12):
    """Ask each PROVED board what employer it is, and prefer that answer.

    Done here rather than during candidate mining because it costs one request
    per board and only the ~130 proved boards matter, not the ~3,700
    candidates.  See site_name() for why the master's `company` column is not
    trustworthy for this family.
    """
    todo = [c for c in proved if c.get("host")]
    if not todo:
        return 0
    print("  asking %d boards for their own name (og:site_name) ..." % len(todo),
          flush=True)
    changed = [0]
    lk = threading.Lock()

    def one(c):
        s = requests.Session()
        try:
            got = site_name(c["host"], sess=s)
        finally:
            s.close()
        if not got:
            return
        with lk:
            if got != c.get("company"):
                c["company_was"] = c.get("company") or ""
                changed[0] += 1
            c["company"] = got
            c["company_src"] = "site"

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, todo))
    print("  %d of %d board names corrected from the board itself"
          % (changed[0], len(todo)), flush=True)
    return changed[0]


def write_tenants(cands):
    os.makedirs(os.path.dirname(TENANT_CSV), exist_ok=True)
    proved = [c for c in cands.values() if c.get("verdict") == "phenom"]
    proved.sort(key=lambda c: -int(c.get("total") or 0))
    if os.environ.get("PHENOM_NO_NAME_PROBE") != "1":
        probe_company_names(proved)
    tmp = TENANT_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # url_mode is appended LAST so any positional reader of the older
        # five-column shape is unmoved.
        w.writerow(["board", "company", "host", "site_path", "source_type",
                    "total", "url_mode", "company_src"])
        used = {}
        for c in proved:
            board = board_id(c["host"])
            # Two hosts can collapse to the same id (careers.x.com and
            # jobs.x.co.uk).  Suffix rather than let one board's state entry
            # silently mark the other done.
            if board in used:
                used[board] += 1
                board = "%s%d" % (board, used[board])
            else:
                used[board] = 1
            w.writerow([board, c.get("company") or board.title(), c["host"],
                        c.get("site_path") or "us/en",
                        c.get("kind") or "ats", c.get("total") or "",
                        c.get("url_mode") or "phenom",
                        c.get("company_src") or "corpus"])
    os.replace(tmp, TENANT_CSV)
    return len(proved)


def fix_paths():
    """Re-run resolve_site_path over the boards already proved.

    Kept separate from --prove because re-probing 3.7k candidates to fix ~150
    stored prefixes is an hour of requests for a minute of work.
    """
    cands = load_candidates()
    st = cc.load_state(STATE)
    for h, rec in st.items():
        if h in cands:
            cands[h].update({k: rec.get(k, "") for k in
                             ("verdict", "total", "site_path", "url_mode",
                              "checked")})
    proved = [h for h in sorted(cands)
              if cands[h].get("verdict") == "phenom"]
    print("re-resolving site_path for %d proved boards" % len(proved))
    lock = threading.Lock()
    changed = [0]

    def one(h):
        s = requests.Session()
        s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                          "Content-Type": "application/json"})
        try:
            throttle()
            r = s.post("https://%s/widgets" % h, json=body(), timeout=TIMEOUT)
            d = r.json()
            jobs = ((d["refineSearch"].get("data") or {}).get("jobs")) or []
            sp, mode = resolve_site_path(s, h, jobs[0] if jobs else None)
        except Exception:                                    # noqa: BLE001
            return                       # leave the stored prefix alone
        finally:
            s.close()
        with lock:
            if (sp, mode) != (cands[h].get("site_path"),
                              cands[h].get("url_mode")):
                print("  %-34s %s/%s -> %s/%s"
                      % (h, cands[h].get("site_path") or "-",
                         cands[h].get("url_mode") or "-", sp, mode), flush=True)
                changed[0] += 1
            cands[h]["site_path"] = sp
            cands[h]["url_mode"] = mode
            st[h] = cands[h]

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(one, proved))
    cc.save_state(STATE, st)
    save_candidates(cands)
    n = write_tenants(cands)
    print("site_path changed on %d of %d boards | tenants/phenom.csv: %d"
          % (changed[0], len(proved), n))
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mine", action="store_true")
    ap.add_argument("--prove", action="store_true")
    ap.add_argument("--limit", type=int, default=LIMIT)
    ap.add_argument("--recheck", action="store_true",
                    help="re-probe hosts already given a verdict")
    ap.add_argument("--paths", action="store_true",
                    help="re-resolve site_path for already-proved boards only")
    ap.add_argument("--names", action="store_true",
                    help="re-ask every proved board for its own employer name "
                         "(og:site_name) and rewrite tenants/phenom.csv")
    a = ap.parse_args()
    if not a.mine and not a.prove and not a.paths and not a.names:
        a.mine = a.prove = True
    if a.names:
        # Repairs the company column in place, without a re-discovery.  The
        # column is stamped on every row the fetcher emits, so a wrong name is
        # a wrong employer on real postings -- see site_name().
        rows = list(csv.DictReader(open(TENANT_CSV, encoding="utf-8"))) \
            if os.path.exists(TENANT_CSV) else []
        if not rows:
            print("no %s to repair" % TENANT_CSV)
            return 1
        probe_company_names(rows)
        cols = list(rows[0])
        for c in ("company_src", "company_was"):
            if c not in cols:
                cols.append(c)
        tmp = TENANT_CSV + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
        os.replace(tmp, TENANT_CSV)
        print("rewrote %s (%d boards)" % (TENANT_CSV, len(rows)))
        return 0
    if a.paths:
        return fix_paths()

    cands = load_candidates()
    if a.mine:
        for h, rec in mine().items():
            cur = cands.setdefault(h, {"host": h})
            if rec["company"]:
                cur["company"] = rec["company"]
            cur["rows"] = rec["rows"]
            cur["shaped"] = rec["shaped"]
            cur.setdefault("kind", "ats")
        for host, company, kind in SEED:
            cur = cands.setdefault(host, {"host": host, "rows": 0,
                                          "shaped": 1})
            cur["company"] = company
            cur["kind"] = kind
        save_candidates(cands)
        print("candidates: %d" % len(cands))

    if not a.prove:
        return 0

    st = cc.load_state(STATE)
    for h, rec in st.items():
        if h in cands and not a.recheck:
            cands[h].update({k: rec.get(k, "") for k in
                             ("verdict", "total", "site_path", "url_mode",
                              "checked")})

    pending = [h for h in sorted(cands)
               if a.recheck or not (cands[h].get("verdict") or "").strip()]
    if a.limit:
        # Prefer the path-shaped hosts and the busiest hosts when sampling, so
        # a smoke run is a fair test of the probe rather than an alphabet walk.
        pending.sort(key=lambda h: (-int(cands[h].get("shaped") or 0),
                                    -int(cands[h].get("rows") or 0)))
        pending = pending[:a.limit]
    print("probing %d of %d candidates (%d already decided)"
          % (len(pending), len(cands),
             len(cands) - len([h for h in cands
                               if not (cands[h].get("verdict") or "")])))

    lock = threading.Lock()
    counts = collections.Counter()
    reasons = collections.Counter()
    # Checkpoint every N decisions.  A 3.7k-host sweep runs for the better part
    # of an hour against a long tail of hosts that answer nothing until the
    # socket times out; saving only in the finally: block means one Ctrl-C
    # throws away every verdict earned so far.
    CKPT = int(os.environ.get("PHENOM_DISC_CKPT", "100"))

    def _checkpoint():
        cc.save_state(STATE, st)
        save_candidates(cands)

    def work(h):
        try:
            total, sp, mode = probe(h)
        except NotPhenom as e:
            with lock:
                cands[h].update({"verdict": "no:%s" % e.reason, "total": "",
                                 "site_path": "",
                                 "checked": time.strftime("%Y-%m-%d")})
                st[h] = cands[h]
                counts["no"] += 1
                reasons[e.reason] += 1
                if (counts["no"] + counts["phenom"]) % CKPT == 0:
                    _checkpoint()
                    print("  ... %d decided (%d phenom), %d pending"
                          % (counts["no"] + counts["phenom"],
                             counts["phenom"], counts["err"]), flush=True)
            return
        except Transient as e:
            # NOT written to state: the host stays pending for the next run.
            with lock:
                counts["err"] += 1
                reasons[str(e)[-24:]] += 1
            return
        except Exception as e:                             # noqa: BLE001
            with lock:
                counts["err"] += 1
                reasons[type(e).__name__] += 1
            return
        with lock:
            cands[h].update({"verdict": "phenom", "total": total if
                             total is not None else "",
                             "site_path": sp, "url_mode": mode,
                             "checked": time.strftime("%Y-%m-%d")})
            st[h] = cands[h]
            counts["phenom"] += 1
            if counts["phenom"] % 10 == 0:
                print("  proved %d ... latest %s (%s jobs)"
                      % (counts["phenom"], h, total), flush=True)
            if (counts["no"] + counts["phenom"]) % CKPT == 0:
                _checkpoint()

    t0 = time.time()
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            list(ex.map(work, pending))
    finally:
        cc.save_state(STATE, st)
        save_candidates(cands)
        n = write_tenants(cands)
        print("PROVED %d phenom hosts | not-phenom %d | unreachable %d "
              "(still pending) | %.1fs"
              % (counts["phenom"], counts["no"], counts["err"],
                 time.time() - t0))
        for r, c in reasons.most_common(12):
            print("    %-28s %d" % (r, c))
        print("tenants/phenom.csv: %d boards" % n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
