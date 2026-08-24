#!/usr/bin/env python3
"""
arbeitsagentur_fetch.py — Bundesagentur fuer Arbeit "jobsuche" REST API
(the German federal employment agency's public job board), verified 2026-07-29.

  search   GET https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs
  detail   GET .../pc/v4/jobdetails/{base64(referenznummer)}
  auth     header  X-API-Key: jobboerse-jobsuche
           A public constant, not a secret: it is the OAuth clientId the
           jobsuche SPA ships in https://www.arbeitsagentur.de/jobsuche/config/js
           ('oidc__clientId': 'jobboerse-jobsuche').  No account needed.
           WITHOUT the header every path answers HTTP 403 with a 1-byte body.

WHAT THIS SOURCE ACTUALLY IS (measured 2026-07-29, read before scoping a run)
  1,015,053 live postings, and it is a GERMAN DOMESTIC board:
    * ~94% of titles are German-language (sampled 600 titles, 3 slices)
    *  977,949 postings (96.3%) are located in Germany
    *   37,104 sit outside Germany, but 36,312 of those are Austria (34,941)
        and Switzerland (1,371) — i.e. still German-language DACH postings
    *      792 are outside the DACH bloc altogether
    *       24 are in the United States
  So for a US-remote / Massachusetts index this is ~1M rows of German
  on-site work.  Dumping it whole would swamp the index, which is why the
  default scope below is a filtered slice, not the corpus.

SCOPES (env ARBEITSAGENTUR_SCOPE, default "relevant")
  relevant  homeoffice + non-DACH foreign + English-titled  (the default)
  remote    the homeoffice slice only
  intl      foreign-located postings only
  english   English-titled postings only
  all       the full 1,015,053-row corpus (partitioned; hours, opt-in)
Flags:  ARBEITSAGENTUR_INCLUDE_DACH=1   add Austria+Switzerland (+36k German rows)
        ARBEITSAGENTUR_ENGLISH_ONLY=1   drop rows whose title reads as German
        ARBEITSAGENTUR_NO_DETAIL=1      skip the apply-URL enrichment pass

THREE TRAPS THIS FILE IS BUILT AROUND — each one measured, not assumed
  1. UNKNOWN QUERY PARAMS ARE SILENTLY IGNORED, NOT REJECTED.  `land=`,
     `country=`, `arbeitsland=` all answer HTTP 200 with the FULL unfiltered
     1,015,053 and a body of German jobs.  A fetcher that trusts an invented
     param "collects" the whole board under a US label.  Every partition
     dimension here was proved to filter by checking that its per-value counts
     SUM to the unfiltered total (see verify_dims()).
  2. `wo=` IS A FUZZY GEO RESOLVER THAT FALLS BACK INSTEAD OF FAILING.
     wo="Vereinigtes Koenigreich" returns 5,077 rows that are ALL land=
     DEUTSCHLAND; wo="New York" returns 5,820 rows centred on Ney, Rhineland.
     The real UK name is "Grossbritannien" and it has 31 jobs.  So every
     country slice is LAND-VALIDATED against the body before it is walked, and
     a slice whose rows do not carry the expected land is dropped.
  3. 10,000-RESULT WINDOW.  size*page must be <= 10000; size caps at 500
     (size=1000 -> HTTP 400) and size=500&page=21 -> HTTP 400
     EINGABEPARAMETER_FEHLERHAFT.  Any slice above 10k is recursively split on
     verified-disjoint dimensions until every cell fits.

VERIFIED PAGINATION.  Pages genuinely advance: walking
homeoffice=nv_true&befristung=1 returned 1040 rows for a declared total of
1040 with ZERO duplicate referenznummer, and the size=500 walk and the
size=100 walk produced the identical 1040-id set.

APPLY URLS.  The search payload has no link at all.  The detail endpoint
carries `externeURL`, the employer/partner-board link, and its coverage splits
perfectly on one facet: externestellenboersen=true -> 100% have externeURL
(100/100 sampled), externestellenboersen=false -> 0% (0/100).  So the detail
pass runs ONLY over the ext=true partition and everything else falls back to
the arbeitsagentur jobdetail page.  That turns a 21k-request enrichment into a
6k-request one with no loss.

MEASURED RUN, scope=relevant, 2026-07-29 — 24,785 rows to
contract_norm/arbeitsagentur.jsonl in ~15 min:
    21,173  homeoffice   (the whole declared 21,174-row remote slice)
       598  intl         (non-DACH foreign, net of cross-country duplicates)
     3,014  english      (English-titled, from the was= keyword sweep)
  0 duplicate job_id, 24,785 distinct apply_url, 0 blank titles.
  employment_type  Full-time 19,621 / Temp 2,241 / Part-time 1,157 /
                   Internship 948 / Freelance 810 / Interim 5 / Contract 3
  6,942 rows (28%) carry an off-arbeitsagentur apply URL.
BUT READ THIS BEFORE WIRING IT IN: of those 24,785 rows, 23,849 are in
Germany, 3,832 have an English title, 22 are in the United States, and
exactly ONE is in Massachusetts.  This source is a strong German/EU feed and
a negligible US-remote one.
"""
import base64
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, urlparse

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "arbeitsagentur.jsonl")

API_KEY = "jobboerse-jobsuche"
SEARCH = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v6/jobs"
DETAIL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobdetails/"
SITE = "https://www.arbeitsagentur.de/jobsuche/"

WINDOW = 10000          # hard server-side cap on size*page
PAGE_SIZE = 500         # max accepted; size=1000 -> HTTP 400
MAX_PAGE = WINDOW // PAGE_SIZE   # 20

SCOPE = os.environ.get("ARBEITSAGENTUR_SCOPE", "relevant")
INCLUDE_DACH = os.environ.get("ARBEITSAGENTUR_INCLUDE_DACH") == "1"
ENGLISH_ONLY = os.environ.get("ARBEITSAGENTUR_ENGLISH_ONLY") == "1"
NO_DETAIL = os.environ.get("ARBEITSAGENTUR_NO_DETAIL") == "1"
DETAIL_WORKERS = int(os.environ.get("ARBEITSAGENTUR_WORKERS") or 8)

# Dimensions proved DISJOINT + EXHAUSTIVE on 2026-07-29 by checking that the
# per-value counts sum to the unfiltered total (verify_dims re-checks at run
# time).  `branche` (sums to 856,302 of 1,015,053) and `behinderung` (655,893)
# are NOT exhaustive and `arbeitszeit` OVERLAPS (sums to 1,196,368 — a posting
# can be both vz and tz), so none of those three can be used to partition.
SPLIT_DIMS = [
    ("befristung", [1, 2, 3]),                 # befristet / unbefristet / n.a.
    ("angebotsart", [1, 2, 4, 34]),            # Arbeit/Selbst./Ausb./Praktikum
    ("pav", ["true", "false"]),                # private Arbeitsvermittlung
    ("quereinstieg", ["true", "false"]),
]
# Always applied first: they double as ROW TAGS.  ext decides whether a detail
# fetch can yield an apply URL at all; zeitarbeit marks Arbeitnehmerueberlassung
# (German temp-agency leasing), which is contingent work and must not be
# reported as Full-time.
TAG_DIMS = [
    ("externestellenboersen", ["true", "false"]),
    ("zeitarbeit", ["true", "false"]),
]

# ---- country slices --------------------------------------------------------
# name -> land code the API must echo back.  Every entry below was confirmed by
# sampling the body: the rows really do carry that land.  Names that LOOK right
# but silently resolve to German results are listed in REJECTED_NAMES so nobody
# re-adds them.
COUNTRIES = {
    "Vereinigte Staaten": "VEREINIGTE_STAATEN", "Kanada": "KANADA",
    "Grossbritannien": "GROSSBRITANNIEN", "Irland": "IRLAND",
    "Niederlande": "NIEDERLANDE", "Belgien": "BELGIEN",
    "Luxemburg": "LUXEMBURG", "Frankreich": "FRANKREICH",
    "Spanien": "SPANIEN", "Portugal": "PORTUGAL", "Italien": "ITALIEN",
    "Daenemark": "DAENEMARK", "Schweden": "SCHWEDEN", "Norwegen": "NORWEGEN",
    "Finnland": "FINNLAND", "Polen": "POLEN",
    "Tschechien": "TSCHECHISCHE_REPUBLIK", "Slowakei": "SLOWAKEI",
    "Slowenien": "SLOWENIEN", "Ungarn": "UNGARN", "Rumaenien": "RUMAENIEN",
    "Bulgarien": "BULGARIEN", "Kroatien": "KROATIEN",
    "Griechenland": "GRIECHENLAND", "Zypern": "ZYPERN", "Malta": "MALTA",
    "Estland": "ESTLAND", "Lettland": "LETTLAND", "Litauen": "LITAUEN",
    "Tuerkei": "TUERKEI", "Ukraine": "UKRAINE", "Israel": "ISRAEL",
    "Vereinigte Arabische Emirate": "VER_ARAB_EMIRATE",
    "Saudi-Arabien": "SAUDI_ARABIEN", "Indien": "INDIEN", "China": "CHINA",
    "Japan": "JAPAN", "Singapur": "SINGAPUR", "Australien": "AUSTRALIEN",
    "Neuseeland": "NEUSEELAND", "Brasilien": "BRASILIEN", "Peru": "PERU",
    "Mexiko": "MEXIKO", "Argentinien": "ARGENTINIEN", "Chile": "CHILE",
    "Suedafrika": "SUEDAFRIKA", "Kenia": "KENIA", "Nigeria": "NIGERIA",
    "Ghana": "GHANA", "Marokko": "MAROKKO", "Vietnam": "VIETNAM",
    "Thailand": "THAILAND", "Philippinen": "PHILIPPINEN",
    "Georgien": "GEORGIEN", "Albanien": "ALBANIEN",
    "Nordmazedonien": "NORDMAZEDONIEN", "Liechtenstein": "LIECHTENSTEIN",
    "Russische Foederation": "RUSSISCHE_FOEDERATION",
}
DACH = {"Oesterreich": "OESTERREICH", "Schweiz": "SCHWEIZ"}
# Do NOT re-add these: each returns a large count of land=DEUTSCHLAND rows.
REJECTED_NAMES = {
    "Vereinigtes Koenigreich": "5,077 rows, all land=DEUTSCHLAND",
    "Vereinigtes Koenigreich (Grossbritannien und Nordirland)": "same 5,077",
    "England": "0", "UK": "0", "Schottland": "0", "Wales": "0",
    "New York": "5,820 rows centred on Ney, Rhineland-Palatinate",
}

LAND_TO_COUNTRY = {
    "DEUTSCHLAND": "Germany", "OESTERREICH": "Austria", "SCHWEIZ": "Switzerland",
    "VEREINIGTE_STAATEN": "United States", "KANADA": "Canada",
    "GROSSBRITANNIEN": "United Kingdom", "IRLAND": "Ireland",
    "NIEDERLANDE": "Netherlands", "BELGIEN": "Belgium",
    "LUXEMBURG": "Luxembourg", "FRANKREICH": "France", "SPANIEN": "Spain",
    "PORTUGAL": "Portugal", "ITALIEN": "Italy", "DAENEMARK": "Denmark",
    "SCHWEDEN": "Sweden", "NORWEGEN": "Norway", "FINNLAND": "Finland",
    "POLEN": "Poland", "TSCHECHISCHE_REPUBLIK": "Czechia",
    "SLOWAKEI": "Slovakia", "SLOWENIEN": "Slovenia", "UNGARN": "Hungary",
    "RUMAENIEN": "Romania", "BULGARIEN": "Bulgaria", "KROATIEN": "Croatia",
    "GRIECHENLAND": "Greece", "ZYPERN": "Cyprus", "MALTA": "Malta",
    "ESTLAND": "Estonia", "LETTLAND": "Latvia", "LITAUEN": "Lithuania",
    "TUERKEI": "Turkey", "UKRAINE": "Ukraine", "ISRAEL": "Israel",
    "VER_ARAB_EMIRATE": "United Arab Emirates", "SAUDI_ARABIEN": "Saudi Arabia",
    "INDIEN": "India", "CHINA": "China", "JAPAN": "Japan",
    "SINGAPUR": "Singapore", "AUSTRALIEN": "Australia",
    "NEUSEELAND": "New Zealand", "BRASILIEN": "Brazil", "PERU": "Peru",
    "MEXIKO": "Mexico", "ARGENTINIEN": "Argentina", "CHILE": "Chile",
    "SUEDAFRIKA": "South Africa", "KENIA": "Kenya", "NIGERIA": "Nigeria",
    "GHANA": "Ghana", "MAROKKO": "Morocco", "VIETNAM": "Vietnam",
    "THAILAND": "Thailand", "PHILIPPINEN": "Philippines",
    "GEORGIEN": "Georgia", "ALBANIEN": "Albania",
    "NORDMAZEDONIEN": "North Macedonia", "LIECHTENSTEIN": "Liechtenstein",
    "RUSSISCHE_FOEDERATION": "Russia",
}

# English-title keyword sweep.  `was=` is a real full-text filter, not a
# silently-ignored param: was="zzqqxxvvnonsense123" answers 0.  89% of the rows
# these terms return carry an English title (1,675 of 1,874 sampled).
ENGLISH_TERMS = [
    "Software Engineer", "Senior Software Engineer", "Data Scientist",
    "Data Engineer", "Data Analyst", "Machine Learning Engineer",
    "Product Manager", "Product Owner", "Project Manager", "Program Manager",
    "Engineering Manager", "DevOps Engineer", "Site Reliability Engineer",
    "Platform Engineer", "Cloud Engineer", "Cloud Architect",
    "Solutions Architect", "Security Engineer", "Frontend Developer",
    "Backend Developer", "Full Stack Developer", "Mobile Developer",
    "QA Engineer", "Test Engineer", "Business Analyst",
    "Business Development Manager", "Account Executive", "Account Manager",
    "Sales Manager", "Customer Success Manager", "Marketing Manager",
    "Growth Marketing", "Content Manager", "UX Designer", "UI Designer",
    "Product Designer", "Research Scientist", "AI Engineer",
    "Technical Writer", "Scrum Master", "Head of Engineering",
    "Head of Product", "Chief Technology Officer", "Working Student",
    "Consultant", "Recruiter", "Controller", "Supply Chain Manager",
]

# ---- language heuristics ---------------------------------------------------
_DE_RX = re.compile(
    r"[äöüßÄÖÜ]|"
    r"\b(und|für|mit|als|im|der|die|das|gesucht|Mitarbeiter|Fachkraft|"
    r"Kaufmann|Kauffrau|Helfer|Fahrer|Pflege|Leiter|Leitung|Assistenz|"
    r"Sachbearbeiter|Berater|Ausbildung|Praktikum|Koch|Monteur|Erzieher|"
    r"Vertrieb|Bereich|zur|zum|bei|von|oder|Werkstudent|Techniker|Meister|"
    r"Verkäufer|Reinigung|Betreuung|Elektroniker|Anlagenmechaniker|"
    r"Stellv|Fachinformatiker|Servicetechniker|Pflegefachkraft|Altenpfleger|"
    r"Aushilfe|Lagerist|Schlosser|Verwaltung|Buchhalter|Gesellschaft)\b",
    re.I)
_EN_RX = re.compile(
    r"\b(engineer|developer|manager|specialist|analyst|scientist|consultant|"
    r"designer|architect|senior|junior|head of|director|software|data|product|"
    r"sales|marketing|business|customer|support|account|project|research|"
    r"associate|officer|coordinator|remote|stack|frontend|backend|devops|"
    r"cloud|security|lead|intern|advisor|strategy|operations|success)\b", re.I)


def is_english_title(t):
    """True when a title reads as English.  Deliberately strict: any German
    marker (including a single umlaut or an (m/w/d) gender tag) disqualifies."""
    t = t or ""
    if "m/w/d" in t.lower() or "w/m/d" in t.lower():
        return False
    return bool(_EN_RX.search(t)) and not bool(_DE_RX.search(t))


# ---- http ------------------------------------------------------------------
def session():
    s = cc.session()
    s.headers.update({"X-API-Key": API_KEY})
    ad = requests.adapters.HTTPAdapter(pool_connections=DETAIL_WORKERS + 4,
                                       pool_maxsize=DETAIL_WORKERS + 4)
    s.mount("https://", ad)
    return s


def api(s, params, tries=5, timeout=40):
    """GET the search endpoint.  Returns a dict, or None on a real failure.

    Never conflates "no jobs" with "request failed" — a None return is loud at
    the call site, because an empty-looking answer is exactly how a throttle or
    a transport blip would otherwise be recorded as a finished slice.

    THIS HOST THROTTLES WITH 403, NOT 429.  Sustained paging earns a 403 with a
    1-byte body — byte-identical to the 403 you get for omitting the API key,
    so it reads like a hard auth failure and not like backpressure.  Measured
    2026-07-29: 117 of 150 detail requests 403'd under load, and every one of
    them returned 200 when replayed a minute later.  Treating 403 as fatal is
    what silently cost the first run 30% of its apply URLs.
    """
    for i in range(tries):
        try:
            r = s.get(SEARCH, params=params, timeout=timeout)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            if r.status_code in (429, 403):
                time.sleep(15 * (i + 1))
                continue
            if r.status_code == 400:
                return {"__bad__": True}
            time.sleep(1.5 * (i + 1))
        except requests.RequestException:
            time.sleep(1.5 * (i + 1))
    return None


def count(s, params):
    """maxErgebnisse for a query, or None."""
    p = dict(params)
    p["size"] = 1
    p["page"] = 1
    d = api(s, p)
    if not d or d.get("__bad__"):
        return None
    return d.get("maxErgebnisse")


# ---- startup self-checks ---------------------------------------------------
def verify_dims(s):
    """Prove each split dimension really filters before relying on it.

    This exists because the API answers HTTP 200 and the FULL unfiltered total
    for any param it does not recognise (`land=`, `country=`, `arbeitsland=`
    all measured returning 1,015,053).  A dimension is trusted only if its
    per-value counts sum to the unfiltered total and no single value returns
    that total on its own.  Index drift of a few rows across the sweep is
    tolerated; a silently-ignored param is off by ~100%.
    """
    total = count(s, {})
    if not total:
        print("  ! cannot read unfiltered total — aborting", flush=True)
        return None, []
    print(f"  unfiltered total: {total:,}", flush=True)
    good = []
    for dim, vals in SPLIT_DIMS:
        parts = []
        for v in vals:
            c = count(s, {dim: v})
            if c is None:
                break
            parts.append(c)
            time.sleep(0.15)
        if len(parts) != len(vals):
            print(f"  ! {dim}: probe failed, not used", flush=True)
            continue
        ssum = sum(parts)
        ignored = [v for v, c in zip(vals, parts) if abs(c - total) <= 2]
        drift = abs(ssum - total) / float(total)
        if ignored or drift > 0.01:
            print(f"  ! {dim}: sum={ssum:,} vs total={total:,} "
                  f"{'IGNORED:' + str(ignored) if ignored else ''} — NOT USED",
                  flush=True)
            continue
        print(f"  ok {dim}: {parts} sums to {ssum:,} (drift {drift * 100:.3f}%)",
              flush=True)
        good.append((dim, vals))
    return total, good


def validate_country(s, name, land, sample=25):
    """Confirm wo=<name> really returns rows in <land>.

    wo= is a fuzzy resolver that FALLS BACK to a German result set rather than
    erroring, so the count alone proves nothing — wo="Vereinigtes Koenigreich"
    reports 5,077 and every one of them is in Germany.  Only the body settles
    it, so this reads the land off the rows and demands a clean majority.
    """
    d = api(s, {"wo": name, "size": sample, "page": 1})
    if not d or d.get("__bad__"):
        return None, "probe failed"
    n = d.get("maxErgebnisse") or 0
    rows = d.get("ergebnisliste") or []
    if not n or not rows:
        return 0, "empty"
    hit = tot = 0
    for j in rows:
        for loc in (j.get("stellenlokationen") or []):
            tot += 1
            if ((loc.get("adresse") or {}).get("land")) == land:
                hit += 1
    if not tot:
        return None, "no locations in body"
    frac = hit / float(tot)
    if frac < 0.9:
        return None, f"body says {frac:.0%} {land} — resolver fell back"
    return n, "ok"


# ---- row mapping -----------------------------------------------------------
def raw_type(j):
    """Build a type string for cc.classify_any from the STRUCTURED v6 fields.

    Order is deliberate.  cc.norm_employment_type bails out on its permanent
    trap first, and 'unbefristet' is inside that trap — so a Zeitarbeit
    (temp-agency) posting that is also unbefristet would come back Full-time if
    both tokens were emitted together.  The contingent signals therefore win
    outright instead of being concatenated.
    """
    art = j.get("stellenangebotsart") or ""
    if art == "SELBSTAENDIGKEIT":
        return "freelance self-employed"
    if art == "AUSBILDUNG":
        return "apprenticeship"
    if art == "PRAKTIKUM_TRAINEE":
        return "internship"
    if j.get("__zeitarbeit__"):
        return "temp agency contract"          # Arbeitnehmerueberlassung
    if j.get("vertragsdauer") == "BEFRISTET":
        return "befristet fixed-term"
    if j.get("istGeringfuegigeBeschaeftigung") is True:
        return "part-time minijob"
    bits = []
    if j.get("vertragsdauer") == "UNBEFRISTET":
        bits.append("unbefristet")
    vz = j.get("arbeitszeitVollzeit") is True
    tz = any(j.get(k) is True for k in (
        "arbeitszeitTeilzeitFlexibel", "arbeitszeitTeilzeitVormittag",
        "arbeitszeitTeilzeitNachmittag", "arbeitszeitTeilzeitAbend"))
    if vz:
        bits.append("full-time")
    elif tz:
        bits.append("part-time")
    return " ".join(bits)


_PERIOD = {"STUNDENLOHN": "Hour", "JAHRESGEHALT": "Year",
           "MONATSGEHALT": "Month",
           "AUSBILDUNGSVERGUETUNG_NACH_JAHREN": "Month"}


def salary(j):
    art = j.get("artDerVerguetung")
    lo = hi = ""
    if art == "GEHALTSSPANNE":
        lo = j.get("gehaltsspanneVon") or ""
        hi = j.get("gehaltsspanneBis") or ""
    elif art == "FESTGEHALT":
        lo = hi = j.get("festgehalt") or ""
    if lo == "" and hi == "":
        return "", "", "", ""
    per = _PERIOD.get(j.get("verguetungsangabe") or "", "")
    return lo, hi, "EUR", per


def job_title(j):
    """Best available title.

    A minority of postings carry NO stellenangebotsTitel at all — not in the
    search payload and not in the detail record either (86 of the first 24,771
    rows collected, all of them '10000-' refnrs).  For those the occupation
    name is the only human label there is, so fall through to it rather than
    emitting a titleless row.
    """
    return (j.get("stellenangebotsTitel") or j.get("hauptberuf")
            or (j.get("alleBerufe") or [""])[0] or "")


def to_row(j, slice_tag, apply_url):
    locs = j.get("stellenlokationen") or []
    adr = (locs[0].get("adresse") or {}) if locs else {}
    land = adr.get("land") or ""
    region = adr.get("region") or ""
    state = "" if region in ("", "SONSTIGES", "Sonstiges") else \
        region.replace("_", " ").title()
    # `ort` frequently carries its own disambiguator — "Kassel, Hessen",
    # "Augsburg, Bayern", "Essen, Ruhr" — so the region is already inside the
    # city string and a naive join produced "Kassel, Hessen, Hessen, Germany".
    city = adr.get("ort") or ""
    if state and city.lower().endswith(", " + state.lower()):
        city = city[: -(len(state) + 2)].strip()
    country = LAND_TO_COUNTRY.get(land, land.replace("_", " ").title())
    parts = []
    for x in (city, state, country):
        if x and x.lower() not in (p.lower() for p in parts):
            parts.append(x)
    loc = ", ".join(parts)

    title = job_title(j)
    # homeofficemoeglich is the STRUCTURED remote flag.  When it is absent
    # entirely there is no signal, so work_mode stays blank rather than guessed.
    ho = j.get("homeofficemoeglich")
    if ho is True:
        remote, mode = True, "Remote"
    elif ho is False:
        remote, mode = False, "On-site"
    else:
        remote, mode = False, ""

    lo, hi, cur, per = salary(j)
    ref = j.get("referenznummer") or ""
    return cc.row(
        source="arbeitsagentur", source_type="job_board",
        company=j.get("firma") or "",
        title=title,
        employment_type=cc.classify_any(raw_type(j), title),
        work_mode=mode, location=loc, city=city, state=state,
        country=country, remote=remote,
        salary_min=lo, salary_max=hi, salary_currency=cur, salary_period=per,
        posted=j.get("datumErsteVeroeffentlichung")
        or ((j.get("veroeffentlichungszeitraum") or {}).get("von")) or "",
        apply_url=apply_url,
        job_id=ref, board=slice_tag, board_url=SITE)


def fallback_url(ref):
    return "https://www.arbeitsagentur.de/jobsuche/jobdetail/" + quote(ref, safe="")


# ---- crawl -----------------------------------------------------------------
def walk(s, params, seen, bag, label):
    """Page one <=10k cell to exhaustion.  Returns rows collected."""
    got = 0
    for page in range(1, MAX_PAGE + 1):
        p = dict(params)
        p["size"] = PAGE_SIZE
        p["page"] = page
        d = api(s, p)
        if d is None:
            print(f"    ! {label} page {page}: request failed, cell truncated",
                  flush=True)
            break
        if d.get("__bad__"):
            break
        rows = d.get("ergebnisliste") or []
        if not rows:
            break
        for j in rows:
            ref = j.get("referenznummer")
            if not ref or ref in seen:
                continue
            seen.add(ref)
            bag.append(j)
            got += 1
        if len(rows) < PAGE_SIZE:
            break
        time.sleep(0.2)
    return got


def crawl(s, params, seen, bag, dims, label):
    """Recursively split until each cell fits the 10,000-result window."""
    n = count(s, params)
    if n is None:
        print(f"    ! {label}: count failed", flush=True)
        return 0
    if n == 0:
        return 0
    if n > WINDOW:
        if dims:
            dim, vals = dims[0]
            tot = 0
            for v in vals:
                p = dict(params)
                p[dim] = v
                tot += crawl(s, p, seen, bag, dims[1:], f"{label} {dim}={v}")
            return tot
        print(f"    ! {label}: {n:,} > {WINDOW} and no split dims left — "
              f"taking first {WINDOW}", flush=True)
    return walk(s, params, seen, bag, label)


def crawl_slice(s, base_params, seen, bag, label, split_dims):
    """Walk a logical slice, always splitting on the TAG dims first so every
    row carries its externestellenboersen / zeitarbeit status.  Those two are
    not just partition tricks: ext decides whether a detail fetch can produce
    an apply URL, and zeitarbeit is the only signal that a posting is German
    temp-agency leasing rather than a permanent role."""
    total = 0
    for ext in TAG_DIMS[0][1]:
        for za in TAG_DIMS[1][1]:
            p = dict(base_params)
            p["externestellenboersen"] = ext
            p["zeitarbeit"] = za
            sub = []
            got = crawl(s, p, seen, sub, split_dims,
                        f"{label} ext={ext} za={za}")
            for j in sub:
                j["__ext__"] = (ext == "true")
                j["__zeitarbeit__"] = (za == "true")
                j["__slice__"] = label
            bag.extend(sub)
            total += got
    return total


# ---- detail enrichment -----------------------------------------------------
def fetch_detail(s, ref):
    """One jobdetail record, or None.

    403 is this host's throttle signal (see api()), so it MUST back off rather
    than burn its retries instantly — an earlier version fell through the
    status ladder and re-issued the same request three times with no sleep,
    which turned a temporary throttle into a permanent miss for that row.
    """
    key = base64.b64encode(ref.encode()).decode()
    for i in range(5):
        try:
            r = s.get(DETAIL + key, timeout=30)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    return None
            if r.status_code in (429, 403):
                time.sleep(12 * (i + 1))
                continue
            if r.status_code in (400, 404):
                return None
            time.sleep(1.5 * (i + 1))
        except requests.RequestException:
            time.sleep(1.5 * (i + 1))
    return None


def enrich(s, jobs):
    """Fill apply URLs from externeURL.  Only the ext=true rows are fetched:
    measured coverage is 100/100 there and 0/100 on ext=false, so hitting the
    rest would triple the request count for nothing.

    Paced deliberately.  A first run at 8 workers flat out drew a sustained
    403 throttle and resolved only 4,811 of 6,940 (69%) even though a slow
    probe of the same rows resolved 100%.  The per-request sleep plus the 403
    backoff in fetch_detail is what turns that back into full coverage, and a
    second pass mops up anything the throttle still swallowed.
    """
    targets = [j for j in jobs if j.get("__ext__") and j.get("referenznummer")]
    if not targets:
        return 0
    print(f"  detail pass over {len(targets):,} ext=true rows "
          f"({DETAIL_WORKERS} workers)", flush=True)
    done = [0]

    def one(j):
        if j.get("__apply__"):
            return
        d = fetch_detail(s, j["referenznummer"])
        done[0] += 1
        if done[0] % 1000 == 0:
            print(f"    {done[0]:,}/{len(targets):,}", flush=True)
        if d and d.get("externeURL"):
            j["__apply__"] = d["externeURL"]
        time.sleep(0.12)

    for attempt in (1, 2):
        todo = [j for j in targets if not j.get("__apply__")]
        if not todo:
            break
        if attempt == 2:
            print(f"    retry pass over {len(todo):,} unresolved", flush=True)
            time.sleep(20)
        done[0] = 0
        with ThreadPoolExecutor(DETAIL_WORKERS) as ex:
            list(ex.map(one, todo))
    return sum(1 for j in targets if j.get("__apply__"))


# ---- main ------------------------------------------------------------------
def main():
    s = session()
    print("arbeitsagentur: verifying endpoint + partition dims", flush=True)
    probe = api(s, {"size": 1, "page": 1})
    if not probe or "ergebnisliste" not in probe:
        print("  ! endpoint did not answer with a job list — abort", flush=True)
        return
    total, dims = verify_dims(s)
    if total is None:
        return

    seen, bag = set(), []
    stats = {}

    want_remote = SCOPE in ("relevant", "remote", "all")
    want_intl = SCOPE in ("relevant", "intl", "all")
    want_en = SCOPE in ("relevant", "english", "all")

    if SCOPE == "all":
        print("\n== FULL CORPUS (opt-in; this is ~1M German rows) ==", flush=True)
        n = crawl_slice(s, {}, seen, bag, "arbeitsagentur", dims)
        stats["all"] = n
        print(f"  all: {n:,}", flush=True)
    else:
        if want_remote:
            print("\n== homeoffice slice ==", flush=True)
            hn = count(s, {"homeoffice": "nv_true"})
            print(f"  declared homeoffice=nv_true total: {hn:,}", flush=True)
            n = crawl_slice(s, {"homeoffice": "nv_true"}, seen, bag,
                            "arbeitsagentur-homeoffice", dims)
            stats["homeoffice"] = n
            print(f"  homeoffice: {n:,} rows", flush=True)

        if want_intl:
            print("\n== international slices (land-validated) ==", flush=True)
            cmap = dict(COUNTRIES)
            if INCLUDE_DACH:
                cmap.update(DACH)
            else:
                print("  (Austria 34,941 + Switzerland 1,371 EXCLUDED — "
                      "set ARBEITSAGENTUR_INCLUDE_DACH=1 to add them)",
                      flush=True)
            n = 0
            for name, land in sorted(cmap.items()):
                c, why = validate_country(s, name, land)
                if c is None:
                    print(f"    ! wo={name}: {why} — SKIPPED", flush=True)
                    continue
                if c == 0:
                    continue
                got = crawl_slice(s, {"wo": name}, seen, bag,
                                  "arbeitsagentur-intl", dims)
                print(f"    {name:<30} declared {c:<6} kept {got}", flush=True)
                n += got
                time.sleep(0.2)
            stats["intl"] = n
            print(f"  international: {n:,} rows", flush=True)

        if want_en:
            print("\n== English-title keyword sweep ==", flush=True)
            n = 0
            for term in ENGLISH_TERMS:
                sub = []
                crawl_slice(s, {"was": term}, seen, sub,
                            "arbeitsagentur-english", dims)
                keep = [j for j in sub
                        if is_english_title(job_title(j))]
                bag.extend(keep)
                n += len(keep)
                print(f"    was={term:<32} raw {len(sub):<5} english {len(keep)}",
                      flush=True)
                time.sleep(0.2)
            stats["english"] = n
            print(f"  english: {n:,} rows", flush=True)

    print(f"\ncollected {len(bag):,} unique postings", flush=True)
    if not bag:
        print("DONE arbeitsagentur (nothing collected)", flush=True)
        return

    if not NO_DETAIL:
        got = enrich(s, bag)
        print(f"  apply URLs resolved from externeURL: {got:,}", flush=True)

    out = cc.Writer(OUT)
    written = eng = rem = 0
    hosts = {}
    for j in bag:
        title = job_title(j)
        if ENGLISH_ONLY and not is_english_title(title):
            continue
        ref = j.get("referenznummer") or ""
        url = j.get("__apply__") or fallback_url(ref)
        r = to_row(j, j.get("__slice__") or "arbeitsagentur", url)
        out.write(r)
        written += 1
        if r["remote"]:
            rem += 1
        if is_english_title(title):
            eng += 1
        if j.get("__apply__"):
            h = urlparse(url).netloc
            hosts[h] = hosts.get(h, 0) + 1
    out.close()
    s.close()

    print(f"\n  wrote {written:,} rows -> {OUT}", flush=True)
    print(f"  remote (homeofficemoeglich=true): {rem:,}", flush=True)
    print(f"  english-titled: {eng:,}", flush=True)
    top = sorted(hosts.items(), key=lambda kv: -kv[1])[:10]
    if top:
        print(f"  top externeURL hosts: {top}", flush=True)
    print(f"  slice counts: {stats}", flush=True)
    print("DONE arbeitsagentur", flush=True)


if __name__ == "__main__":
    sys.exit(main())
