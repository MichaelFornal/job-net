#!/usr/bin/env python3
"""
all_jobs_build.py — merge every pipeline's master into ONE deduplicated
all-jobs master + ONE monolithic workbook.

Inputs (produced by the per-family builders; missing ones are skipped with a
warning so a partial refresh still builds):

  getro_jobs_master.csv        full Getro universe    (~1.54M deduped)
  lever_jobs_master.csv        full Lever universe    (~76k)
  portals_combined_master.csv  FT+contract portals    (~150k: Bullhorn OSCP,
                               Ashby/SmartRecruiters/Recruitee/Breezy/BambooHR,
                               Phenom, marketplaces, Getro/Lever contract slices)

Cross-source dedupe, strongest key available wins:

  1. canonical ATS key parsed from the apply/job URL — (platform, org, job id).
     The same job's URL differs cosmetically between sources (/apply vs
     /application suffixes, boards. vs job-boards.greenhouse.io, www. vs
     jobs.smartrecruiters.com, -title-slug tails); the parsed tuple does not.
  2. (company, title, apply_url) when the URL matches no known platform
  3. (company, title, location) when there is no URL at all (Bullhorn portals)

Merge keeps the first-seen record's identity (ingest order getro -> lever ->
portals), unions sources/boards, keeps the earliest posted date, fills empty
fields from later copies. The key is written to the job_key column — it is
stable across snapshots, so flags can be carried to the next monolith.

Outputs:
  all_jobs_master.csv     complete deduped table (30 cols, always full)
  All_Jobs_<date>.xlsx    Pipeline / README / All Jobs / PDL Jobs / Summary /
                          Sources / Boards.  "All Jobs" holds everything except
                          jobs seen ONLY on the People Data Labs firehose,
                          which get their own tab — that split (plus automatic
                          "<name> 2" spill tabs) keeps every tab under Excel's
                          1,048,576-row limit while the workbook holds ALL jobs.

Posted-date cutoff: jobs whose (earliest-seen) posted date is before
2026-05-01 are cut everywhere — master CSV and workbook. Rows with NO date are
kept: their sources (Phenom, Bullhorn, some BambooHR/Recruitee feeds) never
publish dates, so blank means unknown, not old. When the cut set fits one
Excel tab there is no PDL split — a single "All Jobs" tab holds everything.

Flags:  --csv-only         write the master CSV and stop (fast iteration)
        --no-pdl           omit the PDL Jobs tab if the split is needed
        --since YYYY-MM-DD change the posted cutoff (default 2026-05-01)
        --all-dates        no cutoff: keep every job ever seen
"""

import csv
import json
import os
import re
import sys
import zipfile
from datetime import datetime, timezone
from urllib.parse import urlsplit, parse_qs

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter

import contract_common as cc
import build_sources

BASE = os.path.dirname(os.path.abspath(__file__))
GETRO_CSV = os.path.join(BASE, "getro_jobs_master.csv")
LEVER_CSV = os.path.join(BASE, "lever_jobs_master.csv")
GREENHOUSE_CSV = os.path.join(BASE, "greenhouse_jobs_master.csv")
ASHBY_CSV = os.path.join(BASE, "ashby_jobs_master.csv")
RIPPLING_CSV = os.path.join(BASE, "rippling_jobs_master.csv")
PERSONIO_CSV = os.path.join(BASE, "personio_jobs_master.csv")
YC_CSV = os.path.join(BASE, "yc_jobs_master.csv")
DOVER_CSV = os.path.join(BASE, "dover_jobs_master.csv")
NOTION_CSV = os.path.join(BASE, "notion_jobs_master.csv")
PORTALS_CSV = os.path.join(BASE, "portals_combined_master.csv")
OUT_CSV = os.path.join(BASE, "all_jobs_master.csv")
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_XLSX = os.path.join(BASE, f"All_Jobs_{SNAP}.xlsx")

CSV_ONLY = "--csv-only" in sys.argv
NO_PDL = "--no-pdl" in sys.argv


def _argval(flag, default):
    if flag in sys.argv:
        j = sys.argv.index(flag)
        if j + 1 < len(sys.argv):
            return sys.argv[j + 1]
    return default


POSTED_SINCE = "" if "--all-dates" in sys.argv \
    else _argval("--since", "2026-05-01")

XLSX_CAP = 1_048_000
PDL_BOARD = "People Data Labs"
FLAG_CHOICES = "Interested,Maybe,No"

COLUMNS = [
    "company", "title", "employment_type", "seniority", "department",
    "work_mode", "remote", "location", "city", "state", "country",
    "posted", "salary_min", "salary_max", "salary_currency", "salary_period",
    "equity", "skills", "company_stage", "company_size", "company_industry",
    "apply_url", "job_url", "boards", "board_count",
    "source", "source_type", "sources", "n_sources", "job_key",
    # Empty = live.  APPENDED LAST on purpose: IDX/N are positional and WIDTHS
    # is parallel to this list, so inserting anywhere else silently shifts
    # every column in the workbook.
    "delisted_on",
]
IDX = {c: i for i, c in enumerate(COLUMNS)}
N = len(COLUMNS)
# Trailing columns a master written by an OLDER build may legitimately lack.
# Everything here must be appended at the END of COLUMNS and optional to every
# reader, which is what makes a short header safe: a prefix match guarantees
# every column that IS present sits at the same index, so positional reads stay
# correct.  Bump this when appending another optional column.
OPTIONAL_TAIL = ("delisted_on",)


def check_header(hdr, path=""):
    """Validate a master's header and return how many columns it really has.

    Exits on genuine schema drift -- a reordered or renamed column would make
    every positional read silently wrong, which is the failure this guards.
    A master that merely predates an appended optional column is accepted, so
    a schema addition does not brick the downstream lane builders until the
    next full rebuild.
    """
    if hdr == COLUMNS:
        return len(hdr)
    if len(hdr) >= N - len(OPTIONAL_TAIL) and hdr == COLUMNS[:len(hdr)]:
        import sys as _s
        print(f"  note: {path or 'master'} predates "
              f"{', '.join(COLUMNS[len(hdr):])} — reading {len(hdr)} columns",
              file=_s.stderr)
        return len(hdr)
    import sys as _s
    _s.exit(f"master schema changed; expected {N} cols {COLUMNS[:3]}..., "
            f"got {len(hdr)} {hdr[:3]}...")
SRCS, BRDS, PIPES, BASECNT = N, N + 1, N + 2, N + 3  # aux tail slots

WIDTHS = [26, 44, 14, 12, 18, 9, 7, 30, 14, 10, 12, 11, 10, 10, 8, 9, 7, 40,
          13, 11, 30, 46, 46, 36, 8, 13, 12, 22, 9, 42, 12]

_MEMO = {}


def I(s):  # intern repeated strings; ~1.7M rows fit 16GB this way
    return _MEMO.setdefault(s, s)


def safe(v):
    return ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v


def num(v):
    s = str(v or "").strip()
    if not s:
        return ""
    try:
        f = float(s)
    except ValueError:
        return s
    return int(f) if f == int(f) else f


# ---- canonical ATS key ------------------------------------------------------
_DIGITS = re.compile(r"^(\d{4,})")


def _lead_digits(seg):
    m = _DIGITS.match(seg)
    return m.group(1) if m else ""


def canon_key(url):
    """'ats|platform|org|id' parsed from a job URL, or None."""
    u = (url or "").strip()
    if not u:
        return None
    if "://" not in u:
        u = "https://" + u.lstrip("/")
    try:
        sp = urlsplit(u)
    except ValueError:
        return None
    host = (sp.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    parts = [p.lower() for p in sp.path.split("/") if p]
    plat = org = jid = ""

    if host in ("jobs.lever.co", "jobs.eu.lever.co") and len(parts) >= 2:
        plat, org, jid = "lever", parts[0], parts[1]
    elif host == "jobs.ashbyhq.com" and len(parts) >= 2:
        plat, org, jid = "ashby", parts[0], parts[1]
    elif host in ("boards.greenhouse.io", "job-boards.greenhouse.io",
                  "boards.eu.greenhouse.io", "job-boards.eu.greenhouse.io"):
        if parts[:2] == ["embed", "job_app"]:
            q = parse_qs(sp.query)
            org = (q.get("for") or [""])[0].lower()
            jid = (q.get("token") or [""])[0]
            plat = "greenhouse"
        elif len(parts) >= 3 and parts[1] == "jobs":
            plat, org, jid = "greenhouse", parts[0], parts[2]
    elif host.endswith("smartrecruiters.com"):
        if "company" in parts and "publication" in parts:
            ci, pi = parts.index("company"), parts.index("publication")
            if ci + 1 < len(parts) and pi + 1 < len(parts):
                plat, org, jid = "smartrecruiters", parts[ci + 1], parts[pi + 1]
        elif len(parts) >= 2:
            d = _lead_digits(parts[1])
            if d:
                plat, org, jid = "smartrecruiters", parts[0], d
    elif host == "apply.workable.com" and len(parts) >= 3 and parts[1] == "j":
        plat, org, jid = "workable", parts[0], parts[2]
    elif host == "jobs.workable.com" and len(parts) >= 2 and parts[0] == "view":
        plat, org, jid = "workable", "view", parts[1]
    elif host.endswith(".recruitee.com") and len(parts) >= 2 and parts[0] == "o":
        plat, org, jid = "recruitee", host[:-len(".recruitee.com")], parts[1]
    elif host.endswith(".breezy.hr") and len(parts) >= 2 and parts[0] == "p":
        plat = "breezy"
        org, jid = host[:-len(".breezy.hr")], parts[1].split("-")[0]
    elif host.endswith(".bamboohr.com"):
        org = host[:-len(".bamboohr.com")]
        if len(parts) >= 2 and parts[0] == "careers":
            plat, jid = "bamboohr", parts[1]
        elif "view.php" in (parts[-1] if parts else ""):
            jid = (parse_qs(sp.query).get("id") or [""])[0]
            plat = "bamboohr" if jid else ""
    elif host.endswith(".applytojob.com") and len(parts) >= 2 \
            and parts[0] == "apply":
        plat, org, jid = "jazzhr", host.split(".")[0], parts[1]
    elif (".jobs.personio.de" in host or ".jobs.personio.com" in host) \
            and len(parts) >= 2 and parts[0] == "job":
        plat, org, jid = "personio", host.split(".")[0], parts[1]
    elif host.endswith(".teamtailor.com") and len(parts) >= 2 \
            and parts[0] == "jobs":
        d = _lead_digits(parts[1])
        if d:
            plat, org, jid = "teamtailor", host[:-len(".teamtailor.com")], d
    elif host == "ats.rippling.com" and len(parts) >= 3:
        # {org}/jobs/{uuid}, and a minority carry a locale prefix
        # ({locale}/{org}/jobs/{uuid}).  Locate the org by the "jobs" segment
        # rather than a fixed index, or the locale form is silently dropped.
        if parts[1] == "jobs":
            plat, org, jid = "rippling", parts[0], parts[2]
        elif len(parts) >= 4 and parts[2] == "jobs":
            plat, org, jid = "rippling", parts[1], parts[3]
    elif host.endswith(".rippling-ats.com") and len(parts) >= 3 \
            and parts[0] == "job":
        # Per-tenant host: {tenant}.rippling-ats.com/job/{numeric}/{slug}.
        # Disjoint from the path form above (different tenants, and integer
        # vs UUID ids), so this cannot collide with it — it just lifts ~98
        # rows out of tier 2.
        plat, org = "rippling", host[:-len(".rippling-ats.com")]
        jid = parts[1]
    elif ".myworkdayjobs.com" in host and "job" in parts:
        tail = parts[-1]
        if tail == "apply" and len(parts) >= 2:
            tail = parts[-2]
        jid = tail.rsplit("_", 1)[-1] if "_" in tail else tail
        plat, org = "workday", host.split(".")[0]
    elif (".myworkdaysite.com" in host and "job" in parts
            and len(parts) >= 3 and parts[0] == "recruiting"):
        # Workday's SECOND host shape: wd{N}.myworkdaysite.com/recruiting/
        # {tenant}/{site}/job/...  The tenant is a PATH segment here, not the
        # leftmost host label -- the leftmost label is the pod.  Reusing the
        # branch above would key every tenant on a pod as org="wd1", so two
        # unrelated employers sharing a requisition id like JR00925 would
        # merge into one row.  Same platform id ("workday") on purpose: a
        # board reachable on both hosts must still collapse to one job.
        tail = parts[-1]
        if tail == "apply" and len(parts) >= 2:
            tail = parts[-2]
        jid = tail.rsplit("_", 1)[-1] if "_" in tail else tail
        plat, org = "workday", parts[1]
    elif host.endswith(".csod.com") and "requisition" in parts:
        # Cornerstone: {tenant}.csod.com/ux/ats/careersite/{n}/home/
        # requisition/{id}[?c=tenant][#].  The careersite number and the ?c=
        # echo of the tenant are not identity, and the master's copies carry a
        # trailing '#' that defeats string equality outright -- so key on the
        # host label plus the requisition id, which is unique per tenant.
        i = parts.index("requisition")
        if i + 1 < len(parts):
            plat = "cornerstone"
            org = host[:-len(".csod.com")]
            jid = parts[i + 1]
    elif host == "governmentjobs.com" and len(parts) >= 4 \
            and parts[0] == "careers" and "jobs" in parts:
        # NEOGOV emits TWO url shapes for the same posting:
        #   /careers/{agency}/jobs/{id}/{slug}
        #   /careers/{agency}/index/jobs/{id}/{slug}
        # Measured 2026-08-20 in the master: 2,059 plain and 820 with /index/.
        # Url equality therefore splits one job in two, so the key is
        # (agency, numeric id) and the /index/ segment is ignored.
        i = parts.index("jobs")
        if i + 1 < len(parts):
            plat, org, jid = "neogov", parts[1], parts[i + 1]
    elif "dayforcehcm.com" in host and "jobs" in parts:
        # Dayforce: /[{locale}/]{client}/{site}/jobs/{id}.  The locale segment
        # is optional, which is exactly the shift that made this look like two
        # different corpora: 3,137 postings matched the master by id while only
        # 117 matched by URL -- 3,020 false uniques.  Locate by the "jobs"
        # segment rather than a fixed index, the same fix rippling needed.
        i = parts.index("jobs")
        if i >= 2 and i + 1 < len(parts):
            plat = "dayforce"
            org = "%s/%s" % (parts[i - 2], parts[i - 1])
            jid = parts[i + 1]
    elif host.endswith("careers-page.com"):
        # Manatal, two host shapes:
        #   www.careers-page.com/{slug}/job/{ID}      (www. already stripped)
        #   {slug}.careers-page.com/jobs/{uuid}
        # The two id spaces are disjoint (short code vs uuid), so keying each
        # on its own id cannot collide.
        if host == "careers-page.com" and len(parts) >= 3 and parts[1] == "job":
            plat, org, jid = "manatal", parts[0], parts[2]
        elif len(parts) >= 2 and parts[0] == "jobs":
            plat = "manatal"
            org = host[:-len(".careers-page.com")]
            jid = parts[1]
    elif (("ultipro.com" in host or host.endswith(".rec.pro.ukg.net"))
            and len(parts) >= 1):
        # UKG Pro.  The identity is opportunityId, NOT the URL: UKG serves the
        # same posting from numbered pods (recruiting.ultipro.com and
        # recruiting2.ultipro.com) and from per-tenant rec.pro.ukg.net hosts,
        # so url-equality makes one job look like two.  Measured 2026-08-20
        # against the new ukg pipe: 8,985 postings matched the master by
        # opportunityId but only 6,748 by URL -- 2,237 false uniques without
        # this branch.  org is the tenant CODE (path segment 0), which is
        # stable across pods; the JobBoard uuid is not part of identity.
        q = parse_qs(sp.query)
        oid = (q.get("opportunityId") or [""])[0].lower()
        if oid:
            plat, org, jid = "ukg", parts[0], oid
    elif host.endswith(".icims.com") and len(parts) >= 2 and parts[0] == "jobs":
        org = host.split(".")[0]
        for pre in ("careers-", "jobs-"):
            if org.startswith(pre):
                org = org[len(pre):]
        plat, jid = "icims", parts[1]
    # Zoho Recruit: {tenant}.zohorecruit.{dc}/jobs/{portal}/{id}/{slug}, and
    # the SAME shape on a CNAME'd customer domain (careers.abra.io,
    # jobs.simplesolutions.us) — 221 of 2,641 fetched rows, 8%, which a
    # host-anchored pattern misses entirely.
    #
    # Keyed on the id ALONE, with no org, because everything else about a Zoho
    # URL varies: the ?source= marker (CareerSite or RSS, depending which
    # route found it), the portal segment's case, which of eight data-centre
    # TLDs the customer sits on, and whether the link came via the vendor host
    # or the customer's own domain.  Two host forms of one job have to collapse
    # or the same posting is banked twice.
    #
    # Dropping the org is safe here specifically: Zoho record ids are 17-18
    # digits and encode the org, and across 86 boards and 2,641 rows there
    # were 0 ids shared by two boards.  The 15-digit floor plus the "jobs"
    # segment is what keeps the custom-domain arm from matching unrelated
    # sites that happen to share a /jobs/ path.
    elif (len(parts) >= 3 and parts[0] == "jobs"
            and parts[2].isdigit() and len(parts[2]) >= 15
            and (".zohorecruit." in host
                 or "source=careersite" in sp.query.lower())):
        plat, org, jid = "zoho", "", parts[2]
    elif host.endswith("linkedin.com") and len(parts) >= 3 \
            and parts[:2] == ["jobs", "view"]:
        d = _lead_digits(parts[2]) or parts[2]
        plat, org, jid = "linkedin", "", d
    elif host.endswith("indeed.com"):
        jk = (parse_qs(sp.query).get("jk") or [""])[0]
        if jk:
            plat, org, jid = "indeed", "", jk
    elif host in ("wellfound.com", "angel.co") and "company" in parts:
        ci = parts.index("company")
        if len(parts) > ci + 3 and parts[ci + 2] == "jobs":
            d = _lead_digits(parts[ci + 3])
            if d:
                plat, org, jid = "wellfound", parts[ci + 1], d
    elif host == "ycombinator.com" and len(parts) >= 4 \
            and parts[0] == "companies" and parts[2] == "jobs":
        # /companies/{slug}/jobs/{hashid}-{title-slug}
        # ONLY this form.  A posting also carries a numeric id, exposed as
        # account.ycombinator.com/...?signup_job_id=N, and the two namespaces
        # do not map to each other.  Keying on both would give one posting two
        # tier-1 keys, and tiers never cross-match — strictly worse than
        # leaving the account.* URL unparsed, which is what happens now.
        plat, org, jid = "ycwaas", parts[1], parts[3].split("-")[0]
    elif host in ("app.dover.com", "app.dover.io") and len(parts) >= 3 \
            and parts[0] == "apply":
        # /apply/{org}/{job_uuid}, where {org} is the company slug on some
        # links and the careers-page UUID on others — verified: the same job
        # UUID appears under both.  So the segment is deliberately EXCLUDED
        # from the key; the job UUID is globally unique on its own.
        plat, org, jid = "dover", "", parts[2]
    elif host.endswith(".notion.site") and parts:
        # {sub}.notion.site/{Some-Title-}{32hex}.  Keyed on the row page id
        # ALONE, like Dover: one posting is reachable both as a bare slug
        # (/sdr) and as Title-<hex>, and the id is globally unique.
        #
        # The query and fragment are already gone by the time we see `parts`,
        # which matters more here than anywhere else: vet_seen_keys.txt shows
        # ONE atlaskitchen.notion.site page exploded into 30 fabricated job
        # keys via #anchor fragments.  Lowercasing (parts is lowered above)
        # also collapses the corpus's case-variant duplicates
        # (/Head-of-Growth-... vs /head-of-growth-...).
        m = re.search(r"([0-9a-f]{32})$", parts[-1])
        if m:
            plat, org, jid = "notion", "", m.group(1)

    if plat and jid:
        return I(f"ats|{plat}|{org}|{jid}")
    return None


# Hosts whose job URLs are per-aggregator REDIRECTS, not employer identity.
# Tier 2 keys on the apply_url, which is right when the URL identifies the
# posting — but an aggregator mints its own id for a job it re-listed from
# somewhere else, so keying on it makes every row unique and guarantees the
# job never merges with the copy already held from its real ATS.  Measured
# 2026-07-29 on Adzuna: 12/12 sampled redirect_urls resolve back to
# adzuna.com itself (their T&Cs require the redirect and no employer URL is
# exposed anywhere in the schema), so 100% of its rows would have landed as
# re-labelled duplicates.  Dropping these hosts to tier 3 keys them on
# (company, title, location) instead, which DOES match the existing copy —
# while the URL itself is still kept in the row so it stays clickable.
AGGREGATOR_URL_HOSTS = (
    "adzuna.com", "jobviewtrack.com", "careerjet.com", "jooble.org",
    "neuvoo.com", "talent.com", "whatjobs.com", "jobrapido.com",
    "trovit.com", "mitula.com", "jobisjob.com", "careerbuilder.com",
)


def _is_aggregator_url(u):
    host = (urlsplit(u if "://" in u else "https://" + u.lstrip("/"))
            .hostname or "").lower()
    return any(host == h or host.endswith("." + h)
               for h in AGGREGATOR_URL_HOSTS)


def job_key(r):
    k = canon_key(r[IDX["apply_url"]]) or canon_key(r[IDX["job_url"]])
    if k:
        return k, 1
    comp = r[IDX["company"]].strip().lower()
    title = r[IDX["title"]].strip().lower()
    au = r[IDX["apply_url"]].strip().lower()
    if au and not _is_aggregator_url(au):
        return f"ct|{comp}|{title}|{au}", 2
    return I(f"cl|{comp}|{title}|{r[IDX['location']].strip().lower()}"), 3


# ---- loaders (one fixed-index row list per job) -----------------------------
def blank_row():
    r = [""] * (N + 4)
    r[SRCS], r[BRDS], r[PIPES], r[BASECNT] = set(), set(), set(), 0
    return r


_MORE = re.compile(r"\s*\(\+\d+ more\)$")


def load_getro(path):
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        i = {c: n for n, c in enumerate(next(rd))}
        for row in rd:
            title = row[i["title"]].strip()
            if not title:
                continue
            r = blank_row()
            r[IDX["company"]] = I(row[i["company"]])
            r[IDX["title"]] = title
            r[IDX["employment_type"]] = I(cc.title_type(title) or "")
            r[IDX["seniority"]] = I(row[i["seniority"]])
            wm = I(row[i["work_mode"]])
            r[IDX["work_mode"]] = wm
            r[IDX["remote"]] = "True" if wm == "Remote" else ""
            r[IDX["location"]] = I(row[i["location"]])
            r[IDX["posted"]] = I(row[i["posted"]])
            r[IDX["salary_min"]] = num(row[i["salary_min"]])
            r[IDX["salary_max"]] = num(row[i["salary_max"]])
            r[IDX["salary_currency"]] = I(row[i["salary_currency"]])
            r[IDX["salary_period"]] = I(row[i["salary_period"]])
            r[IDX["equity"]] = I(row[i["equity"]])
            r[IDX["skills"]] = I(row[i["skills"]])
            r[IDX["company_stage"]] = I(row[i["company_stage"]])
            r[IDX["company_size"]] = I(row[i["company_size"]])
            r[IDX["company_industry"]] = I(row[i["company_industry"]])
            r[IDX["apply_url"]] = row[i["apply_url"]]
            r[IDX["job_url"]] = row[i["board_job_url"]]
            r[IDX["source"]] = "getro"
            r[IDX["source_type"]] = "vc_board"
            r[SRCS] = {"getro"}
            r[BRDS] = {I(_MORE.sub("", b)) for b in
                       row[i["boards"]].split("; ") if b and "(+" not in b}
            r[PIPES] = {"getro"}
            try:
                r[BASECNT] = int(row[i["board_count"]] or 0)
            except ValueError:
                pass
            yield r


def load_lever(path):
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        i = {c: n for n, c in enumerate(next(rd))}
        for row in rd:
            title = row[i["title"]].strip()
            if not title:
                continue
            r = blank_row()
            commit = row[i["commitment"]].strip()
            dept = row[i["department"]].strip()
            team = row[i["team"]].strip()
            r[IDX["company"]] = I(row[i["company"]])
            r[IDX["title"]] = title
            r[IDX["employment_type"]] = I(
                cc.classify_any(commit, title) if commit
                else (cc.title_type(title) or ""))
            r[IDX["department"]] = I(dept + (" / " + team if team else ""))
            wm = I(row[i["work_mode"]])
            r[IDX["work_mode"]] = wm
            r[IDX["remote"]] = "True" if wm == "Remote" else ""
            r[IDX["location"]] = I(row[i["location"]])
            r[IDX["country"]] = I(row[i["country"]])
            r[IDX["posted"]] = I(row[i["posted"]])
            r[IDX["salary_min"]] = num(row[i["salary_min"]])
            r[IDX["salary_max"]] = num(row[i["salary_max"]])
            r[IDX["salary_currency"]] = I(row[i["salary_currency"]])
            r[IDX["salary_period"]] = I(row[i["salary_period"]])
            r[IDX["apply_url"]] = row[i["apply_url"]]
            r[IDX["job_url"]] = row[i["posting_url"]]
            r[IDX["source"]] = "lever"
            r[IDX["source_type"]] = "ats"
            r[SRCS] = {"lever"}
            r[BRDS] = {I(row[i["board"]])} if row[i["board"]] else set()
            r[PIPES] = {"lever"}
            r[BASECNT] = 1
            yield r


def load_ashby(path):
    """ashby_jobs_master.csv (ashby_build_sheet.py). The only ATS pipe with
    structured pay — Ashby's compensation API gives real min/max/currency/
    period, so the salary columns are not parsed from prose. company is the
    board slug (Ashby exposes no org display name); canon_key merges these
    with other pipelines' copies on the ats|ashby|org|id job_url key."""
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        i = {c: n for n, c in enumerate(next(rd))}
        for row in rd:
            title = row[i["title"]].strip()
            if not title:
                continue
            r = blank_row()
            et = row[i["employment_type"]].strip()
            dept = row[i["department"]].strip()
            team = row[i["team"]].strip()
            r[IDX["company"]] = I(row[i["company"]])
            r[IDX["title"]] = title
            r[IDX["employment_type"]] = I(et or cc.title_type(title) or "")
            r[IDX["department"]] = I(dept + (" / " + team if team else ""))
            wm = I(row[i["work_mode"]])
            r[IDX["work_mode"]] = wm
            r[IDX["remote"]] = "True" if wm == "Remote" else ""
            r[IDX["location"]] = I(row[i["location"]])
            r[IDX["city"]] = I(row[i["city"]])
            r[IDX["state"]] = I(row[i["state"]])
            r[IDX["country"]] = I(row[i["country"]])
            r[IDX["posted"]] = I(row[i["posted"]])
            r[IDX["salary_min"]] = num(row[i["salary_min"]])
            r[IDX["salary_max"]] = num(row[i["salary_max"]])
            r[IDX["salary_currency"]] = I(row[i["salary_currency"]])
            r[IDX["salary_period"]] = I(row[i["salary_period"]])
            r[IDX["apply_url"]] = row[i["apply_url"]]
            r[IDX["job_url"]] = row[i["job_url"]]
            r[IDX["source"]] = "ashby"
            r[IDX["source_type"]] = "ats"
            r[SRCS] = {"ashby"}
            r[BRDS] = {I(row[i["board"]])} if row[i["board"]] else set()
            r[PIPES] = {"ashby"}
            r[BASECNT] = 1
            yield r


def load_greenhouse(path):
    """greenhouse_jobs_master.csv (greenhouse_build_sheet.py). No structured
    salary/commitment/skills in Greenhouse's public API. job_url is the
    hosted board page — canon_key parses it into the same ats|greenhouse|…
    key as Getro's copies of the posting, even when apply_url is a custom
    company domain."""
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        i = {c: n for n, c in enumerate(next(rd))}
        for row in rd:
            title = row[i["title"]].strip()
            if not title:
                continue
            r = blank_row()
            r[IDX["company"]] = I(row[i["company"]])
            r[IDX["title"]] = title
            r[IDX["employment_type"]] = I(cc.title_type(title) or "")
            dept = row[i["department"]].strip()
            r[IDX["department"]] = I(dept)
            wm = I(row[i["work_mode"]])
            r[IDX["work_mode"]] = wm
            r[IDX["remote"]] = "True" if wm == "Remote" else ""
            r[IDX["location"]] = I(row[i["location"]].strip()
                                   or row[i["office"]].strip())
            r[IDX["posted"]] = I(row[i["posted"]])
            r[IDX["apply_url"]] = row[i["apply_url"]]
            jid = row[i["gh_job_id"]].strip()
            bu = row[i["board_url"]].strip()
            r[IDX["job_url"]] = f"{bu}/jobs/{jid}" if bu and jid else ""
            r[IDX["source"]] = "greenhouse"
            r[IDX["source_type"]] = "ats"
            r[SRCS] = {"greenhouse"}
            r[BRDS] = {I(row[i["board"]])} if row[i["board"]] else set()
            r[PIPES] = {"greenhouse"}
            r[BASECNT] = 1
            yield r


def _simple_loader(name, source_type, extra=()):
    """Reader factory for the four families whose master CSVs already use this
    schema's column names (rippling / personio / yc / dover).

    They differ only in which optional columns exist, so `extra` lists the
    (csv_column, schema_column) pairs to copy when present.  Each writes its
    own canonical job_url, so canon_key merges its rows with the copies Getro
    syndicates without any per-family logic here.
    """
    def loader(path):
        with open(path, newline="", encoding="utf-8") as f:
            rd = csv.reader(f)
            i = {c: n for n, c in enumerate(next(rd))}

            def cell(row, col):
                j = i.get(col)
                return row[j].strip() if j is not None and j < len(row) else ""

            for row in rd:
                title = cell(row, "title")
                if not title:
                    continue
                r = blank_row()
                r[IDX["company"]] = I(cell(row, "company"))
                r[IDX["title"]] = title
                r[IDX["employment_type"]] = I(cell(row, "employment_type")
                                              or cc.title_type(title) or "")
                wm = I(cell(row, "work_mode"))
                r[IDX["work_mode"]] = wm
                r[IDX["remote"]] = "True" if wm == "Remote" else ""
                r[IDX["location"]] = I(cell(row, "location"))
                r[IDX["city"]] = I(cell(row, "city"))
                r[IDX["state"]] = I(cell(row, "state"))
                r[IDX["country"]] = I(cell(row, "country"))
                r[IDX["department"]] = I(cell(row, "department"))
                r[IDX["posted"]] = I(cell(row, "posted"))
                r[IDX["salary_min"]] = num(cell(row, "salary_min"))
                r[IDX["salary_max"]] = num(cell(row, "salary_max"))
                r[IDX["salary_currency"]] = I(cell(row, "salary_currency"))
                r[IDX["salary_period"]] = I(cell(row, "salary_period"))
                for src_col, dst_col in extra:
                    v = cell(row, src_col)
                    if v:
                        r[IDX[dst_col]] = I(v)
                r[IDX["apply_url"]] = cell(row, "apply_url")
                r[IDX["job_url"]] = cell(row, "job_url") or cell(row,
                                                                 "apply_url")
                r[IDX["source"]] = name
                r[IDX["source_type"]] = source_type
                r[SRCS] = {name}
                board = cell(row, "board")
                r[BRDS] = {I(board)} if board else set()
                r[PIPES] = {name}
                r[BASECNT] = 1
                yield r
    loader.__name__ = "load_" + name
    return loader


load_rippling = _simple_loader("rippling", "ats")
load_personio = _simple_loader("personio", "ats",
                               extra=(("seniority", "seniority"),))
load_yc = _simple_loader(
    "yc", "startup_board",
    extra=(("equity", "equity"), ("company_stage", "company_stage"),
           ("company_size", "company_size"),
           ("company_industry", "company_industry")))
load_dover = _simple_loader("dover", "ats", extra=(("equity_min", "equity"),))
load_notion = _simple_loader("notion", "ats")


def load_portals(path):
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        i = {c: n for n, c in enumerate(next(rd))}
        for row in rd:
            title = row[i["title"]].strip()
            if not title:
                continue
            r = blank_row()
            src = I(row[i["source"]])
            r[IDX["company"]] = I(row[i["company"]])
            r[IDX["title"]] = title
            r[IDX["employment_type"]] = I(row[i["employment_type"]])
            wm = I(row[i["work_mode"]])
            r[IDX["work_mode"]] = wm
            r[IDX["remote"]] = "True" if (row[i["remote"]] == "True"
                                          or wm == "Remote") else ""
            r[IDX["location"]] = I(row[i["location"]])
            r[IDX["city"]] = I(row[i["city"]])
            r[IDX["state"]] = I(row[i["state"]])
            r[IDX["country"]] = I(row[i["country"]])
            r[IDX["posted"]] = I(row[i["posted"]])
            r[IDX["salary_min"]] = num(row[i["salary_min"]])
            r[IDX["salary_max"]] = num(row[i["salary_max"]])
            r[IDX["salary_currency"]] = I(row[i["salary_currency"]])
            r[IDX["salary_period"]] = I(row[i["salary_period"]])
            r[IDX["apply_url"]] = row[i["apply_url"]]
            r[IDX["source"]] = src
            r[IDX["source_type"]] = I(row[i["source_type"]])
            # .get on the header map, not row[i[...]]: a portals CSV built
            # before this column existed has no such key, and a KeyError here
            # would take down the whole build over an optional field.
            if "delisted_on" in i and i["delisted_on"] < len(row):
                r[IDX["delisted_on"]] = I(row[i["delisted_on"]])
            srcs = row[i["sources"]]
            r[SRCS] = ({I(s) for s in srcs.split(", ") if s} if srcs
                       else {src})
            b = row[i["board"]]
            r[BRDS] = {I(f"{src}:{b}")} if b else set()
            r[PIPES] = {"portals"}
            r[BASECNT] = 1
            yield r


# ---- merge ------------------------------------------------------------------
FILL_IF_EMPTY = [IDX[k] for k in (
    "company", "employment_type", "seniority", "department", "work_mode",
    "remote", "location", "city", "state", "country", "salary_min",
    "salary_max", "salary_currency", "salary_period", "equity", "skills",
    "company_stage", "company_size", "company_industry", "apply_url",
    "job_url", "source_type")]


def merge(a, b):
    # LIVE WINS, and it is deliberately not in FILL_IF_EMPTY.  "Live" is
    # encoded as the EMPTY string, so fill-if-empty would do exactly the wrong
    # thing: a delisted copy would fill a live row's blank and bury a job that
    # is still open on another board.  A job seen live anywhere is live.
    if not b[IDX["delisted_on"]]:
        a[IDX["delisted_on"]] = ""
    elif a[IDX["delisted_on"]] and b[IDX["delisted_on"]] > a[IDX["delisted_on"]]:
        # Both delisted: keep the LATEST sighting-of-absence, which is the
        # most recent evidence.
        a[IDX["delisted_on"]] = b[IDX["delisted_on"]]
    a[SRCS] |= b[SRCS]
    a[BRDS] |= b[BRDS]
    a[PIPES] |= b[PIPES]
    if b[BASECNT] > a[BASECNT]:
        a[BASECNT] = b[BASECNT]
    pa, pb = a[IDX["posted"]], b[IDX["posted"]]
    if pb and (not pa or pb < pa):
        a[IDX["posted"]] = pb
    for i in FILL_IF_EMPTY:
        if a[i] == "" and b[i] != "":
            a[i] = b[i]


# ---- pipeline (Remote + MA/RI/South-FL) region logic ------------------------
SFL_BARE = [
    "miami", "fort lauderdale", "boca raton", "coral gables", "delray beach",
    "boynton beach", "pompano beach", "deerfield beach", "hialeah", "doral",
    "aventura", "coral springs", "pembroke pines", "hallandale beach",
    "dania beach", "lauderhill", "tamarac", "opa-locka", "cutler bay",
    "palmetto bay", "sunny isles", "key biscayne", "wilton manors",
    "palm beach", "coconut creek", "cooper city", "juno beach", "tequesta",
    "miami-dade", "broward", "south florida",
]
SFL_NEED_FL = [
    "hollywood", "plantation", "sunrise", "davie", "weston", "miramar",
    "margate", "parkland", "homestead", "kendall", "jupiter", "wellington",
    "lake worth", "greenacres", "lantana", "riviera beach", "oakland park",
]
SFL_BARE_RE = re.compile(r"\b(" + "|".join(re.escape(c) for c in SFL_BARE)
                         + r")\b")
SFL_FL_RE = re.compile(r"\b(" + "|".join(re.escape(c) for c in SFL_NEED_FL)
                       + r"),?\s+(fl|florida)\b")
MA_RE = re.compile(r"massachusetts|,\s*ma\b|\bboston\b|greater boston")
RI_RE = re.compile(r"rhode island|,\s*ri\b|\bprovidence\b")
REGION_RANK = {"MA": 1, "RI": 2, "South FL": 3, "FL (statewide tag)": 4}


def statewide_fl(l):
    for seg in l.split(";"):
        first = seg.split(",", 1)[0].strip()
        if first in ("florida", "fl") or first.endswith("- florida") \
                or first.endswith("- fl"):
            return True
    return False


def regions_of(loc, state, country):
    if country and country.upper() not in ("US", "USA", "UNITED STATES", ""):
        return []
    l = (loc + " " + (state or "")).lower()
    out = []
    st = (state or "").strip().lower()
    if MA_RE.search(l) or st in ("ma", "massachusetts"):
        out.append("MA")
    if RI_RE.search(l) or st in ("ri", "rhode island"):
        out.append("RI")
    if SFL_BARE_RE.search(l) or SFL_FL_RE.search(l):
        out.append("South FL")
    elif "florida" in l and statewide_fl(l):
        out.append("FL (statewide tag)")
    return out


def is_remote(r):
    return (r[IDX["remote"]] == "True" or r[IDX["work_mode"]] == "Remote"
            or "remote" in r[IDX["location"]].lower())


# ---- workbook helpers -------------------------------------------------------
def hdr(ws, names, bold):
    out = []
    for nm in names:
        c = WriteOnlyCell(ws, value=nm)
        c.font = bold
        out.append(c)
    return out


def data_tabs(wb, base_name, columns, widths, table, bold, cap=XLSX_CAP):
    """Write `table` (list of value-lists) across '<name>', '<name> 2', ...
    so no tab exceeds the Excel row cap. Returns the tab names written."""
    names = []
    total = len(table)
    chunks = max(1, -(-total // cap)) if total else 1
    for ci in range(chunks):
        name = base_name if ci == 0 else f"{base_name} {ci + 1}"
        ws = wb.create_sheet(name)
        for j, wd in enumerate(widths[:len(columns)], 1):
            ws.column_dimensions[get_column_letter(j)].width = wd
        ws.freeze_panes = "A2"
        chunk = table[ci * cap:(ci + 1) * cap]
        ws.auto_filter.ref = (f"A1:{get_column_letter(len(columns))}"
                              f"{len(chunk) + 1}")
        ws.append(hdr(ws, columns, bold))
        for vals in chunk:
            ws.append(vals)
        names.append(name)
    return names


def readme_tab(wb, bold, lines):
    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 116
    for a, b in lines:
        ca = WriteOnlyCell(ws, value=a)
        ca.font = bold
        cb = WriteOnlyCell(ws, value=b)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        ws.append([ca, cb])


# ---- main -------------------------------------------------------------------
def main():
    csv.field_size_limit(10 ** 7)
    inputs = [("getro", GETRO_CSV, load_getro),
              ("lever", LEVER_CSV, load_lever),
              ("greenhouse", GREENHOUSE_CSV, load_greenhouse),
              ("ashby", ASHBY_CSV, load_ashby),
              # First-party ATS feeds sit ahead of the heterogeneous portals
              # crawl for the same reason greenhouse/ashby do: where both see
              # a posting, the richer first-party record should own its
              # identity.  They stay AFTER getro deliberately — reordering
              # would hand them first-writer rights over title/source on
              # 1.1M rows to reclaim attribution on ~5k.
              ("rippling", RIPPLING_CSV, load_rippling),
              ("personio", PERSONIO_CSV, load_personio),
              ("yc", YC_CSV, load_yc),
              ("dover", DOVER_CSV, load_dover),
              # notion sits here for the same reason, and the gap is unusually
              # wide: Getro carries ~1,063 notion.site rows as bare apply-URL
              # residue — including 340 from a VC READING LIST it mislabelled
              # as jobs — while the notion pipe reads the posting itself and
              # brings a REAL created date with it.
              ("notion", NOTION_CSV, load_notion),
              ("portals", PORTALS_CSV, load_portals)]

    by_key = {}
    raw_per = {}
    tier_hits = {1: 0, 2: 0, 3: 0}
    tier_merges = {1: 0, 2: 0, 3: 0}
    for pname, path, loader in inputs:
        if not os.path.exists(path):
            print(f"WARNING: {os.path.basename(path)} missing — "
                  f"{pname} not included", flush=True)
            continue
        n = 0
        for r in loader(path):
            n += 1
            if n % 250_000 == 0:
                print(f"  ...{pname} {n} rows", flush=True)
            k, tier = job_key(r)
            tier_hits[tier] += 1
            prev = by_key.get(k)
            if prev is not None:
                merge(prev, r)
                tier_merges[tier] += 1
            else:
                r[IDX["job_key"]] = k
                by_key[k] = r
        raw_per[pname] = n
        print(f"loaded {pname}: {n} rows (unified so far {len(by_key)})",
              flush=True)

    rows = list(by_key.values())
    by_key = None
    for r in rows:
        names = sorted(r[BRDS])
        r[IDX["board_count"]] = max(r[BASECNT], len(names))
        r[IDX["boards"]] = "; ".join(names[:6]) + (
            f" (+{len(names) - 6} more)" if len(names) > 6 else "")
        srcs = sorted(r[SRCS])
        r[IDX["sources"]] = ", ".join(srcs)
        r[IDX["n_sources"]] = len(srcs)
    n_cut = n_undated = 0
    if POSTED_SINCE:
        pre = len(rows)
        rows = [r for r in rows
                if not r[IDX["posted"]] or r[IDX["posted"]] >= POSTED_SINCE]
        n_cut = pre - len(rows)
        n_undated = sum(1 for r in rows if not r[IDX["posted"]])
        print(f"posted-date cutoff {POSTED_SINCE}: cut {n_cut} older jobs, "
              f"kept {len(rows)} ({n_undated} undated kept)", flush=True)
    rows.sort(key=lambda r: (r[IDX["posted"]] or "", r[IDX["n_sources"]]),
              reverse=True)
    total_raw = sum(raw_per.values())
    cross = sum(1 for r in rows if len(r[PIPES]) > 1)
    print(f"raw {total_raw} -> unified {len(rows)} "
          f"(merges: canonical-ats {tier_merges[1]}, company+title+url "
          f"{tier_merges[2]}, company+title+location {tier_merges[3]}; "
          f"{cross} rows span 2+ pipelines)", flush=True)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for r in rows:
            w.writerow(r[:N])
    print(f"wrote {OUT_CSV}", flush=True)
    if CSV_ONLY:
        print("--csv-only: skipping workbook", flush=True)
        return

    # ---- split: only when one tab can't hold everything do the
    # PDL-firehose-only rows move to their own tab
    split = len(rows) > XLSX_CAP
    if split:
        eco, pdl = [], []
        for r in rows:
            (pdl if (r[SRCS] == {"getro"} and r[BRDS] == {PDL_BOARD})
             else eco).append(r)
        print(f"split: {len(eco)} ecosystem rows, {len(pdl)} PDL-only rows",
              flush=True)
    else:
        eco, pdl = rows, []
        print(f"no split needed: {len(rows)} rows fit one tab", flush=True)

    # ---- pipeline selection (Remote + MA/RI/South-FL)
    pipe_rows = []
    n_rem = n_loc = 0
    for r in rows:
        regs = regions_of(r[IDX["location"]], r[IDX["state"]],
                          r[IDX["country"]])
        if is_remote(r):
            pipe_rows.append((0, REGION_RANK.get(regs[0], 9) if regs else 9,
                              r[IDX["posted"]], "Remote", "; ".join(regs), r))
            n_rem += 1
        elif r[IDX["work_mode"]] in ("On-site", "", "Hybrid") and regs:
            pipe_rows.append((1, REGION_RANK.get(regs[0], 9),
                              r[IDX["posted"]], "Local", "; ".join(regs), r))
            n_loc += 1
    pipe_rows.sort(key=lambda t: t[2], reverse=True)
    pipe_rows.sort(key=lambda t: (t[0], t[1]))
    print(f"pipeline rows: {len(pipe_rows)} ({n_rem} remote, {n_loc} local)",
          flush=True)

    # ---- summaries
    by_pipe, by_source, by_type, by_wm = {}, {}, {}, {}
    by_feed = {}
    for r in rows:
        pk = " + ".join(sorted(r[PIPES]))
        by_pipe[pk] = by_pipe.get(pk, 0) + 1
        s = r[IDX["source"]] or "(none)"
        by_source[s] = by_source.get(s, 0) + 1
        # "by source" credits only the FIRST loader that produced a row, and
        # getro runs first with ~88% of the corpus, so a later family looks
        # far smaller than the work it actually did.  by_feed counts a job
        # once per feed that carried it, which is the honest figure.
        for s2 in r[SRCS]:
            by_feed[s2] = by_feed.get(s2, 0) + 1
        et = r[IDX["employment_type"]] or "(not stated)"
        by_type[et] = by_type.get(et, 0) + 1
        wm = r[IDX["work_mode"]] or "(not stated)"
        by_wm[wm] = by_wm.get(wm, 0) + 1
    board_stats = {}
    for r in rows:
        for bn in r[BRDS]:
            st = board_stats.setdefault(bn, [0, ""])
            st[0] += 1
            if (r[IDX["posted"]] or "") > st[1]:
                st[1] = r[IDX["posted"]] or ""

    # ---- workbook (Pipeline FIRST: the dropdown splice targets sheet1.xml)
    wb = Workbook(write_only=True)
    bold = Font(bold=True)

    pcols = ["flag", "notes", "bucket", "region"] + COLUMNS
    ws = wb.create_sheet("Pipeline")
    for j, wd in enumerate([11, 24, 9, 18] + WIDTHS, 1):
        ws.column_dimensions[get_column_letter(j)].width = wd
    ws.freeze_panes = "E2"
    n_pipe = len(pipe_rows)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(pcols))}{n_pipe + 1}"
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE",
                        fill_type="solid")
    ws.conditional_formatting.add(
        f"A2:{get_column_letter(len(pcols))}{n_pipe + 1}",
        FormulaRule(formula=['$A2="Interested"'], fill=green,
                    stopIfTrue=False))
    ws.append(hdr(ws, pcols, bold))
    for _, _, _, bucket, region, r in pipe_rows:
        ws.append(["", "", bucket, region] + [safe(v) for v in r[:N]])

    n_remote_all = sum(1 for r in rows if r[IDX["remote"]] == "True")
    lines = [
        ("ALL JOBS — every pipeline, one workbook", ""),
        ("Snapshot (UTC)", SNAP),
        ("", ""),
        ("Unique jobs (deduped)", len(rows)),
        ("Raw rows merged", total_raw),
        ("  from Getro master", raw_per.get("getro", 0)),
        ("  from Lever master", raw_per.get("lever", 0)),
        # greenhouse/ashby were missing here, so the sub-lines never summed to
        # "Raw rows merged"; the four new families are added for the same
        # reason.
        ("  from Greenhouse master", raw_per.get("greenhouse", 0)),
        ("  from Ashby master", raw_per.get("ashby", 0)),
        ("  from Rippling master", raw_per.get("rippling", 0)),
        ("  from Personio master", raw_per.get("personio", 0)),
        ("  from YC (Work at a Startup) master", raw_per.get("yc", 0)),
        ("  from Dover master", raw_per.get("dover", 0)),
        ("  from Notion (notion.site) master", raw_per.get("notion", 0)),
        ("  from FT+contract portals master", raw_per.get("portals", 0)),
        ("Seen in 2+ pipelines", cross),
        ("Remote", n_remote_all),
        ("", ""),
        ("Posted-date cutoff", (f"Jobs first seen before {POSTED_SINCE} are "
         f"excluded ({n_cut} cut this snapshot). {n_undated} rows with no "
         "date are kept — their sources (Phenom, Bullhorn, some BambooHR/"
         "Recruitee feeds) never publish dates, so blank means unknown, not "
         "old. Rebuild with --since YYYY-MM-DD to move the line, or "
         "--all-dates for everything ever seen.") if POSTED_SINCE
         else "None — every job ever seen is included (--all-dates)."),
        ("", ""),
        ("What this is", "Every job from every crawl, merged and deduplicated: "
         "the full Getro VC/network-board universe, every live public Lever "
         "board, and the FT+contract portal crawl (Bullhorn staffing agencies, "
         "Ashby/SmartRecruiters/Recruitee/Breezy/BambooHR first-party boards, "
         "Allegis brands on Phenom, freelance marketplaces and remote boards)."),
        ("Pipeline tab", "The actionable slice: every Remote role plus every "
         "on-site/hybrid role in MA, RI, or the Miami / Fort Lauderdale / "
         "West Palm Beach metros. Pick Interested / Maybe / No in the flag "
         "column (dropdown); Interested rows turn green. Sorted Remote first, "
         "then Local, newest first."),
        ("All Jobs tab", "Every job in one tab." if not split else
         "Excel caps a tab at 1,048,576 rows, so the workbook is split to "
         "still hold everything: 'All Jobs' = every job except those seen "
         "ONLY on Getro's People Data Labs firehose board (a LinkedIn-scale "
         "aggregator); 'PDL Jobs' = that firehose. A job on PDL *and* any "
         "real board counts as ecosystem and sits in All Jobs."),
        ("How deduping works", "The same posting is one row even when it "
         "appears in several crawls. Match order: (1) canonical ATS identity "
         "parsed from the apply URL — platform + org + job id, which survives "
         "cosmetic URL differences like /apply suffixes or boards. vs "
         "job-boards.greenhouse.io; (2) company + title + apply URL; "
         "(3) company + title + location when there is no URL. 'sources' / "
         "'n_sources' list every feed that carried the job."),
        ("job_key", "The dedupe key. It is stable across snapshots — match on "
         "it to carry your flags/notes into the next refresh."),
        ("employment_type", "Full-time / Part-time / Internship or a "
         "contingent label (Contract, Temp, Freelance, ...). Blank = the "
         "source stated nothing and the title gave no signal (most Getro "
         "rows: Getro has no employment-type field)."),
        ("posted", "Earliest date any source saw the job (first-seen)."),
        ("boards / board_count", "Every board carrying the job. Getro board "
         "names are plain; portal boards are prefixed 'source:board'."),
        ("Master CSV", "all_jobs_master.csv holds the complete table (this "
         "workbook's tabs are views of it)."),
        ("Refresh", "python3 run_all.py  (full re-fetch + rebuild; hours). "
         "python3 run_all.py --build-only rebuilds sheets from data on disk. "
         "See run_all.py --help for --discover / --resume / --only / --skip."),
    ]
    readme_tab(wb, bold, lines)

    eco_table = [[safe(v) for v in r[:N]] for r in eco]
    data_tabs(wb, "All Jobs", COLUMNS, WIDTHS, eco_table, bold)
    eco_table = None
    if split and not NO_PDL:
        pdl_table = [[safe(v) for v in r[:N]] for r in pdl]
        data_tabs(wb, "PDL Jobs", COLUMNS, WIDTHS, pdl_table, bold)
        pdl_table = None

    ws = wb.create_sheet("Summary")
    for col, wd in zip("AB", (34, 14)):
        ws.column_dimensions[col].width = wd
    ws.append(hdr(ws, ["jobs by pipeline overlap", "jobs"], bold))
    for k, v in sorted(by_pipe.items(), key=lambda kv: -kv[1]):
        ws.append([k, v])
    ws.append([])
    ws.append(hdr(ws, ["by source (first pipeline to claim the row)", "jobs"],
                  bold))
    for k, v in sorted(by_source.items(), key=lambda kv: -kv[1]):
        ws.append([k, v])
    ws.append([])
    ws.append(hdr(ws, ["by feed (a job counts once per feed that carried it)",
                       "jobs"], bold))
    for k, v in sorted(by_feed.items(), key=lambda kv: -kv[1]):
        ws.append([k, v])
    ws.append([])
    ws.append(hdr(ws, ["by employment_type", "jobs"], bold))
    for k, v in sorted(by_type.items(), key=lambda kv: -kv[1]):
        ws.append([k, v])
    ws.append([])
    ws.append(hdr(ws, ["by work_mode", "jobs"], bold))
    for k, v in sorted(by_wm.items(), key=lambda kv: -kv[1]):
        ws.append([k, v])
    ws.append([])
    ws.append([WriteOnlyCell(ws, value="TOTAL unique"), len(rows)])

    ws = wb.create_sheet("Sources")
    for col, wd in zip("ABCDEFG", (22, 16, 26, 30, 12, 60, 60)):
        ws.column_dimensions[col].width = wd
    ws.freeze_panes = "A2"
    ws.append(hdr(ws, build_sources.COLS, bold))
    ws.append(["getro", "vc_board", "open API",
               "title keywords only (no type field)", "harvested",
               "api.getro.com/api/v2/collections/{id}/search/jobs",
               "~768 live VC/accelerator network boards; full universe here "
               "and in getro_jobs_master.csv"])
    ws.append(["lever", "ats", "open API", "commitment field + title",
               "harvested", "api.lever.co/v0/postings/{slug}",
               "~2,842 live public boards; full universe here and in "
               "lever_jobs_master.csv"])
    for sr in build_sources.rows():
        ws.append([safe(sr[c]) for c in build_sources.COLS])

    ws = wb.create_sheet("Boards")
    for col, wd in zip("ABC", (52, 10, 12)):
        ws.column_dimensions[col].width = wd
    ws.freeze_panes = "A2"
    top = sorted(board_stats.items(), key=lambda kv: -kv[1][0])[:20000]
    ws.auto_filter.ref = f"A1:C{len(top) + 1}"
    ws.append(hdr(ws, ["board", "jobs", "newest_post"], bold))
    for bn, (n_, newest) in top:
        ws.append([safe(bn), n_, newest])

    wb.save(OUT_XLSX)
    print(f"wrote {OUT_XLSX}", flush=True)

    # ---- splice the flag dropdown into the Pipeline sheet XML
    dv_xml = (f'<dataValidations count="1">'
              f'<dataValidation type="list" allowBlank="1" '
              f'showErrorMessage="0" sqref="A2:A{n_pipe + 1}">'
              f'<formula1>"{FLAG_CHOICES}"</formula1>'
              f'</dataValidation></dataValidations>')
    tmp = OUT_XLSX + ".tmp"
    with zipfile.ZipFile(OUT_XLSX) as zin, \
            zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename == "xl/worksheets/sheet1.xml":
                xml = data.decode("utf-8")
                anchor = xml.rfind("</conditionalFormatting>")
                if anchor == -1:
                    raise SystemExit("conditionalFormatting anchor missing")
                pos = anchor + len("</conditionalFormatting>")
                xml = xml[:pos] + dv_xml + xml[pos:]
                data = xml.encode("utf-8")
                print("dropdown spliced into sheet1", flush=True)
            zout.writestr(item, data)
    os.replace(tmp, OUT_XLSX)

    with zipfile.ZipFile(OUT_XLSX) as z:
        xml = z.read("xl/worksheets/sheet1.xml").decode("utf-8")
    i_dv = xml.find("<dataValidations")
    i_cf = xml.rfind("</conditionalFormatting>")
    i_pm = xml.find("<pageMargins")
    ok = (i_dv != -1 and i_cf != -1 and i_dv > i_cf
          and (i_pm == -1 or i_dv < i_pm)
          and xml.count("<dataValidations") == 1)
    print(f"verify: dropdown present and schema-ordered: {ok}", flush=True)
    if not ok:
        raise SystemExit("dataValidations splice verification failed")


if __name__ == "__main__":
    main()
