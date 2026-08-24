#!/usr/bin/env python3
"""
contract_common.py — shared helpers for the contract-gig pipeline.

Canonical normalized row (dict) every harvester emits, one JSON per line, into
contract_norm/<source>.jsonl:

  source          short source id            e.g. "ashby", "phenom", "bullhorn"
  source_type     family                     "staffing_ats" | "marketplace" |
                                             "remote_board" | "vc_board" |
                                             "agency" | "ats"
  company         employer / agency          str
  title           role title                 str
  employment_type normalized contingent tag  Contract|Temp|Freelance|
                                             Contract-to-Hire|Interim|Seasonal|
                                             Per Diem|Fractional|Contract(?)
  work_mode       Remote|Hybrid|On-site|""
  location        freeform location string
  city, state, country
  remote          bool
  salary_min, salary_max (numbers or "")
  salary_currency, salary_period
  posted          YYYY-MM-DD or ""
  apply_url, job_id, board, board_url
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
NORM_DIR = os.path.join(BASE, "contract_norm")
os.makedirs(NORM_DIR, exist_ok=True)

# ---- which files in contract_norm/ to read --------------------------------
# The Jul-19 contract-only harvests.  Each has a KEEP_ALL successor covering
# the same boards plus every other employment type, so reading both merges a
# fortnight-dead posting into a live one and hands it the fresh row's date.
SUPERSEDED = {
    "ats_ashby": "ats_all_ashby",
    "ats_smartrecruiters": "ats_all_smartrecruiters",
    "ats_recruitee": "ats_all_recruitee",
    "ats_breezy": "ats_all_breezy",
    "ats_bamboohr": "ats_all_bamboohr",
    "bullhorn": "bullhorn_all",
    "phenom": "phenom_all",
    "boards": "boards_all",
}


def norm_files(norm_dir=None):
    """The newest generation of every normalized source, one file per source.

    Generation is a SEPARATE question from employment type.  Nothing here
    decides what is contingent -- callers do that per row with classify(),
    because filenames stopped carrying that meaning on 2026-07-29 when ~25 new
    pipes began writing full-employment-type dumps under names with no `_all`
    suffix (workday, careerjet, oracle, icims, jobgether, themuse, adzuna...).
    """
    import glob

    norm_dir = norm_dir or NORM_DIR
    stems = {}
    for path in sorted(glob.glob(os.path.join(norm_dir, "*.jsonl"))):
        stem = os.path.basename(path)[:-len(".jsonl")]
        if stem.endswith(".prev"):
            continue
        stems[stem] = path
    return [p for stem, p in sorted(stems.items())
            if SUPERSEDED.get(stem) not in stems]

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

COLUMNS = [
    "source", "source_type", "company", "title", "employment_type",
    "work_mode", "location", "city", "state", "country", "remote",
    "salary_min", "salary_max", "salary_currency", "salary_period",
    "posted", "apply_url", "job_id", "board", "board_url",
    # Set by ats_fetch's ATS_DELIST_MODE=flag merge: the date a posting stopped
    # appearing on a board that was authoritatively re-fetched.  Empty means
    # live.  Appended LAST so every positional reader of this list is unmoved.
    "delisted_on",
]

# ---- contract/temp classification -----------------------------------------
# Map any raw employment-type / commitment string to a normalized contingent
# label, or None if it is not contingent work.
_TYPE_MAP = [
    (re.compile(r"contract[\s_-]*to[\s_-]*hire|c2h|temp[\s_-]*to[\s_-]*perm|"
                r"temp[\s_-]*to[\s_-]*hire", re.I), "Contract-to-Hire"),
    (re.compile(r"fractional", re.I), "Fractional"),
    (re.compile(r"freelance|freelancer|independent contractor|self[\s-]*employed",
                re.I), "Freelance"),
    (re.compile(r"per[\s_-]*diem|locum", re.I), "Per Diem"),
    (re.compile(r"interim", re.I), "Interim"),
    (re.compile(r"season", re.I), "Seasonal"),
    (re.compile(r"temp\b|temporary|fixed[\s_-]*term|fixed term|befristet|"
                r"\bcdd\b|interim|casual|\bzzp\b", re.I), "Temp"),
    (re.compile(r"contract|contractor|contractual|contingent|1099|c2c|"
                r"corp[\s_-]*to[\s_-]*corp|day[\s_-]*rate|\bgig\b|consult",
                re.I), "Contract"),
]
# permanent-contract traps: a "contract" token that means a permanent
# employment contract, never contingent work.
_PERM_TRAP = re.compile(
    r"\bcdi\b|permanent contract|contrat permanent|unbefristet|"
    r"vast contract|vaste overeenkomst|festanstellung|contrato indefinido|"
    r"contrato permanente|permanent employment|regular full[\s-]*time|"
    r"fulltime_permanent|full[\s-]*time permanent|permanent full[\s-]*time",
    re.I)


def norm_employment_type(raw):
    """Normalize a raw type/commitment string -> contingent label or None."""
    if not raw:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if _PERM_TRAP.search(s):
        return None
    for rx, label in _TYPE_MAP:
        if rx.search(s):
            return label
    return None


# Title-based detection (fallback when a source has no type field).
_TITLE = re.compile(
    r"\b(freelance|freelancer|contractor|temporary|temp|interim|"
    r"seasonal|locum|per[\s-]*diem|fixed[\s-]*term|fractional|"
    r"contract[\s-]*to[\s-]*hire|1099|c2c)\b"
    r"|\(contract\)|\bcontract\s*[:-]|\bon contract\b|contract role|"
    r"contract position|contract opportunity|- contract\b|, contract\b",
    re.I)
_TITLE_FP = re.compile(
    r"contract manager|contract managers|contract administ|contract analyst|"
    r"contract specialist|contract coordinat|contract negotiat|"
    r"contract attorney|contract counsel|contract lawyer|contract law|"
    r"contracts? engineer|contract review|contract lifecycle|"
    r"smart contract|contract logistics|government contract",
    re.I)
_TITLE_STRONG = re.compile(
    r"\bfreelance|\bcontractor\b|\btemporary\b|\binterim\b|\bseasonal\b|"
    r"\blocum\b|\bfractional\b|\(contract\)|contract[\s-]*to[\s-]*hire",
    re.I)


def title_type(title):
    """Return a contingent label inferred from the title, or None."""
    t = title or ""
    if _TITLE_FP.search(t) and not _TITLE_STRONG.search(t):
        return None
    if _TITLE.search(t):
        return norm_employment_type(t) or "Contract"
    return None


def classify(raw_type, title):
    """Best contingent label from an explicit type first, then the title."""
    return norm_employment_type(raw_type) or title_type(title)


# ---- full employment classification (INCLUDES full-time) -------------------
# For the FT+contract crawl: never returns None; falls back to "Full-time".
_FT_RX = re.compile(r"full[\s_-]*time|permanent|\bregular\b|\bfte\b|\bcdi\b|"
                    r"festanstellung|unbefristet", re.I)
_PT_RX = re.compile(r"part[\s_-]*time", re.I)
_INTERN_RX = re.compile(r"\bintern\b|internship|apprentic|werkstudent|praktik",
                        re.I)
_VOL_RX = re.compile(r"volunteer|voluntary", re.I)


def classify_any(raw_type, title):
    """Employment label including full-time roles. Contingent types keep their
    fine-grained label; everything else resolves to Full-time / Part-time /
    Internship / Volunteer, defaulting to Full-time when there is no signal."""
    c = classify(raw_type, title)
    if c:
        return c
    s = str(raw_type or "")
    if _INTERN_RX.search(s):
        return "Internship"
    if _PT_RX.search(s):
        return "Part-time"
    if _VOL_RX.search(s):
        return "Volunteer"
    if _FT_RX.search(s):
        return "Full-time"
    t = str(title or "")
    if _INTERN_RX.search(t):
        return "Internship"
    if _PT_RX.search(t):
        return "Part-time"
    return "Full-time"


# ---- work mode -------------------------------------------------------------
def work_mode(remote=None, hybrid=None, text=""):
    if remote is True:
        return "Remote"
    if hybrid is True:
        return "Hybrid"
    t = (text or "").lower()
    if "remote" in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    return ""


def ymd(ts=None, iso=None):
    """YYYY-MM-DD from a unix ts (s or ms) or an ISO string.

    A trailing alphabetic timezone name is stripped before parsing.  This is
    not cosmetic: Python 3.9's datetime.fromisoformat is strict, and Recruitee
    publishes "2026-07-23 19:13:42 UTC", which it refuses outright.  Every one
    of the 64,906 rows in contract_norm/ats_all_recruitee.jsonl came back
    undated because of those four characters — and all_jobs_build keeps undated
    rows UNCONDITIONALLY past its posted cutoff, so they could never age out.
    The failure was invisible because ymd() returns "" on error, which is
    indistinguishable from a source that publishes no date at all.
    """
    try:
        if ts:
            ts = float(ts)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                "%Y-%m-%d")
        if iso:
            s = str(iso).strip().replace("Z", "+00:00")
            # " UTC" / " GMT" / " EST" ... but never a numeric offset such as
            # "+00:00", which fromisoformat handles itself.
            m = re.search(r"\s+([A-Za-z]{2,5})$", s)
            if m:
                s = s[:m.start()]
            return datetime.fromisoformat(s).strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return ""
    return ""


def row(**kw):
    r = {c: kw.get(c, "") for c in COLUMNS}
    r["remote"] = bool(kw.get("remote"))
    return r


# ---- http ------------------------------------------------------------------
def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    return s


def get_json(s, url, tries=3, timeout=25, **kw):
    last = None
    for i in range(tries):
        try:
            r = s.get(url, timeout=timeout, **kw)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 401, 403, 400):
                return {"__status__": r.status_code}
            last = r.status_code
        except (requests.RequestException, ValueError) as e:
            last = str(e)
        time.sleep(0.5 * (i + 1))
    return {"__error__": last}


def post_json(s, url, payload, tries=3, timeout=25):
    last = None
    for i in range(tries):
        try:
            r = s.post(url, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            last = r.status_code
        except (requests.RequestException, ValueError) as e:
            last = str(e)
        time.sleep(0.5 * (i + 1))
    return {"__error__": last}


class Writer:
    """Append-only JSONL writer with a periodic flush counter."""
    def __init__(self, path):
        self.f = open(path, "a", encoding="utf-8")
        self.n = 0

    def write(self, d):
        self.f.write(json.dumps(d) + "\n")
        self.n += 1
        if self.n % 500 == 0:
            self.f.flush()

    def close(self):
        self.f.flush()
        self.f.close()


def load_state(path):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}


def save_state(path, st):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.replace(tmp, path)
