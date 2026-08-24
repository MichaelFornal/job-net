#!/usr/bin/env python3
"""
rippling_build_sheet.py — rippling_all_jobs.jsonl -> rippling_jobs_master.csv
+ Rippling_All_Jobs_<date>.xlsx.

The Rippling board API returns only uuid / name / department / workLocation /
url (verified across 85 postings on 10 boards), so most columns here are
derived rather than read:

  employment_type  cc.title_type(title)   — no type field exists
  work_mode        regex over workLocation — no remote flag exists
  city/state       split of workLocation.label ("Conway, AR")
  posted           rippling_posted_ledger.json first-seen date, NOT blank:
                   all_jobs_build.py keeps undated rows forever, so a blank
                   here would park every Rippling job in the vet queue
                   permanently.

job_url is the API's own `url`, which is already
https://ats.rippling.com/{org}/jobs/{uuid} — exactly the shape
all_jobs_build.canon_key parses into ats|rippling|{org}|{uuid}.  That is what
merges these rows with the ~2,586 copies Getro already syndicates, so the
fallback below reconstructs the same shape rather than inventing one.
"""
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from openpyxl.utils.exceptions import IllegalCharacterError  # noqa: F401
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

IN_JSONL = os.path.join(BASE, PFX + "rippling_all_jobs.jsonl")
LEDGER_JSON = os.path.join(BASE, PFX + "rippling_posted_ledger.json")
BOARDS_CSV = os.path.join(BASE, PFX + "rippling_job_boards.csv")
OUT_CSV = os.path.join(BASE, PFX + "rippling_jobs_master.csv")
SNAPSHOT = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_XLSX = os.path.join(BASE, "%sRippling_All_Jobs_%s.xlsx" % (
    "Smoke_" if SMOKE else "", SNAPSHOT))
XLSX_ROW_CAP = 1_048_000

COLUMNS = ["title", "company", "location", "work_mode", "remote",
           "employment_type", "department", "city", "state", "country",
           "posted", "posted_precision", "apply_url", "job_url",
           "board", "board_url", "rippling_job_id"]
IDX = {c: i for i, c in enumerate(COLUMNS)}
N = len(COLUMNS)

REMOTE_RE = re.compile(r"remote|anywhere|work from home", re.I)
HYBRID_RE = re.compile(r"hybrid", re.I)
# "Conway, AR" / "Austin, TX, USA" -> trailing 2-letter US state code
STATE_RE = re.compile(r",\s*([A-Z]{2})(?:,|\s*$)")

_MEMO = {}


def I(s):
    return _MEMO.setdefault(s, s)


def safe(v):
    return ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v


def norm(rec, ledger):
    j = rec.get("job") or {}
    slug = rec.get("slug") or ""
    uuid = (j.get("uuid") or "").strip()
    title = (j.get("name") or "").strip()
    wl = j.get("workLocation") or {}
    loc = I(((wl.get("label") or wl.get("id") or "") or "").strip())
    dept = j.get("department") or {}
    dept = I(((dept.get("label") or dept.get("id") or "") or "").strip())

    if REMOTE_RE.search(loc):
        wm = "Remote"
    elif HYBRID_RE.search(loc):
        wm = "Hybrid"
    elif loc:
        wm = "On-site"
    else:
        wm = ""

    city = state = country = ""
    if loc and not REMOTE_RE.search(loc):
        m = STATE_RE.search(loc)
        if m:
            state = m.group(1)
            city = loc.split(",")[0].strip()
            if loc.rstrip().endswith(("USA", "United States", "US")):
                country = "US"
            elif state:
                country = "US"

    url = (j.get("url") or "").strip() or (
        "https://ats.rippling.com/%s/jobs/%s" % (slug, uuid) if slug and uuid
        else "")

    entry = ledger.get(uuid) or {}
    posted = entry.get("first_seen") or ""

    r = [""] * N
    r[IDX["title"]] = title
    r[IDX["company"]] = I(rec.get("company") or slug)
    r[IDX["location"]] = loc
    r[IDX["work_mode"]] = I(wm)
    r[IDX["remote"]] = "True" if wm == "Remote" else ""
    r[IDX["employment_type"]] = I(cc.title_type(title) or "")
    r[IDX["department"]] = dept
    r[IDX["city"]] = I(city)
    r[IDX["state"]] = I(state)
    r[IDX["country"]] = I(country)
    r[IDX["posted"]] = I(posted)
    r[IDX["posted_precision"]] = I("first_seen" if posted else "")
    r[IDX["apply_url"]] = url
    r[IDX["job_url"]] = url
    r[IDX["board"]] = I(slug)
    r[IDX["board_url"]] = I(rec.get("board_url") or
                            ("https://ats.rippling.com/%s" % slug))
    r[IDX["rippling_job_id"]] = uuid
    return r


def main():
    if not os.path.exists(IN_JSONL):
        raise SystemExit("missing %s — run rippling_fetch_jobs.py" % IN_JSONL)
    ledger = {}
    if os.path.exists(LEDGER_JSON):
        try:
            with open(LEDGER_JSON, encoding="utf-8") as f:
                ledger = json.load(f)
        except (OSError, json.JSONDecodeError):
            ledger = {}

    by_id, raw = {}, 0
    with open(IN_JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            raw += 1
            r = norm(rec, ledger)
            if not r[IDX["title"]]:
                continue
            key = r[IDX["rippling_job_id"]] or "%s|%s|%s" % (
                r[IDX["company"]], r[IDX["title"]], r[IDX["location"]])
            by_id.setdefault(key, r)
    n_ids = len(by_id)

    by_sig = {}
    for r in by_id.values():
        sig = (r[IDX["company"]].lower(), r[IDX["title"]].lower(),
               (r[IDX["apply_url"]] or r[IDX["location"]]).lower())
        by_sig.setdefault(sig, r)
    rows = list(by_sig.values())
    rows.sort(key=lambda r: (r[IDX["posted"]] or "", r[IDX["company"]]),
              reverse=True)
    print("raw rows %d -> unique ids %d -> deduped %d"
          % (raw, n_ids, len(rows)), flush=True)

    n_undated = sum(1 for r in rows if not r[IDX["posted"]])
    if n_undated:
        print("  WARNING: %d rows have no ledger date (%.1f%%) — they will "
              "survive the all_jobs_build posted cutoff indefinitely"
              % (n_undated, 100.0 * n_undated / max(1, len(rows))), flush=True)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for r in rows:
            w.writerow(r[:N])
    print("wrote %s" % OUT_CSV, flush=True)
    write_xlsx(rows)


def write_xlsx(rows):
    wb = Workbook(write_only=True)
    bold = Font(bold=True)

    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 100
    n_rem = sum(1 for r in rows if r[IDX["remote"]])
    for a, b in [
        ("Rippling — all jobs", "Snapshot %s" % SNAPSHOT),
        ("Rows", "%s deduplicated postings" % format(len(rows), ",")),
        ("Remote", "%s inferred remote from workLocation text — Rippling's "
                   "API has no remote flag" % format(n_rem, ",")),
        ("Source", "api.rippling.com/platform/api/ats/v1/board/{slug}/jobs — "
                   "unauthenticated, whole board in one call, no pagination."),
        ("Fields", "The API returns only uuid, name, department, "
                   "workLocation and url. There is no employment type, no "
                   "salary and no posting date, so those columns here are "
                   "derived from the title/location or left blank."),
        ("posted", "first-seen date from rippling_posted_ledger.json, frozen "
                   "the first time a uuid is observed. Not the true posting "
                   "date — Rippling publishes none — but stable across runs "
                   "and it ages out, which a blank never does."),
        ("Boards", "rippling_job_boards.csv lists live and currently-empty "
                   "boards; ~1/3 of real boards have no openings at any "
                   "moment and must not be treated as dead."),
        ("Dedupe", "job_url is ats.rippling.com/{org}/jobs/{uuid}, which "
                   "all_jobs_build.canon_key turns into ats|rippling|org|id "
                   "— the same key Getro's syndicated copies produce."),
    ]:
        ca = WriteOnlyCell(ws, value=a)
        ca.font = bold
        cb = WriteOnlyCell(ws, value=b)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        ws.append([ca, cb])

    ws = wb.create_sheet("All Jobs")
    ws.freeze_panes = "A2"
    widths = {"title": 52, "company": 26, "location": 30, "work_mode": 10,
              "remote": 8, "employment_type": 14, "department": 22,
              "city": 18, "state": 7, "country": 9, "posted": 11,
              "posted_precision": 15, "apply_url": 52, "job_url": 52,
              "board": 22, "board_url": 40, "rippling_job_id": 38}
    for i, c in enumerate(COLUMNS, 1):
        ws.column_dimensions[get_column_letter(i)].width = widths.get(c, 14)
    hdr = []
    for c in COLUMNS:
        cell = WriteOnlyCell(ws, value=c)
        cell.font = bold
        hdr.append(cell)
    ws.append(hdr)
    ws.auto_filter.ref = "A1:%s%d" % (get_column_letter(N),
                                      min(len(rows), XLSX_ROW_CAP) + 1)
    for r in rows[:XLSX_ROW_CAP]:
        ws.append([safe(v) for v in r[:N]])

    ws = wb.create_sheet("Boards")
    for col, wd in zip("ABCD", (30, 30, 10, 12)):
        ws.column_dimensions[col].width = wd
    ws.freeze_panes = "A2"
    hdr = []
    for c in ("board", "company", "jobs", "newest_post"):
        cell = WriteOnlyCell(ws, value=c)
        cell.font = bold
        hdr.append(cell)
    ws.append(hdr)
    stats = {}
    for r in rows:
        st = stats.setdefault(r[IDX["board"]], [r[IDX["company"]], 0, ""])
        st[1] += 1
        if (r[IDX["posted"]] or "") > st[2]:
            st[2] = r[IDX["posted"]] or ""
    for b, (comp, n, newest) in sorted(stats.items(), key=lambda kv: -kv[1][1]):
        ws.append([safe(b), safe(comp), n, newest])

    wb.save(OUT_XLSX)
    print("wrote %s" % OUT_XLSX, flush=True)


if __name__ == "__main__":
    sys.exit(main())
