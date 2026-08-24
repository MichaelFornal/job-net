#!/usr/bin/env python3
"""
ai_ma_remote.py — the AI-jobs cut that matters here: MA + remote, recent only.

Reads all_jobs_master.csv, keeps rows whose title/skills say the job is about
building with AI (same tiering as ai_jobs_filter.py), then slices geography to
US-remote or Massachusetts, and posted date to the last N days (default 7).

  python3 ai_ma_remote.py               last 7 days
  python3 ai_ma_remote.py --days 3      last 3 days
  python3 ai_ma_remote.py --since 2026-07-18

Output: AI_MA_Remote_<date>.xlsx
  Summary      counts + what each tab is
  Best Bets    the whole slice ranked (Claude/agentic/FDE first, MA before
               remote inside a band, newest first) — start here
  MA           Massachusetts on-site / hybrid / MA-tagged
  Remote       US-remote
  All          everything in the slice, flat
and AI_MA_Remote_<date>.csv with the same rows.
"""

import csv
import datetime as dt
import os
import re
import sys

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

import all_jobs_build as ab
from ai_jobs_filter import CORE, ADJ, SKILL, ENG_TITLE, combo_hit

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")
SNAP = dt.datetime.now().strftime("%Y-%m-%d")
OUT_XLSX = os.path.join(BASE, f"AI_MA_Remote_{SNAP}.xlsx")
OUT_CSV = os.path.join(BASE, f"AI_MA_Remote_{SNAP}.csv")

csv.field_size_limit(10**9)

US_OK = {"US", "USA", "UNITED STATES", ""}

# Rows with a blank country need a US hint in the location before a "remote"
# tag counts as US-remote — otherwise "Remote, Israel" lands in the slice.
US_HINT = re.compile(r"(?i)\bunited\s+states\b|\bu\.?s\.?a?\b|,\s*[A-Z]{2}\b|"
                     r"\b(?:anywhere|worldwide|global)\b")

# "Remote" with no geography at all: no country column, no place name. Could
# be US-eligible, so it gets its own bucket instead of being thrown away.
BARE_REMOTE = re.compile(r"(?i)^\s*(remote(\s*[/,-]\s*(online|remote|anywhere))?"
                         r"|work\s*from\s*home|wfh)\s*$")


def _argval(flag, default):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


DAYS = int(_argval("--days", "7"))
SINCE = _argval(
    "--since",
    (dt.date.today() - dt.timedelta(days=DAYS - 1)).isoformat(),
)

# ---- ranking bands (strongest signal first) --------------------------------

# Kept in step with ai_jobs_filter.CORE's forward-deployed alternative: the
# spelling variants and the FDE acronym are band-1 roles, and banding them 2
# (which the AI-modifier recovery would do for "Forward Deployment Engineer,
# AI") would rank the single most on-target title below a generic AI engineer.
BAND_1 = re.compile(r"(?i)\bclaude\b|\banthropic\b|\bagentic\b|\bai\s+agents?\b"
                    r"|\bforward[\s-]+deploy(?:ed|ment)?\b|\bfde\b"
                    r"|\bcoding\s+agents?\b")
BAND_2 = re.compile(r"(?i)\bai[\s/-]+(?:engineer|developer|dev\b|builder|"
                    r"software|platform|application|apps?\b|automation|"
                    r"solutions?\s+engineer|integration|tooling)"
                    r"|\bllms?\b|\bgen\s?ai\b|\bgenerative\s+ai\b"
                    r"|\bapplied\s+ai\b|\bprompt\s+engineer|\bfounding\s+ai\b")

BANDS = [
    (1, "1 Claude / agentic / FDE", BAND_1),
    (2, "2 AI engineer / LLM / GenAI", BAND_2),
]

COLS = [
    "company", "title", "seniority", "employment_type", "work_mode", "remote",
    "location", "state", "country", "posted", "salary_min", "salary_max",
    "salary_currency", "salary_period", "skills", "company_stage",
    "company_size", "company_industry", "apply_url", "job_url", "source",
]
CTX = ["rank", "band", "bucket", "tier", "matched"]
OUT_COLS = CTX + COLS

WIDTHS = {
    "rank": 6, "band": 26, "bucket": 9, "tier": 16, "matched": 20,
    "company": 26, "title": 48, "seniority": 10, "employment_type": 13,
    "work_mode": 10, "remote": 8, "location": 30, "state": 8, "country": 8,
    "posted": 11, "salary_min": 10, "salary_max": 10, "salary_currency": 8,
    "salary_period": 8, "skills": 50, "company_stage": 12, "company_size": 12,
    "company_industry": 20, "apply_url": 45, "job_url": 45, "source": 14,
}


def clean(v):
    return "" if v is None else ILLEGAL_CHARACTERS_RE.sub("", str(v))


def tier_of(row):
    """(tier, matched) or None — same rules as ai_jobs_filter.py."""
    title = row.get("title", "")
    m = CORE.search(title)
    if m:
        return "Core AI Building", m
    m = ADJ.search(title)
    if m:
        return "AI-ML Adjacent", m
    # Titles where the AI token and the role noun are not adjacent, which CORE
    # cannot see at all.  Tried last so it only ever ADDS rows: anything CORE
    # or ADJ already claims keeps the tier it always had.
    m = combo_hit(title)
    if m:
        return "Core AI Building", m
    sk = row.get("skills", "")
    if sk and ENG_TITLE.search(title):
        m = SKILL.search(sk)
        if m:
            return "Skills Match", m
    return None


def band_of(title, tier):
    for n, label, rx in BANDS:
        if rx.search(title):
            return n, label
    # A non-adjacent AI engineering title is a band-2 role ("AI engineer / LLM /
    # GenAI"), not band 3.  Without this the rows combo_hit newly admits land in
    # "3 Other core AI" — rank_tier 3 — and the AI 1-2 lane, which selects
    # rank_tier <= 2, never sees a single one of them.
    #
    # Gated on CORE and ADJ BOTH missing so the promotion is strictly additive.
    # Ungated it also lifted 602 rows that already had a band — "AI Scientist",
    # "Senior Machine Learning Engineer – Physical AI" — out of band 4 and into
    # the top lane, which is the entire thing the AI-ML-adjacent band exists to
    # prevent.  Rows CORE or ADJ already claim keep the band they always had.
    if not CORE.search(title) and not ADJ.search(title) and combo_hit(title):
        return 2, "2 AI engineer / LLM / GenAI"
    if tier == "Core AI Building":
        return 3, "3 Other core AI"
    if tier == "AI-ML Adjacent":
        return 4, "4 AI-ML adjacent"
    return 5, "5 AI tooling in skills"


def main():
    rows = []
    scanned = ai_hits = 0
    with open(SRC, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            scanned += 1
            posted = (r.get("posted") or "")[:10]
            if not posted or posted < SINCE:
                continue
            hit = tier_of(r)
            if not hit:
                continue
            ai_hits += 1
            tier, m = hit

            country = (r.get("country") or "").strip().upper()
            loc = r.get("location") or ""
            state = r.get("state") or ""
            regs = ab.regions_of(loc, state, r.get("country") or "")
            is_ma = "MA" in regs
            remote = (r.get("remote") == "True"
                      or r.get("work_mode") == "Remote"
                      or "remote" in loc.lower())
            us = country in ("US", "USA", "UNITED STATES") or (
                not country and bool(US_HINT.search(loc + " " + state)))

            if is_ma:
                bucket = "MA remote" if remote else "MA"
            elif remote and us:
                bucket = "Remote"
            elif remote and not country and BARE_REMOTE.match(loc):
                bucket = "Remote (no country)"
            else:
                continue

            n, label = band_of(r.get("title", ""), tier)
            rec = {c: r.get(c, "") for c in COLS}
            rec.update(band=label, bucket=bucket, tier=tier,
                       matched=re.sub(r"\s+", " ", m.group(0)).strip())
            rec["_n"] = n
            rec["_ma"] = 0 if is_ma else 1
            rec["_posted"] = posted
            rows.append(rec)

    rows.sort(key=lambda r: (r["_n"], r["_ma"], _negdate(r["_posted"]),
                             r["company"]))
    for i, r in enumerate(rows, 1):
        r["rank"] = i

    ma = [r for r in rows if r["bucket"].startswith("MA")]
    rem = [r for r in rows if r["bucket"] in ("Remote", "MA remote")]
    anyrem = [r for r in rows if r["bucket"] == "Remote (no country)"]

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    write_xlsx(rows, ma, rem, anyrem, scanned, ai_hits)

    print(f"scanned {scanned:,} jobs, {ai_hits:,} AI roles posted >= {SINCE}")
    print(f"  MA (incl. MA-remote): {len(ma):,}")
    print(f"  Remote (US, incl. MA-remote): {len(rem):,}")
    print(f"  Remote (no country given): {len(anyrem):,}")
    print(f"  slice total: {len(rows):,}")
    for n, label, _ in BANDS + [(3, "3 Other core AI", None),
                                (4, "4 AI-ML adjacent", None),
                                (5, "5 AI tooling in skills", None)]:
        c = sum(1 for r in rows if r["_n"] == n)
        print(f"    {label}: {c:,}")
    print(f"wrote {OUT_XLSX}")
    print(f"wrote {OUT_CSV}")


def _negdate(d):
    """Sort newest-first inside a band."""
    return tuple(-int(x) for x in d.split("-")) if d.count("-") == 2 else (0,)


def sheet(wb, name, rows, bold):
    ws = wb.create_sheet(name)
    for i, c in enumerate(OUT_COLS, 1):
        ws.column_dimensions[get_column_letter(i)].width = WIDTHS.get(c, 14)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = \
        f"A1:{get_column_letter(len(OUT_COLS))}{max(len(rows) + 1, 2)}"
    ws.append(OUT_COLS)
    for r in rows:
        ws.append([clean(r.get(c, "")) for c in OUT_COLS])


def write_xlsx(rows, ma, rem, anyrem, scanned, ai_hits):
    wb = Workbook()
    wb.remove(wb.active)
    bold = Font(bold=True)

    ws = wb.create_sheet("Summary")
    for col, w in (("A", 30), ("B", 12), ("C", 88)):
        ws.column_dimensions[col].width = w
    ws.append([f"AI jobs — Massachusetts + remote — {SNAP}"])
    ws["A1"].font = bold
    ws.append([f"Posted on or after {SINCE} (last {DAYS} days)."])
    ws.append([])
    ws.append(["Tab", "Jobs", "What it is"])
    for c in ws[ws.max_row]:
        c.font = bold
    ws.append(["Best Bets", len(rows),
               "The whole slice ranked: Claude/agentic/forward-deployed first, "
               "then AI engineer/LLM/GenAI, then other core AI, AI-ML adjacent, "
               "and finally engineering roles whose skills list AI tooling. "
               "MA before remote inside a band, newest first."])
    ws.append(["MA", len(ma),
               "Massachusetts: on-site, hybrid, or MA-tagged (includes MA "
               "roles that are also remote)."])
    ws.append(["Remote", len(rem),
               "US-remote: country is US, or the location names a US state / "
               "'United States'. MA-remote roles appear here and on the MA tab."])
    ws.append(["Remote (no country)", len(anyrem),
               "Listed only as 'Remote' with no country or place at all — may "
               "or may not be US-eligible, kept separate so it doesn't dilute "
               "the Remote tab."])
    ws.append(["All", len(rows), "Every row in the slice, flat."])
    ws.append([])
    ws.append(["Band", "Jobs", ""])
    for c in ws[ws.max_row]:
        c.font = bold
    for label in ["1 Claude / agentic / FDE", "2 AI engineer / LLM / GenAI",
                  "3 Other core AI", "4 AI-ML adjacent",
                  "5 AI tooling in skills"]:
        ws.append([label, sum(1 for r in rows if r["band"] == label), ""])
    ws.append([])
    ws.append(["Source", "",
               f"Filtered from {scanned:,} jobs in all_jobs_master.csv; "
               f"{ai_hits:,} AI roles were posted in the window before the "
               "MA/remote geography cut."])
    ws.append(["matched column", "",
               "The keyword that pulled the job in (title, or a skills entry "
               "for tier 'Skills Match')."])

    sheet(wb, "Best Bets", rows, bold)
    sheet(wb, "MA", ma, bold)
    sheet(wb, "Remote", rem, bold)
    sheet(wb, "Remote (no country)", anyrem, bold)
    sheet(wb, "All", rows, bold)
    wb.save(OUT_XLSX)


if __name__ == "__main__":
    main()
