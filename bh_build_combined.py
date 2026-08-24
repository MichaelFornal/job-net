#!/usr/bin/env python3
"""
bh_build_combined.py — merge the FT+contract portal crawls into one deduplicated
master + workbook.

Explicit source list (NOT a glob) so the FT-inclusive *_all files are used and
the older contract-only duplicates (ats_*, phenom.jsonl, bullhorn.jsonl) are
skipped:

  bullhorn_all.jsonl        Bullhorn OSCP staffing agencies   (FT + contract)
  ats_all_*.jsonl (x5)      Ashby/SmartRecruiters/Recruitee/Breezy/BambooHR
                            first-party ATS boards            (FT + contract)
  phenom_all.jsonl          Allegis staffing brands (Phenom)  (FT + contract)
  boards_all.jsonl          freelance marketplaces + remote boards (FT +
                            contract; boards_fetch.py under BOARDS_KEEP_ALL=1,
                            which run_all.py always passes).  The older
                            contract-only boards.jsonl is NOT included:
                            keep-all keeps a strict superset of the same rows,
                            so listing both would add nothing but a file that
                            no longer gets refreshed and therefore only ages.
  getro.jsonl / lever.jsonl contract slice of the VC/Lever board crawls

Dedupe is the same two-pass scheme as contract_build.py.

Outputs:
  portals_combined_master.csv
  FT_Contract_Portals_<date>.xlsx   README / All Jobs / Summary / Boards
"""
import csv
import glob
import json
import os
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.utils import get_column_letter

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_CSV = os.path.join(BASE, "portals_combined_master.csv")
OUT_XLSX = os.path.join(BASE, f"FT_Contract_Portals_{SNAP}.xlsx")
XLSX_CAP = 1_048_000
COLS = cc.COLUMNS + ["sources", "n_sources"]

INCLUDE = [
    "bullhorn_all.jsonl",
    "ats_all_ashby.jsonl", "ats_all_smartrecruiters.jsonl",
    "ats_all_recruitee.jsonl", "ats_all_breezy.jsonl", "ats_all_bamboohr.jsonl",
    "ats_all_workable.jsonl", "ats_all_jazzhr.jsonl",
    "ats_all_teamtailor.jsonl",
    "phenom_all.jsonl", "boards_all.jsonl", "getro.jsonl", "lever.jsonl",

    # --- added 2026-07-29: the remote-source and un-piped-ATS expansion -------
    # A missing file here is NOT an error — main() prints "(missing X)" and
    # continues — so a source whose fetcher has not been run yet, or which was
    # verified as genuinely closed, simply contributes nothing.  That is what
    # makes it safe to list the whole intended set rather than editing this
    # list every time one more pipe lands.
    #
    # ATS families mined out of the corpus (see ats_mine_tenants.py):
    "workday.jsonl", "adp.jsonl", "paycom.jsonl", "icims.jsonl",
    "oracle.jsonl", "successfactors.jsonl", "eightfold.jsonl",
    "comeet.jsonl", "jobvite.jsonl", "gem.jsonl", "atstail.jsonl",
    # Remote boards / aggregators:
    "themuse.jsonl", "jobgether.jsonl", "remoteleaf.jsonl", "web3career.jsonl",
    "smallremote.jsonl", "nicheboards.jsonl", "careerjet.jsonl",
    # International:
    "intlboards.jsonl", "intlboards_torre.jsonl", "intlmisc.jsonl",
    "arbeitsagentur.jsonl",
    # Key-gated (produce rows only once the free API keys are in the env):
    "usajobs.jsonl", "adzuna.jsonl",

    # --- added 2026-08-20: families host_census.py surfaced ------------------
    # None of these had ever appeared in a census, because every census in this
    # repo ran off a closed regex list and could only report on families
    # somebody had already written a rule for.  Ranked by distinct EMPLOYERS in
    # the master at the time they were built:
    #   manatal 606 (careers-page.com) · ukg 327 · neogov 159 (governmentjobs,
    #   US state/municipal, zero overlap with the federal usajobs pipe) ·
    #   dayforce 129 · cornerstone 75 · radancy 75 hosts / 62k rows.
    "ukg.jsonl", "manatal.jsonl", "dayforce.jsonl", "neogov.jsonl",
    "radancy.jsonl", "cornerstone.jsonl",
]
# which sources carry full-time as well as contingent roles
FT_SOURCES = {"bullhorn", "ashby", "smartrecruiters", "recruitee", "breezy",
              "bamboohr", "phenom", "workable", "jazzhr", "teamtailor",
              # 2026-08-20 additions: all six run KEEP_ALL, so they carry
              # full-time as well as contingent roles.
              "ukg", "manatal", "dayforce", "neogov", "radancy",
              "cornerstone",
              # the remote boards + marketplaces, once boards_fetch.py is run
              # with BOARDS_KEEP_ALL=1 (run_all.py passes it)
              "remotive", "remoteok", "himalayas", "jobicy", "workingnomads",
              "arbeitnow", "weworkremotely", "problogger"}


def safe(v):
    return ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v


def merge(a, b):
    a["_sources"] |= b["_sources"]
    a["_boards"] |= b["_boards"]
    if (b.get("posted") or "") > (a.get("posted") or ""):
        a["posted"] = b["posted"]
    for k in ("apply_url", "location", "city", "state", "country",
              "salary_min", "salary_max", "salary_currency", "work_mode"):
        if not a.get(k) and b.get(k):
            a[k] = b[k]


def warn_unlisted():
    """Name every contract_norm/*.jsonl that INCLUDE does not read.

    INCLUDE is deliberately an explicit list, not a glob, so that superseded
    contract-only files stay out.  The cost of that choice is that a NEW pipe
    whose output filename nobody added here contributes silently nothing —
    and the build still reports a healthy-looking total, so the omission is
    invisible.  This turns that failure mode into a printed line.
    """
    listed = set(INCLUDE)
    # Deliberately superseded by their *_all.jsonl counterparts (see the module
    # docstring) — skipped on purpose, so they must not be reported as strays.
    known_skip = {"boards.jsonl", "phenom.jsonl", "bullhorn.jsonl"}
    stray = []
    for path in sorted(glob.glob(os.path.join(cc.NORM_DIR, "*.jsonl"))):
        name = os.path.basename(path)
        if (name in listed or name in known_skip or ".prev." in name
                or name.startswith("ats_")):
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                n = sum(1 for _ in f)
        except OSError:
            n = -1
        stray.append((name, n))
    for name, n in stray:
        print(f"  !! NOT IN INCLUDE: {name} ({n} rows) — add it or it is "
              f"silently dropped", flush=True)
    return stray


def main():
    by_id = {}
    raw = dup_id = 0
    per_file = {}
    warn_unlisted()
    for name in INCLUDE:
        path = os.path.join(cc.NORM_DIR, name)
        if not os.path.exists(path):
            print(f"  (missing {name})", flush=True)
            continue
        n = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                raw += 1
                n += 1
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not (r.get("title") and str(r.get("title")).strip()):
                    continue
                r["_sources"] = {r.get("source")}
                r["_boards"] = {f"{r.get('source')}:{r.get('board')}"}
                jid = r.get("job_id")
                # board-scoped: bamboohr/bullhorn reuse per-board integer ids,
                # so (source, job_id) alone over-merges across agencies.
                k = (r.get("source"), r.get("board"), str(jid)) if jid else None
                if k and k in by_id:
                    merge(by_id[k], r)
                    dup_id += 1
                elif k:
                    by_id[k] = r
                else:
                    by_id[id(r)] = r
        per_file[name] = n
    after_id = len(by_id)

    by_key = {}
    for r in by_id.values():
        comp = (r.get("company") or "").strip().lower()
        title = (r.get("title") or "").strip().lower()
        au = (r.get("apply_url") or "").strip().lower()
        k = (comp, title, au or "loc:" + (r.get("location") or "").lower())
        if k in by_key:
            merge(by_key[k], r)
        else:
            by_key[k] = r
    rows = list(by_key.values())
    for r in rows:
        srcs = sorted(x for x in r["_sources"] if x)
        r["sources"] = ", ".join(srcs)
        r["n_sources"] = len(srcs)
    rows.sort(key=lambda r: (r.get("posted") or "", r.get("n_sources", 0)),
              reverse=True)
    print(f"raw {raw} -> id-dedupe {after_id} ({dup_id} dups) -> "
          f"deduped {len(rows)}", flush=True)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLS)
        for r in rows:
            w.writerow([r.get(c, "") for c in COLS])
    print(f"wrote {OUT_CSV}", flush=True)

    by_source, by_type, by_board = {}, {}, {}
    ft = contingent = 0
    for r in rows:
        s = r.get("source") or ""
        by_source[s] = by_source.get(s, 0) + 1
        et = r.get("employment_type") or "(none)"
        by_type[et] = by_type.get(et, 0) + 1
        if et in ("Full-time", "Part-time", "Internship", "Volunteer"):
            ft += 1
        else:
            contingent += 1
        bk = (s, r.get("board") or "")
        st = by_board.setdefault(bk, [0, r.get("board_url") or "", ""])
        st[0] += 1
        if (r.get("posted") or "") > st[2]:
            st[2] = r.get("posted") or ""

    wb = Workbook(write_only=True)
    bold = Font(bold=True)

    def hdr(ws, names):
        out = []
        for nm in names:
            c = WriteOnlyCell(ws, value=nm)
            c.font = bold
            out.append(c)
        return out

    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 118
    n_remote = sum(1 for r in rows if r.get("remote"))
    lines = [
        ("Full-time & contract jobs — portal crawl master", ""),
        ("Snapshot (UTC)", SNAP),
        ("", ""),
        ("Unique jobs (deduped)", len(rows)),
        ("Raw rows harvested", raw),
        ("Full-time / part-time / intern", ft),
        ("Contract / temp / freelance / etc.", contingent),
        ("Remote", n_remote),
        ("Distinct sources", len(by_source)),
        ("", ""),
        ("What this is", "Every full-time AND contingent role reachable through "
         "open, unauthenticated feeds across staffing ATSs (Bullhorn OSCP), "
         "first-party company ATS boards (Ashby/SmartRecruiters/Recruitee/"
         "Breezy/BambooHR), Allegis staffing brands (Phenom), freelance "
         "marketplaces + remote boards, and the contract slice of the Getro "
         "(VC portfolio) and Lever board crawls."),
        ("Full-time coverage", "Bullhorn, the five ATS platforms, and Phenom "
         "were crawled keeping ALL employment types. Marketplaces (Freelancer, "
         "Braintrust, Himalayas...) are contract/freelance by nature. The "
         "Getro and Lever rows here are the contract slice only; their full "
         "full-time universes are the separate multi-million-row Getro and "
         "Lever masters."),
        ("employment_type", "Full-time / Part-time / Internship, or the "
         "contingent label: Contract, Temp, Freelance, Contract-to-Hire, "
         "Interim, Seasonal, Per Diem, Fractional."),
        ("Dedupe", "Same posting collapses across sources on (company, title, "
         "apply URL) or (company, title, location); 'sources'/'n_sources' show "
         "where it was seen."),
        ("Bullhorn note", "Bullhorn agencies expose jobs only through their "
         "agency portal, so those rows have no public apply URL; the company "
         "column is the portal domain."),
        ("Master CSV", "portals_combined_master.csv holds the complete table."),
    ]
    for a, b in lines:
        ca = WriteOnlyCell(ws, value=a)
        ca.font = bold
        cb = WriteOnlyCell(ws, value=b)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        ws.append([ca, cb])

    ws = wb.create_sheet("All Jobs")
    widths = [12, 14, 28, 44, 16, 9, 30, 16, 12, 9, 7, 10, 10, 8, 8, 11, 46,
              20, 18, 34, 22, 9]
    for i, wd in enumerate(widths[:len(COLS)], 1):
        ws.column_dimensions[get_column_letter(i)].width = wd
    ws.freeze_panes = "A2"
    n_x = min(len(rows), XLSX_CAP)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(COLS))}{n_x + 1}"
    ws.append(hdr(ws, COLS))
    for r in rows[:XLSX_CAP]:
        ws.append([safe(r.get(c, "")) for c in COLS])
    if len(rows) > XLSX_CAP:
        print(f"NOTE: {len(rows) - XLSX_CAP} rows beyond Excel cap (CSV only)",
              flush=True)

    ws = wb.create_sheet("Summary")
    for col, wd in zip("ABCD", (20, 16, 3, 20)):
        ws.column_dimensions[col].width = wd
    ws.append(hdr(ws, ["by source", "jobs", "", ""]))
    for s, n in sorted(by_source.items(), key=lambda kv: -kv[1]):
        ws.append([s, n])
    ws.append(["", "", "", ""])
    ws.append(hdr(ws, ["by employment_type", "jobs", "", ""]))
    for t, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
        ws.append([t, n])
    ws.append(["", "", "", ""])
    ws.append(hdr(ws, ["rows per source file", "raw", "", ""]))
    for nm, n in per_file.items():
        ws.append([nm, n])
    ws.append(["", "", "", ""])
    ws.append([WriteOnlyCell(ws, value="TOTAL unique"), len(rows)])

    ws = wb.create_sheet("Boards")
    for col, wd in zip("ABCD", (16, 40, 46, 9)):
        ws.column_dimensions[col].width = wd
    ws.freeze_panes = "A2"
    top = sorted(by_board.items(), key=lambda kv: -kv[1][0])[:20000]
    ws.auto_filter.ref = f"A1:D{len(top) + 1}"
    ws.append(hdr(ws, ["source", "board", "board_url", "jobs"]))
    for (s, b), st in top:
        ws.append([s, b, st[1], st[0]])

    wb.save(OUT_XLSX)
    print(f"wrote {OUT_XLSX}", flush=True)


if __name__ == "__main__":
    main()
