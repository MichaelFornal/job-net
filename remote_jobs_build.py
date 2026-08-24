#!/usr/bin/env python3
"""Remote-only cut of all_jobs_master.csv.

Streams the master CSV, keeps every row that satisfies the same is_remote()
rule the All_Jobs workbook uses for its "Remote" stat, and writes
Remote_Jobs_<date>.xlsx + Remote_Jobs_<date>.csv.

An extra `remote_signal` column records which of the three criteria fired,
so the fuzzy location-substring matches can be filtered out if wanted.
"""
import csv
import os
import sys
from collections import Counter

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "all_jobs_master.csv")
DATE = sys.argv[1] if len(sys.argv) > 1 else "2026-07-29"
OUT_XLSX = os.path.join(HERE, f"Remote_Jobs_{DATE}.xlsx")
OUT_CSV = os.path.join(HERE, f"Remote_Jobs_{DATE}.csv")

XLSX_CAP = 1_048_000
COLUMNS = [
    "company", "title", "employment_type", "seniority", "department",
    "work_mode", "remote", "remote_signal", "location", "city", "state",
    "country", "posted", "salary_min", "salary_max", "salary_currency",
    "salary_period", "equity", "skills", "company_stage", "company_size",
    "company_industry", "apply_url", "job_url", "boards", "board_count",
    "source", "source_type", "sources", "n_sources", "job_key",
]
WIDTHS = [26, 44, 14, 12, 18, 9, 7, 13, 30, 14, 10, 12, 11, 10, 10, 8, 9, 7,
          40, 13, 11, 30, 46, 46, 36, 8, 13, 12, 22, 9, 42]

_MEMO = {}


def I(s):
    return _MEMO.setdefault(s, s)


def safe(v):
    return ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v


def main():
    csv.field_size_limit(10 ** 9)
    rows = []
    total = 0
    sig_counts = Counter()
    wm_counts = Counter()
    src_counts = Counter()
    ctry_counts = Counter()

    with open(SRC, newline="", encoding="utf-8") as f:
        rd = csv.reader(f)
        hdr = next(rd)
        i = {n: k for k, n in enumerate(hdr)}
        need = [c for c in COLUMNS if c != "remote_signal"]
        missing = [c for c in need if c not in i]
        if missing:
            sys.exit(f"master csv missing columns: {missing}")

        for row in rd:
            total += 1
            flag = row[i["remote"]] == "True"
            wm = row[i["work_mode"]] == "Remote"
            loc = "remote" in row[i["location"]].lower()
            if not (flag or wm or loc):
                continue
            sig = I("+".join(
                s for s, on in (("flag", flag), ("work_mode", wm),
                                ("location", loc)) if on))
            sig_counts[sig] += 1
            wm_counts[row[i["work_mode"]] or "(not stated)"] += 1
            src_counts[row[i["source"]] or "(none)"] += 1
            ctry_counts[row[i["country"]] or "(not stated)"] += 1
            out = []
            for c in COLUMNS:
                out.append(sig if c == "remote_signal" else I(row[i[c]]))
            rows.append(out)
            if total % 250_000 == 0:
                print(f"  scanned {total:,} -> kept {len(rows):,}", flush=True)

    print(f"scanned {total:,} rows, kept {len(rows):,} remote", flush=True)

    # newest first, then company, then title
    rows.sort(key=lambda r: (r[12] or "", r[0].lower(), r[1].lower()),
              reverse=False)
    rows.reverse()

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        w.writerows(rows)
    print(f"wrote {OUT_CSV}", flush=True)

    wb = Workbook(write_only=True)
    bold = Font(bold=True)

    def hdr_cells(ws, names):
        from openpyxl.cell import WriteOnlyCell
        out = []
        for nm in names:
            c = WriteOnlyCell(ws, value=nm)
            c.font = bold
            out.append(c)
        return out

    # ---- summary tab
    ws = wb.create_sheet("Summary")
    ws.column_dimensions["A"].width = 34
    ws.column_dimensions["B"].width = 14
    ws.append(hdr_cells(ws, ["Remote Jobs", DATE]))
    ws.append(["source workbook", "All_Jobs_" + DATE])
    ws.append(["master rows scanned", total])
    ws.append(["remote rows kept", len(rows)])
    ws.append([])
    ws.append(hdr_cells(ws, ["remote_signal", "jobs"]))
    for k, v in sig_counts.most_common():
        ws.append([k, v])
    ws.append([])
    ws.append(hdr_cells(ws, ["work_mode", "jobs"]))
    for k, v in wm_counts.most_common():
        ws.append([k, v])
    ws.append([])
    ws.append(hdr_cells(ws, ["country", "jobs"]))
    for k, v in ctry_counts.most_common(30):
        ws.append([k, v])
    ws.append([])
    ws.append(hdr_cells(ws, ["source", "jobs"]))
    for k, v in src_counts.most_common(40):
        ws.append([k, v])
    ws.append([])
    ws.append(["rule", "a job is kept if ANY of the three fire:"])
    ws.append(["flag", "remote column == True"])
    ws.append(["work_mode", "work_mode == Remote"])
    ws.append(["location", "'remote' appears in the location string"])
    ws.append(["", "(same is_remote() rule all_jobs_build.py uses)"])

    # ---- data tabs
    ncols = len(COLUMNS)
    chunks = max(1, -(-len(rows) // XLSX_CAP))
    for ci in range(chunks):
        name = "Remote Jobs" if ci == 0 else f"Remote Jobs {ci + 1}"
        ws = wb.create_sheet(name)
        for j, wd in enumerate(WIDTHS, 1):
            ws.column_dimensions[get_column_letter(j)].width = wd
        ws.freeze_panes = "A2"
        chunk = rows[ci * XLSX_CAP:(ci + 1) * XLSX_CAP]
        ws.auto_filter.ref = (f"A1:{get_column_letter(ncols)}"
                              f"{len(chunk) + 1}")
        ws.append(hdr_cells(ws, COLUMNS))
        for r in chunk:
            ws.append([safe(v) for v in r])
        print(f"  tab '{name}': {len(chunk):,} rows", flush=True)

    wb.save(OUT_XLSX)
    print(f"wrote {OUT_XLSX}", flush=True)


if __name__ == "__main__":
    main()
