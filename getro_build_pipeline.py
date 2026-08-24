#!/usr/bin/env python3
"""
getro_build_pipeline.py
Build the personal pipeline workbook from getro_jobs_master.csv:

  - every Remote role (ecosystem boards + People Data Labs)
  - every On-site (or work-mode-untagged) role located in Massachusetts,
    Rhode Island, or the South Florida metros (Miami-Dade / Broward /
    Palm Beach counties), plus rows tagged only "Florida, USA" statewide,
    labeled "FL (statewide tag)" so they filter in/out in one click.

Adds `flag` + `notes` columns up front for triage. openpyxl's write-only
mode can't attach data validation, so after saving we splice a
<dataValidations> dropdown (Interested / Maybe / No) into the Pipeline
sheet's XML — inserted right after </conditionalFormatting>, which is the
schema-correct slot. Flagging "Interested" turns the row green via a
formula conditional-format rule.

Output: Getro_Pipeline_<date>.xlsx  (Pipeline / Summary / README)
"""

import csv
import os
import re
import zipfile
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.cell import WriteOnlyCell
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter

BASE = os.path.dirname(os.path.abspath(__file__))
IN_CSV = os.path.join(BASE, "getro_jobs_master.csv")
SNAPSHOT = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_XLSX = os.path.join(BASE, f"Getro_Pipeline_{SNAPSHOT}.xlsx")

FLAG_CHOICES = "Interested,Maybe,No"

SFL_CITIES = [
    "miami", "miami beach", "north miami", "coral gables", "doral", "hialeah",
    "kendall", "homestead", "aventura", "miami gardens", "miami lakes",
    "sunny isles", "key biscayne", "cutler bay", "palmetto bay", "opa-locka",
    "fort lauderdale", "hollywood", "pembroke pines", "miramar", "davie",
    "plantation", "sunrise", "coral springs", "pompano beach",
    "deerfield beach", "weston", "tamarac", "lauderhill", "coconut creek",
    "margate", "oakland park", "wilton manors", "dania beach",
    "hallandale beach", "parkland", "cooper city", "west palm beach",
    "palm beach", "palm beach gardens", "boca raton", "delray beach",
    "boynton beach", "jupiter", "wellington", "royal palm beach",
    "lake worth", "greenacres", "riviera beach", "north palm beach",
    "juno beach", "tequesta", "lantana",
]
SFL_PAT = [c + ", fl" for c in SFL_CITIES] + [
    "miami-dade", "broward", "palm beach county", "south florida"]

REGION_RANK = {"MA": 1, "RI": 2, "South FL": 3, "FL (statewide tag)": 4}


def regions_of(loc):
    l = loc.lower()
    out = []
    if ", ma, usa" in l or "massachusetts" in l:
        out.append("MA")
    if ", ri, usa" in l or "rhode island" in l:
        out.append("RI")
    if any(p in l for p in SFL_PAT):
        out.append("South FL")
    elif "florida, usa" in l:
        out.append("FL (statewide tag)")
    return out


def main():
    csv.field_size_limit(10**7)
    with open(IN_CSV, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        master_cols = next(r)
        i_loc = master_cols.index("location")
        i_wm = master_cols.index("work_mode")
        i_posted = master_cols.index("posted")

        rows = []  # (bucket_rank, region_rank, posted, bucket, region, row)
        n_remote = 0
        n_local = 0
        for row in r:
            wm = row[i_wm]
            regs = regions_of(row[i_loc])
            if wm == "Remote":
                rows.append((0, REGION_RANK.get(regs[0], 9) if regs else 9,
                             row[i_posted], "Remote", "; ".join(regs), row))
                n_remote += 1
            elif wm in ("On-site", "", "Hybrid") and regs:
                rows.append((1, REGION_RANK.get(regs[0], 9),
                             row[i_posted], "Local", "; ".join(regs), row))
                n_local += 1

    # newest first, then stable-group by (bucket, primary region)
    rows.sort(key=lambda t: t[2], reverse=True)
    rows.sort(key=lambda t: (t[0], t[1]))
    print(f"pipeline rows: {len(rows)} ({n_remote} remote, {n_local} local)",
          flush=True)

    cols = ["flag", "notes", "bucket", "region"] + master_cols
    ncols = len(cols)
    nrows = len(rows)

    wb = Workbook(write_only=True)
    bold = Font(bold=True)

    # ---- Pipeline
    ws = wb.create_sheet("Pipeline")
    widths = [11, 24, 9, 18, 46, 26, 30, 10, 12, 11, 11, 11, 9, 12, 7, 44,
              13, 12, 34, 46, 46, 40, 12, 12, 13]
    for i, wdt in enumerate(widths[:ncols], 1):
        ws.column_dimensions[get_column_letter(i)].width = wdt
    ws.freeze_panes = "E2"
    ws.auto_filter.ref = f"A1:{get_column_letter(ncols)}{nrows + 1}"
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE",
                        fill_type="solid")
    ws.conditional_formatting.add(
        f"A2:{get_column_letter(ncols)}{nrows + 1}",
        FormulaRule(formula=['$A2="Interested"'], fill=green,
                    stopIfTrue=False))
    hdr = []
    for c in cols:
        cell = WriteOnlyCell(ws, value=c)
        cell.font = bold
        hdr.append(cell)
    ws.append(hdr)
    for _, _, _, bucket, region, row in rows:
        ws.append(["", "", bucket, region] + row)

    # ---- Summary
    counts = {}
    for _, _, _, bucket, region, row in rows:
        key = (bucket, region or "(anywhere)")
        counts[key] = counts.get(key, 0) + 1
    ws = wb.create_sheet("Summary")
    for col, wdt in zip("ABC", (12, 26, 12)):
        ws.column_dimensions[col].width = wdt
    ws.append([WriteOnlyCell(ws, value=v) for v in ("bucket", "region",
                                                    "jobs")])
    for (bucket, region), n in sorted(counts.items(),
                                      key=lambda kv: -kv[1]):
        ws.append([bucket, region, n])
    ws.append(["TOTAL", "", nrows])

    # ---- README
    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 110
    lines = [
        ("Personal pipeline", f"Snapshot {SNAPSHOT} (UTC). {nrows} roles: "
         "every Remote job across all 768 Getro boards, plus every On-site "
         "job in MA, RI, and the Miami / Fort Lauderdale / West Palm Beach "
         "metros."),
        ("flag", f"Pick {FLAG_CHOICES.replace(',', ' / ')} from the dropdown "
         "(free text works too). Rows flagged Interested turn green. Use "
         "the filter arrow on 'flag' to see only your picks."),
        ("notes", "Free text — next steps, contacts, application status."),
        ("bucket", "Remote (works from anywhere) or Local (on-site in your "
         "regions)."),
        ("region", "MA / RI / South FL. 'FL (statewide tag)' = the posting "
         "says only 'Florida, USA' with no city — could be your metro, "
         "could be Jacksonville; filter it out via the region column if it "
         "adds noise. Remote rows show a region when the employer also "
         "tagged a local office."),
        ("everything else", "Columns carried from getro_jobs_master.csv — "
         "see the README in Getro_All_Jobs for the full glossary."),
        ("refresh", "Re-run the fetch + build pipeline, then "
         "getro_build_pipeline.py. Re-apply your flags to a new snapshot "
         "by matching getro_job_id."),
    ]
    for a, b in lines:
        ca = WriteOnlyCell(ws, value=a)
        ca.font = bold
        cb = WriteOnlyCell(ws, value=b)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        ws.append([ca, cb])

    wb.save(OUT_XLSX)
    print(f"wrote {OUT_XLSX}", flush=True)

    # ---- splice the flag dropdown into the Pipeline sheet XML
    dv_xml = (f'<dataValidations count="1">'
              f'<dataValidation type="list" allowBlank="1" '
              f'showErrorMessage="0" sqref="A2:A{nrows + 1}">'
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
                    raise SystemExit("conditionalFormatting anchor missing; "
                                     "dropdown not spliced")
                pos = anchor + len("</conditionalFormatting>")
                xml = xml[:pos] + dv_xml + xml[pos:]
                data = xml.encode("utf-8")
                print("dropdown spliced into sheet1", flush=True)
            zout.writestr(item, data)
    os.replace(tmp, OUT_XLSX)

    # ---- verify structure via string positions (no XML parser: the sheet
    # is our own just-written output; stdlib etree is XXE-prone by policy)
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
