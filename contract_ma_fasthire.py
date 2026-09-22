#!/usr/bin/env python3
"""
contract_ma_fasthire.py — from contract_master.csv, build a ranked workbook of
MA-relevant contract gigs that are structurally likely to hire FAST.

Scope (decided with the user):
  geo    MA-located OR US-remote OR commutable nearby (RI / southern NH /
         northern CT).  Non-US and EU (m/w/d ...) roles dropped.
  field  professional / office / tech only — clinical per-diem (PT/RN/NP/PA...)
         and retail-floor / manual-labor roles dropped.  Freelancer.com
         marketplace microtasks (source_type=marketplace) dropped.

Ranking — "structural speed first":
  primary   structural_score  (staffing-agency source, contingent type that
                               hires on a rolling/urgent basis, cross-posting)
  tie 1     urgency_score     (urgent/immediate/ASAP/temp-to-perm/1099 in title)
  tie 2     recency_score     (how recently posted)
  A 0-100 composite `fast_hire_score` and a plain-English `why_fast` column are
  shown so the rank is auditable.

Output: MA_Contract_FastHire_<date>.xlsx  (Top 100 / All Ranked / README) with a
flag dropdown (Interested/Maybe/No), matching the other personal pipelines.
"""
import csv
import os
import re
import zipfile
from datetime import date, datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.cell import WriteOnlyCell
from openpyxl.formatting.rule import FormulaRule, CellIsRule
from openpyxl.utils import get_column_letter

BASE = os.path.dirname(os.path.abspath(__file__))
IN_CSV = os.path.join(BASE, "contract_master.csv")
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
TODAY = date(2026, 7, 21)                      # reference for recency scoring
OUT_XLSX = os.path.join(BASE, f"MA_Contract_FastHire_{SNAP}.xlsx")
FLAG_CHOICES = "Interested,Maybe,No"

# ---------------------------------------------------------------- geography
# Match against the PRIMARY location segment (before the first ';') plus the
# parsed state field, and require a state qualifier — bare city names alone
# match across states ("New Haven, Indiana" is not CT; "Springfield" is 40
# places). A handful of unambiguous MA cities are allowed as a backstop.
EU_RE = re.compile(r"\(m/w/d\)|\(w/m/d\)|\(m/f/d\)|\(m/f/x\)|\(f/m/d\)|"
                   r"\(gn\)|\(m/w/x\)", re.I)
MA_RE = re.compile(r"\bmassachusetts\b|,\s*ma\b|"
                   r"\b(?:boston|cambridge|worcester|somerville|brookline|"
                   r"newton|quincy|medford|waltham|framingham)\b", re.I)
RI_RE = re.compile(r"\brhode island\b|,\s*ri\b|\bprovidence\b|\bpawtucket\b|"
                   r"\bcranston\b|\bwarwick,\s*ri\b", re.I)
NH_RE = re.compile(r"\bnew hampshire\b|,\s*nh\b|\bnashua,?\s*nh\b|"
                   r"\bmanchester,\s*nh\b|\bportsmouth,\s*nh\b", re.I)
CT_RE = re.compile(r"\bconnecticut\b|,\s*ct\b|\bhartford,\s*ct\b|"
                   r"\bnew haven,\s*ct\b|\bstamford,\s*ct\b", re.I)


def is_us(country):
    return country.strip().upper().replace(".", "").replace(" ", "") in \
        {"", "US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA"}


def is_remote(r):
    if r["remote"].strip().lower() in ("true", "1", "yes"):
        return True
    if r["work_mode"].strip().lower() == "remote":
        return True
    return "remote" in (r["location"] + " " + r["city"]).lower()


def geo_of(r):
    """Return (bucket, remote_ok) or (None, _)."""
    if not is_us(r["country"]):
        return None, False
    prim = (r["location"] or "").split(";")[0].strip()   # primary segment only
    blob = f"{prim} {r['city']} {r['state']}"
    st = r["state"].strip().lower()
    remote = is_remote(r)
    if st in ("ma", "massachusetts") or MA_RE.search(blob):
        return "MA", remote
    if (st in ("ri", "rhode island", "nh", "new hampshire", "ct",
               "connecticut")
            or RI_RE.search(blob) or NH_RE.search(blob) or CT_RE.search(blob)):
        return "Nearby (RI/NH/CT)", remote
    if remote:
        return "Remote (US)", True
    return None, False


# ---------------------------------------------------------------- field filter
# Drop clinical patient-care roles and retail-floor / manual-labor / skilled-
# trade roles; keep office / professional / tech / lab-scientist / creative /
# finance work.  Two patterns: STEM matches a word PREFIX (leading \b only, so
# "phlebotom" catches "phlebotomist"); WORD needs both boundaries (so short
# acronyms like "pa"/"rn" don't match inside other words, and "server" is kept
# because it appears in tech titles like "SQL Server Developer").
DROP_STEM = re.compile(r"\b(?:"
    # clinical / patient-care
    r"physical therap|occupational therap|speech.?language|speech therap|"
    r"phlebotom|radiolog|sonograph|mammograph|ultrasound tech|veterinar|"
    r"pharmac|hygienist|nursing|nurse practitioner|registered nurse|nurse aide|"
    r"physician assistant|physician|surgeon|anesthes|psychiatr|psycholog|"
    r"chiroprac|optometr|respiratory therap|dietit|epidemiolog|clinician|"
    r"phlebot|patient care|home health|medical assistant|caregiver|"
    r"direct support|personal care|scrub tech|surgical tech|behavior technician|"
    r"licensed practical|clinical nurse|"
    # Added 2026-08-06: the fast-money lane has no fit floor, so these reached
    # the top of it.  They pass the rules above because the stems there are
    # word-PREFIX anchored and these forms hide the stem inside or after
    # another word — "Teleradiologist" is not "\bradiolog", "Telehealth MD" is
    # not "\bphysician", and \bmd\b/\bnp\b stay excluded on purpose (Maryland,
    # Managing Director).  Prefixing "tele" fixes the first class; the rest are
    # specialties with no non-clinical reading.
    r"telehealth|telemedicine|teleradiolog|telepsych|"
    r"gynecolog|obstetric|pediatric|cardiolog|dermatolog|oncolog|neurolog|"
    r"urolog|endocrinolog|gastroenterolog|rheumatolog|nephrolog|hematolog|"
    r"orthoped|podiatr|hospitalist|midwif|nurse|"
    # retail / food / manual-labor / trades
    r"cashier|barista|bartender|line cook|prep cook|dishwash|busser|waiter|"
    r"waitress|food runner|food service|warehouse|forklift|merchandis|grocery|"
    r"housekeep|custodian|janitor|groundskeep|landscap|valet|delivery driver|"
    r"truck driver|route driver|laborer|dock worker|assembler|machine operator|"
    r"sanitation|lifeguard|cable tech|low voltage|electrical apprentice|"
    r"electrician|install technician|installation tech|field install|lineman|"
    r"plumber|welder|machinist|pipefitter|carpenter|roofer|fabricator|"
    r"animal care|animal handler|kennel|groomer|meat cutter|butcher|stocker|"
    r"bagger|security guard|security officer|crew member|team member|"
    r"brand ambassador|seasonal sales|package handler|material handler|"
    r"order picker|cart attendant|sales associate|retail associate|"
    r"store associate|stock associate|deli clerk|bakery clerk|floral|painter|"
    r"\bhvac\b|maintenance technician|maintenance mechanic"
    r")", re.I)
# Only unambiguous clinical / CDL acronyms — "md"/"pa"/"np"/"do"/"driver" are
# excluded because they collide with Maryland / Pennsylvania / Managing
# Director / "Device Driver Engineer" etc.
DROP_WORD = re.compile(r"\b(?:rn|cna|lpn|cma|hha|pct|pca|rbt|ekg|cdl|dvm|slp|"
                       # NOT "do" (Doctor of Osteopathy): it is also the most
                       # common verb in English and would eat the whole list.
                       r"emt|aprn|crna|fnp|pmhnp|dnp|dds|dmd|lcsw|lmhc)s?\b",
                       re.I)


def field_ok(r):
    if r["source_type"] == "marketplace":
        return False
    t = r["title"] or ""
    if EU_RE.search(t):
        return False
    return not (DROP_STEM.search(t) or DROP_WORD.search(t))


# ---------------------------------------------------------------- scoring
AGENCY_SRC = {"agency", "staffing_ats"}
TYPE_SPEED = {                     # structural: how fast this type tends to fill
    "Per Diem": 20, "Temp": 18, "Contract-to-Hire": 18, "Interim": 16,
    "Seasonal": 16, "Freelance": 14, "Fractional": 12, "Contract": 10,
}
URGENT_RE = re.compile(
    r"\b(urgent(?:ly)?|immediate(?:ly)?|asap|start (?:now|immediately|asap)|"
    r"temp[- ]to[- ]perm|temp[- ]to[- ]hire|contract[- ]to[- ]hire|"
    r"now hiring|urgently hiring|immediate (?:need|opening|start|hire)|"
    r"quick start|start date asap|hiring now|\b1099\b|\bw2\b|w-2)\b", re.I)
DURATION_RE = re.compile(
    r"\b(\d{1,2})[- ]?(?:month|mo|week|wk)\b|\b(?:short[- ]term|"
    r"3[- ]month|6[- ]month|12[- ]month|13[- ]week|6[- ]week)\b", re.I)


def parse_posted(s):
    s = (s or "").strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def score(r):
    reasons = []
    # --- structural (primary) ---
    structural = 0
    src = r["source_type"]
    board = (r["source"] or "").lower()
    if src in AGENCY_SRC:
        structural += 25
        agency = (r["company"] or r["board"] or r["source"] or "").strip()
        reasons.append(f"Staffing agency ({agency})" if agency
                       else "Staffing-agency posting")
    et = r["employment_type"].strip()
    structural += TYPE_SPEED.get(et, 8)
    if et:
        reasons.append(et)
    try:
        nsrc = int(r["n_sources"] or 1)
    except ValueError:
        nsrc = 1
    if nsrc > 1:
        bonus = min(nsrc - 1, 3) * 2
        structural += bonus
        reasons.append(f"cross-posted x{nsrc}")

    # --- urgency (tie 1) ---
    urgency = 0
    title = r["title"] or ""
    um = URGENT_RE.findall(title)
    if um:
        urgency += 12
        reasons.append(f"'{um[0] if isinstance(um[0], str) else um[0][0]}' in title")
    if DURATION_RE.search(title):
        urgency += 5
        reasons.append("defined duration")

    # --- recency (tie 2) ---
    recency = 0
    p = parse_posted(r["posted"])
    if p:
        days = (TODAY - p).days
        if days < 0:
            days = 0
        if days <= 3:
            recency = 15; reasons.append(f"posted {days}d ago")
        elif days <= 7:
            recency = 12; reasons.append(f"posted {days}d ago")
        elif days <= 14:
            recency = 8; reasons.append(f"posted {days}d ago")
        elif days <= 30:
            recency = 4; reasons.append(f"posted {days}d ago")

    composite = min(structural + urgency + recency, 100)
    return structural, urgency, recency, composite, " · ".join(reasons)


def main():
    csv.field_size_limit(10 ** 7)
    rows = list(csv.DictReader(open(IN_CSV, encoding="utf-8")))
    n_all = len(rows)

    kept = []
    n_geo = n_field = 0
    for r in rows:
        bucket, remote_ok = geo_of(r)
        if not bucket:
            continue
        n_geo += 1
        if not field_ok(r):
            continue
        n_field += 1
        s, u, c, comp, why = score(r)
        r["_geo"] = bucket
        r["_remote_ok"] = "Yes" if remote_ok else ""
        r["_structural"] = s
        r["_urgency"] = u
        r["_recency"] = c
        r["_score"] = comp
        r["_why"] = why
        kept.append(r)

    # light extra dedupe: same company+title+geo -> keep best score, count listings
    best = {}
    for r in kept:
        k = ((r["company"] or "").strip().lower(),
             (r["title"] or "").strip().lower(), r["_geo"])
        if k not in best or r["_score"] > best[k]["_score"]:
            if k in best:
                r["_dupes"] = best[k].get("_dupes", 1) + 1
            best[k] = r
        else:
            best[k]["_dupes"] = best[k].get("_dupes", 1) + 1
    final = list(best.values())
    for r in final:
        r.setdefault("_dupes", 1)

    # sort: structural first, then urgency, recency, composite
    final.sort(key=lambda r: (r["_structural"], r["_urgency"], r["_recency"],
                              r["_score"]), reverse=True)

    from collections import Counter
    geo_counts = Counter(r["_geo"] for r in final)
    print(f"master {n_all} -> geo-in-scope {n_geo} -> field-ok {n_field} "
          f"-> deduped {len(final)}", flush=True)
    print("  by geo:", dict(geo_counts), flush=True)
    print("  top structural sources:",
          dict(Counter(r["source_type"] for r in final[:200])), flush=True)

    # ------------------------------------------------------------- workbook
    COLS = ["flag", "notes", "rank", "fast_hire_score", "why_fast", "geo",
            "remote_ok", "employment_type", "company", "title", "source",
            "source_type", "location", "salary_min", "salary_max",
            "salary_period", "posted", "listings", "apply_url"]
    WIDTHS = [11, 22, 6, 9, 52, 17, 9, 16, 26, 46, 15, 12, 30, 10, 10, 9, 11,
              8, 50]

    def rowvals(r, rank):
        sal_p = r.get("salary_period", "")
        return ["", "", rank, r["_score"], r["_why"], r["_geo"],
                r["_remote_ok"], r["employment_type"], r["company"],
                r["title"], r["source"], r["source_type"], r["location"],
                r.get("salary_min", ""), r.get("salary_max", ""), sal_p,
                r["posted"], r.get("_dupes", 1), r["apply_url"]]

    wb = Workbook(write_only=True)
    bold = Font(bold=True)
    green = PatternFill("solid", fgColor="C6EFCE")
    hot = PatternFill("solid", fgColor="FFC7CE")     # score >=38 (top tier)
    warm = PatternFill("solid", fgColor="FFEB9C")    # score 28-37
    link = Font(color="0563C1", underline="single")

    def write_sheet(name, data, dropdown=True):
        ws = wb.create_sheet(name)
        ncols = len(COLS)
        nrows = len(data)
        for idx, wd in enumerate(WIDTHS, 1):
            ws.column_dimensions[get_column_letter(idx)].width = wd
        ws.freeze_panes = "C2"
        ws.auto_filter.ref = f"A1:{get_column_letter(ncols)}{nrows + 1}"
        last = get_column_letter(ncols)
        # interested rows -> green (flag col A)
        ws.conditional_formatting.add(
            f"A2:{last}{nrows + 1}",
            FormulaRule(formula=['$A2="Interested"'], fill=green,
                        stopIfTrue=False))
        # score heat on the score column (D)
        ws.conditional_formatting.add(
            f"D2:D{nrows + 1}",
            CellIsRule(operator="greaterThanOrEqual", formula=["38"], fill=hot))
        ws.conditional_formatting.add(
            f"D2:D{nrows + 1}",
            CellIsRule(operator="between", formula=["28", "37"], fill=warm))
        hdr = []
        for c in COLS:
            cell = WriteOnlyCell(ws, value=c)
            cell.font = bold
            hdr.append(cell)
        ws.append(hdr)
        for rank, r in enumerate(data, 1):
            vals = rowvals(r, rank)
            cells = []
            for ci, v in enumerate(vals):
                cell = WriteOnlyCell(ws, value=v)
                if ci == ncols - 1 and v:            # apply_url -> hyperlink
                    cell.hyperlink = v
                    cell.font = link
                cells.append(cell)
            ws.append(cells)
        return nrows

    n_top = write_sheet("Top 100", final[:100])
    n_all_ranked = write_sheet("All Ranked", final)

    # README
    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 24
    ws.column_dimensions["B"].width = 112
    lines = [
        ("MA contract gigs — fast-hire ranked", f"Snapshot {SNAP} (UTC). "
         f"{len(final)} gigs ranked by how quickly they are likely to hire."),
        ("Scope — geography", "MA-located, US-remote, or commutable nearby "
         "(RI / southern NH / northern CT). Non-US and EU (m/w/d) roles "
         "excluded."),
        ("Scope — field", "Professional / office / tech / lab / creative / "
         "finance. Clinical patient-care (PT/RN/NP/PA...) and retail-floor / "
         "manual-labor roles excluded, plus Freelancer.com microtasks."),
        ("Ranking", "Structural hire-speed FIRST, then urgency, then recency:"),
        ("  structural_score", "Staffing-agency source (+25); contingent type "
         "that fills fast — Per Diem/Temp/Contract-to-Hire/Interim/Seasonal/"
         "Freelance > plain Contract; cross-posted on multiple feeds (+)."),
        ("  urgency_score", "'urgent / immediate / ASAP / temp-to-perm / "
         "contract-to-hire / now hiring / 1099 / W2' in the title (+12); a "
         "defined contract duration (+5)."),
        ("  recency_score", "Posted <=3d (+15), <=7d (+12), <=14d (+8), "
         "<=30d (+4)."),
        ("fast_hire_score", "Composite of the three (higher = faster; ~0-83 in "
         "practice since structural signals dominate). Top-tier >=38 shaded "
         "red, 28-37 amber. RANK is the primary output; why_fast spells out "
         "which signals fired."),
        ("flag / notes", f"Pick {FLAG_CHOICES.replace(',', ' / ')} from the "
         "dropdown; Interested rows turn green. notes = free text."),
        ("listings", "How many near-identical postings (same company+title) "
         "collapsed into this row — higher = actively/continuously hiring."),
        ("Tabs", "Top 100 = headline picks. All Ranked = full pool, same "
         "columns and sort."),
        ("Source data", "contract_master.csv (rebuilt this run from the fresh "
         "07-21 Getro + Lever contract feeds; ATS/phenom/bullhorn feeds are "
         "2 days old). Re-run: python3 contract_ma_fasthire.py."),
        ("Caveat", "'Likely to hire quickly' is a heuristic from structural "
         "signals, not a guarantee. Locations are freeform across dozens of "
         "feeds; geo matching is best-effort. Verify each posting before "
         "applying."),
    ]
    for a, b in lines:
        ca = WriteOnlyCell(ws, value=a)
        ca.font = bold
        cb = WriteOnlyCell(ws, value=b)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        ws.append([ca, cb])

    wb.save(OUT_XLSX)
    print(f"wrote {OUT_XLSX}  (Top 100 + {n_all_ranked} All Ranked)", flush=True)

    # ---- splice the flag dropdown into both data sheets (sheet1, sheet2)
    def dv_xml(nrows):
        return (f'<dataValidations count="1"><dataValidation type="list" '
                f'allowBlank="1" showErrorMessage="0" sqref="A2:A{nrows + 1}">'
                f'<formula1>"{FLAG_CHOICES}"</formula1></dataValidation>'
                f'</dataValidations>')
    counts = {"xl/worksheets/sheet1.xml": n_top,
              "xl/worksheets/sheet2.xml": n_all_ranked}
    tmp = OUT_XLSX + ".tmp"
    with zipfile.ZipFile(OUT_XLSX) as zin, \
            zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename in counts:
                xml = data.decode("utf-8")
                anchor = xml.rfind("</conditionalFormatting>")
                if anchor == -1:
                    raise SystemExit(f"cf anchor missing in {item.filename}")
                pos = anchor + len("</conditionalFormatting>")
                xml = xml[:pos] + dv_xml(counts[item.filename]) + xml[pos:]
                data = xml.encode("utf-8")
            zout.writestr(item, data)
    os.replace(tmp, OUT_XLSX)

    ok = True
    with zipfile.ZipFile(OUT_XLSX) as z:
        for sheet in counts:
            xml = z.read(sheet).decode("utf-8")
            i_dv = xml.find("<dataValidations")
            i_cf = xml.rfind("</conditionalFormatting>")
            i_pm = xml.find("<pageMargins")
            if not (i_dv != -1 and i_cf != -1 and i_dv > i_cf
                    and (i_pm == -1 or i_dv < i_pm)
                    and xml.count("<dataValidations") == 1):
                ok = False
    print(f"verify: dropdowns present and schema-ordered: {ok}", flush=True)
    if not ok:
        raise SystemExit("dataValidations splice verification failed")


if __name__ == "__main__":
    main()
