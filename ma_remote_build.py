#!/usr/bin/env python3
"""
ma_remote_build.py — EVERY Massachusetts, US-remote and South Florida job in
the master, on one sheet, with the startup and AI classifications as columns
beside them.

  python3 ma_remote_build.py [--worldwide-remote] [--since YYYY-MM-DD]
                             [--all-dates] [--no-dedupe] [--strict-us]

This is the whole-corpus counterpart to the "US Remote + MA" tab in
new_jobs_build.py, which only ever shows rows that are new since the previous
snapshot.  Same geo predicate, same classifiers — applied to all of
all_jobs_master.csv instead of the diff.

The startup and AI data are COLUMNS here, not a separate tab.  A row that is
neither still appears; it simply has blanks in `startup_tier` and `ai_band`.
That keeps one sheet you can filter, rather than three you have to reconcile:

  startup_tier non-blank            -> startup-shaped employer, evidence in
                                       `startup_evidence`
  ai_band non-blank                 -> an AI role, 1 is the strongest signal
  both non-blank                    -> highlighted green

Outputs MA_US_Remote_<date>.xlsx (Jobs [+ spill] / Summary) and .csv.
"""

import csv
import os
import re
import sys
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font, PatternFill
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter

import all_jobs_build as ajb
import startup_boards as sb
import startup_build as sbd
from ai_ma_remote import band_of, tier_of
from remote_us_classify import us_basis

BASE = os.path.dirname(os.path.abspath(__file__))
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
MASTER = os.path.join(BASE, "all_jobs_master.csv")


def _argval(flag, default):
    if flag in sys.argv:
        j = sys.argv.index(flag)
        if j + 1 < len(sys.argv):
            return sys.argv[j + 1]
    return default


# The master already carries all_jobs_build's own posted-date cutoff
# (2026-05-01 by default), so "all dates" here means all dates IT kept.
POSTED_SINCE = "" if "--all-dates" in sys.argv else _argval("--since", "")
WORLDWIDE = "--worldwide-remote" in sys.argv
DEDUPE = "--no-dedupe" not in sys.argv
STRICT = "--strict-us" in sys.argv

# A remote row with a BLANK `country` fails us_basis() on absent evidence, not
# on foreign evidence, and the strict rule dropped every one of them: 23,873
# rows, 269 of them AI band 1-2 including three literal "Forward Deployed
# Engineer" postings (measured against the 2026-08-08 master).  Humana and
# GitLab were in there next to genuinely foreign rows, so neither trusting nor
# discarding the whole set is right.
#
# Admit the ones carrying indirect US evidence:
#   * the location names no country the master itself has ever called non-US
#     (and is not a multi-country list), AND
#   * the company posts US-located jobs elsewhere in the master, OR the salary
#     is quoted in USD.
#
# The LABEL is as load-bearing as the rule.  `us_basis` rides into triage as a
# column, so an inferred row stays sortable and auditable instead of becoming
# indistinguishable from one that said "Boston, MA" — triage/lanes.py sorts on
# it (basis_weak) to keep inferred rows behind proven ones.  `--strict-us`
# restores the old behaviour.
INFERRED_BASIS = "inferred (company/USD)"

# Gig marketplaces republish one-off client projects with no employer and no
# geography ("Remote / Online").  They have their own feed via contract_pool.py
# and would otherwise dominate the inferred set.
GIG_SRC = {"freelancer", "upwork"}

IDX = ajb.IDX
CTX = ["bucket", "us_basis", "startup_tier", "startup_evidence",
       "ai_band", "ai_tier"]
COLUMNS = CTX + ajb.COLUMNS
WIDTHS = [12, 15, 14, 42, 26, 17] + ajb.WIDTHS
# Also the dedupe preference: this sort runs immediately before the
# (company, title) dedupe below, so the lowest-ranked bucket is the copy that
# survives when one job reaches us from two sources that disagree about where
# it is.  South FL sits LAST so that adding it moved no existing row out of
# Remote.  Measured cost of that choice on the 2026-08-12 master: 11,529
# distinct South FL (company, title) keys, 703 of which also exist elsewhere —
# but 574 of those collide with MA local, which outranks Remote anyway, so
# ranking South FL ahead of Remote would have recovered only 129 rows.  Not
# worth making the change stop being purely additive.
BUCKET_RANK = {"MA local": 0, "Remote": 1, "South FL": 2}


def is_remote(r):
    return (r[IDX["remote"]] == "True" or r[IDX["work_mode"]] == "Remote"
            or "remote" in (r[IDX["location"]] or "").lower())


def geo_of(r):
    """(bucket, us_basis) or None.  Raw master row — no context prefix.

    MA is taken from all_jobs_build.regions_of, which already refuses any
    non-US country, so an MA hit is US by construction.  The remote leg is
    gated on remote_us_classify.us_basis rather than the US_HINT regex that
    ai_ma_remote/startup_build carry: US_HINT puts [A-Z]{2} inside a (?i)
    group, so ", on" (Ontario) and ", bc" read as US.

    South FL is the third market and comes LAST on purpose, so adding it was
    purely additive: every row that bucketed MA local or Remote before it
    existed still does.  A "Miami, FL — Remote" posting therefore stays Remote,
    which is right twice over — it is already reachable from the AI/startup
    lanes, and it leaves `South FL` meaning what the lane label promises, a job
    you have to be in South Florida to take.

    The `not is_remote(r)` is doing real work and is not a restatement of the
    leg above it.  A remote row whose us_basis cannot be proved falls THROUGH
    that leg rather than returning from it, and without this guard it would land
    in South FL by the back door.  Measured on the 2026-08-12 master that was 6
    rows, and they are the argument for the guard rather than against it: three
    were "Miami-Dade, Broward and Palm Beach counties" region-remote roles, and
    two were Philippines virtual-assistant gigs quoting a Miami client address.

    It also matches the PRIMARY location segment only, where the MA leg matches
    the whole string.  MA is the home market and a multi-city req there is still
    worth seeing; South FL is a secondary market where those reqs are noise.
    Measured on the 2026-08-12 master: matching any segment gives 18,179 rows,
    the primary segment 16,030, and the 2,149 difference is enterprise reqs
    enumerating 100+ cities (Scholastic, Elevance, DoorDash) that are no more a
    Miami job than they are any other.
    """
    loc = r[IDX["location"]] or ""
    state = r[IDX["state"]] or ""
    country = r[IDX["country"]] or ""
    if "MA" in ajb.regions_of(loc, state, country):
        return "MA local", us_basis(country, state, loc) or "state"
    if is_remote(r):
        basis = us_basis(country, state, loc)
        if basis:
            return "Remote", basis
        if WORLDWIDE:
            return "Remote", "(non-US)"
    if not is_remote(r) and "South FL" in ajb.regions_of(loc.split(";")[0],
                                                         state, country):
        return "South FL", us_basis(country, state, loc) or "state"
    return None


def infer_candidate(r):
    """Is this row eligible for the inferred-US second chance?

    Only ever consulted for rows geo_of() already rejected, so "not MA" and
    "no hard US basis" are established by the time we get here.
    """
    if STRICT or WORLDWIDE:
        return False                      # nothing to infer: off, or already in
    if (r[IDX["country"]] or "").strip():
        return False                      # a country IS stated — and it is not US
    if r[IDX["source"]] in GIG_SRC:
        return False
    return is_remote(r)


# Words a location may contain and still be US-plausible: they say "remote" or
# they say "US", and nothing else.  ANY other token disqualifies the row.
#
# This is an allowlist because the blocklist version of it failed.  Rejecting
# locations that name a foreign COUNTRY leaves every row whose location names a
# foreign city or region — "Gurugram", "Nairobi", "APAC", "EMEA", "3 Locations"
# — looking clean, because none of those strings is a country name.  Measured
# on the 2026-08-08 master, that let 4,029 rows in and the AI ones among them
# were almost all offshore.  An allowlist cannot fail that way: an unrecognised
# token is a reason to reject, not a gap in a list someone has to maintain.
_OK_TOK = {
    "remote", "remotely", "work", "from", "home", "wfh", "telecommute",
    "virtual", "virtually", "anywhere", "distributed", "based", "office",
    "fully", "full", "time", "100", "online", "job", "jobs", "position",
    "opportunity", "location", "flexible",
    "us", "usa", "u.s", "u.s.", "united", "states", "america", "american",
    "nationwide", "national", "domestic",
}
_TOK = re.compile(r"[a-z0-9\.]+")


def us_plausible_location(loc):
    """Does this location string say nothing that contradicts 'US remote'?

    Empty counts: the row simply did not state a place, and the company/USD
    evidence is what carries it.  "Remote", "Work From Home", "Remote (US)"
    and "US - Remote" all pass.  "Gurugram", "APAC", "Remote - Canada" and
    "3 Locations" do not, because they carry a token that is neither a
    remote-word nor a US-word.
    """
    low = loc.strip().lower()
    if not low:
        return True
    if ";" in low:            # himalayas enumerating countries it hires from
        return False
    toks = _TOK.findall(low)
    return bool(toks) and all(t in _OK_TOK for t in toks)


def ai_of(r):
    """(band_label, ai_tier) or ("", "")."""
    title = r[IDX["title"]] or ""
    t = tier_of({"title": title, "skills": r[IDX["skills"]] or ""})
    if not t:
        return "", ""
    _, label = band_of(title, t[0])
    return label, t[0]


def startup_of(r, verdicts, co_all):
    """(tier, evidence) or ("", "") — startup_build.py on a raw row."""
    src = r[IDX["source"]]
    if src in sbd.EXCL_SRC:
        return "", ""
    stage = (r[IDX["company_stage"]] or "").strip()
    hc = (r[IDX["company_size"]] or "").strip()
    if stage in sbd.LATE or hc == sbd.BIG_HC:
        return "", ""

    board_toks = [b.strip() for b in (r[IDX["boards"]] or "").split(";")
                  if b.strip() and ":" not in b]
    known = [b for b in board_toks if b in verdicts]
    mine = [b for b in known if verdicts[b] == "vc"]
    on_vc = bool(mine)
    if known and not mine:            # every known board is junk/ecosystem
        return "", ""

    tier = evidence = ""
    if stage in sbd.FUNDED and hc in sbd.FUNDED_HC:
        tier = "1 Funded"
        evidence = (f"{stage} · "
                    f"{'<=500' if hc in sbd.SMALL_HC else '501-1000'} staff")
    # "Pre Seed" is Getro's junk default, not a round — two corroborating
    # signals required before it counts as funded.
    elif stage == "Pre Seed" and hc in sbd.SMALL_HC and on_vc:
        tier = "1 Funded"
        evidence = f"Pre Seed · <=500 staff · VC board: {mine[0]}"
    elif on_vc and hc in sbd.SMALL_HC:
        tier = "2 VC-backed"
        evidence = f"VC board: {mine[0]}" + (
            f" (+{len(mine) - 1})" if len(mine) > 1 else "")
    elif on_vc and not hc:
        tier = "2 VC-backed"
        evidence = f"VC board: {mine[0]} · size unknown"
    elif src in sbd.ATS_DENSE and not stage and not hc:
        tier = "3 Startup ATS"
        evidence = f"{src} ATS only (startup-dense)"
    elif src in sbd.ATS_MIXED and not stage and not hc:
        tier = "4 SMB ATS"
        evidence = f"{src} ATS only (mixed SMB)"
    if not tier:
        return "", ""

    co = (r[IDX["company"]] or "").strip().lower()
    mid = sbd.HC_MID.get(hc)
    cap = min(sbd.BULK_MAX,
              max(sbd.BULK_FLOOR, sbd.BULK_PER_HEAD * mid)) if mid \
        else sbd.BULK_MAX
    if co_all.get(co, 0) > cap:
        return "", ""
    if sbd.JUNK_TITLE.search(r[IDX["title"]] or ""):
        return "", ""
    return tier, evidence


def main():
    csv.field_size_limit(10 ** 9)
    if not os.path.exists(MASTER):
        sys.exit(f"master missing: {MASTER}")

    verdicts = sb.load()
    if verdicts is None:
        print("WARNING: startup_boards.csv missing — Tier 2 (VC-backed) cannot "
              "be assigned. Run startup_boards.py first.", flush=True)
        verdicts = {}

    # ---- one disk pass: keep the geo matches, and count openings per company
    # across the WHOLE master (the bulk-poster cap needs the global figure, not
    # the figure within this slice).
    print(f"streaming {os.path.basename(MASTER)} "
          f"({os.path.getsize(MASTER) / 1e9:.2f} GB) ...", flush=True)
    hits, co_all = [], {}
    # Resolved AFTER the pass, not during it: the inferred-US test needs two
    # facts about the whole corpus (which companies prove they are US, which
    # country names the master treats as foreign) that are not known until the
    # last row is read.  Deferring the candidates keeps this a ONE disk pass —
    # the same trick co_all already uses for the bulk-poster cap.
    maybe, us_co = [], set()
    total = cut = 0
    with open(MASTER, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        hdr_row = next(r)
        ncols = ajb.check_header(hdr_row, MASTER)
        ci, pi = IDX["company"], IDX["posted"]
        si, li, yi = IDX["state"], IDX["location"], IDX["country"]
        for row in r:
            total += 1
            # Against the HEADER's width, not the current schema's: a master
            # written before an optional column was appended is one short of
            # len(ajb.COLUMNS), and comparing to that would skip every row in
            # the file rather than none.
            if len(row) < ncols:
                continue
            co = (row[ci] or "").strip().lower()
            if co:
                co_all[co] = co_all.get(co, 0) + 1
            if not STRICT and co and us_basis(row[yi] or "", row[si] or "",
                                              row[li] or ""):
                us_co.add(co)
            g = geo_of(row)
            if not g:
                if infer_candidate(row):
                    maybe.append(row)
                continue
            if POSTED_SINCE:
                p = (row[pi] or "").strip()
                if p and p < POSTED_SINCE:
                    cut += 1
                    continue
            hits.append((g, row))
            if total % 500_000 == 0:
                print(f"  {total:,} rows, {len(hits):,} matched ...",
                      flush=True)

    # ---- the inferred-US second chance (see INFERRED_BASIS)
    if maybe:
        added = 0
        for row in maybe:
            if not us_plausible_location(row[IDX["location"]] or ""):
                continue
            co = (row[IDX["company"]] or "").strip().lower()
            if not (co in us_co
                    or (row[IDX["salary_currency"]] or "").upper() == "USD"):
                continue
            if POSTED_SINCE:
                p = (row[IDX["posted"]] or "").strip()
                if p and p < POSTED_SINCE:
                    cut += 1
                    continue
            hits.append((("Remote", INFERRED_BASIS), row))
            added += 1
        print(f"  {added:,} inferred-US rows admitted of {len(maybe):,} "
              f"blank-country remote candidates "
              f"({len(us_co):,} companies proven US)", flush=True)
    print(f"master {total:,} rows -> {len(hits):,} MA/US-remote/South-FL"
          + (f" ({cut:,} cut by --since {POSTED_SINCE})" if cut else ""),
          flush=True)

    # ---- classify
    out = []
    for (bucket, basis), row in hits:
        st, ev = startup_of(row, verdicts, co_all)
        band, atier = ai_of(row)
        out.append([bucket, basis, st, ev, band, atier] + row)

    pi = IDX["posted"] + len(CTX)
    out.sort(key=lambda r: (r[pi] or ""), reverse=True)     # newest first
    out.sort(key=lambda r: BUCKET_RANK[r[0]])               # MA before Remote

    n_dup = 0
    if DEDUPE:
        # One job reached by two sources is two job_keys but one job to look
        # at.  company+title, not location: the Getro and ATS copies of a row
        # disagree about where it is.  Sorted first, so the survivor is the
        # newest MA-preferring copy.
        seen, keep = set(), []
        for r in out:
            k = ((r[IDX["company"] + len(CTX)] or "").strip().lower(),
                 (r[IDX["title"] + len(CTX)] or "").strip().lower())
            if k in seen:
                n_dup += 1
                continue
            seen.add(k)
            keep.append(r)
        out = keep

    n_ma = sum(1 for r in out if r[0] == "MA local")
    n_sfl = sum(1 for r in out if r[0] == "South FL")
    n_rem = len(out) - n_ma - n_sfl
    n_start = sum(1 for r in out if r[2])
    n_ai = sum(1 for r in out if r[4])
    n_both = sum(1 for r in out if r[2] and r[4])
    print(f"  {len(out):,} rows ({n_ma:,} MA, {n_rem:,} US remote, "
          f"{n_sfl:,} South FL"
          + (f"; {n_dup:,} duplicate company+title collapsed" if n_dup else "")
          + ")", flush=True)
    print(f"  {n_start:,} startup-tiered | {n_ai:,} AI roles | "
          f"{n_both:,} both", flush=True)

    # ---- CSV
    out_csv = os.path.join(BASE, f"MA_US_Remote_{SNAP}.csv")
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        w.writerows(out)
    print(f"wrote {out_csv}", flush=True)

    # ---- workbook
    wb = Workbook(write_only=True)
    bold = Font(bold=True)
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE",
                        fill_type="solid")
    last = get_column_letter(len(COLUMNS))
    cap = ajb.XLSX_CAP
    chunks = [out[i:i + cap] for i in range(0, len(out), cap)] or [[]]
    for n, chunk in enumerate(chunks, 1):
        ws = wb.create_sheet("Jobs" if n == 1 else f"Jobs {n}")
        for j, wd in enumerate(WIDTHS, 1):
            ws.column_dimensions[get_column_letter(j)].width = wd
        ws.freeze_panes = f"{get_column_letter(len(CTX) + 1)}2"
        ws.auto_filter.ref = f"A1:{last}{len(chunk) + 1}"
        # green = startup AND AI, the intersection worth looking at first
        ws.conditional_formatting.add(
            f"A2:{last}{len(chunk) + 1}",
            FormulaRule(formula=['AND($C2<>"",$E2<>"")'], fill=green,
                        stopIfTrue=False))
        ws.append(ajb.hdr(ws, COLUMNS, bold))
        for row in chunk:
            ws.append([ajb.safe(v) for v in row])
    if len(chunks) > 1:
        print(f"split across {len(chunks)} tabs (Excel row cap)", flush=True)

    ws = wb.create_sheet("Summary")
    ws.column_dimensions["A"].width = 46
    ws.column_dimensions["B"].width = 16

    def sec(title, pairs, limit=None):
        ws.append([])
        c = WriteOnlyCell(ws, value=title)
        c.font = bold
        ws.append([c])
        for k, v in sorted(pairs.items(), key=lambda kv: -kv[1])[:limit] \
                if limit else sorted(pairs.items(), key=lambda kv: -kv[1]):
            ws.append([ajb.safe(k), v])

    def tally(fn):
        d = {}
        for r in out:
            k = fn(r) or "(none)"
            d[k] = d.get(k, 0) + 1
        return d

    c = WriteOnlyCell(ws, value="MASSACHUSETTS + US REMOTE — every job in the "
                                "master")
    c.font = bold
    ws.append([c])
    ws.append(["Snapshot (UTC)", SNAP])
    ws.append(["Master rows scanned", total])
    ws.append(["Rows on this sheet", len(out)])
    ws.append(["  MA (on-site, hybrid or remote-in-MA)", n_ma])
    ws.append(["  US remote (outside MA)", n_rem])
    ws.append(["  South FL (on-site or hybrid)", n_sfl])
    ws.append(["  duplicate company+title collapsed", n_dup])
    ws.append([])
    ws.append(["Startup-tiered rows", n_start])
    ws.append(["AI roles", n_ai])
    ws.append(["Both (highlighted green)", n_both])
    ws.append([])
    c = WriteOnlyCell(ws, value="How to read the columns")
    c.font = bold
    ws.append([c])
    ws.append(["bucket", "MA local = physically in Massachusetts (on-site, "
                         "hybrid, or remote-in-MA). Remote = remote and "
                         "US-eligible. South FL = physically in the Miami / "
                         "Fort Lauderdale / Palm Beach metro, on-site or "
                         "hybrid only — a remote job that happens to name a "
                         "South FL city stays in Remote."])
    ws.append(["us_basis", "which signal proved US eligibility: country, "
                           "state, location wording, or worldwide-open."])
    ws.append(["startup_tier", "blank = no startup evidence, NOT 'not a "
                               "startup'. Tiers 1-2 rest on a funding round "
                               "or a VC-portfolio board. Tiers 3-4 are "
                               "inferred from which ATS the company uses and "
                               "carry no per-company data — treat them as a "
                               "prior, not a fact."])
    ws.append(["ai_band", "1 is strongest: Claude/agentic/FDE, then AI "
                          "engineer/LLM/GenAI, then other core AI, then "
                          "AI-ML adjacent, then AI tooling in skills only."])
    ws.append(["Aggregator caveat",
               "rows from aggregators (himalayas, careerjet, jobgether, "
               "workday and the rest of startup_build.EXCL_SRC) can never "
               "carry a startup tier — they publish no company stage, "
               "headcount or board. They still appear here as jobs."])

    sec("By bucket", {"MA local": n_ma, "Remote": n_rem, "South FL": n_sfl})
    sec("By startup tier", tally(lambda r: r[2] or "(no startup evidence)"))
    sec("By AI band", tally(lambda r: r[4] or "(not an AI role)"))
    sec("By us_basis", tally(lambda r: r[1]))
    sec("By work mode", tally(lambda r: r[IDX["work_mode"] + len(CTX)]))
    sec("By source", tally(lambda r: r[IDX["source"] + len(CTX)]))
    sec("By posted date (top 40)",
        tally(lambda r: r[IDX["posted"] + len(CTX)]), 40)
    sec("Top 60 companies", tally(lambda r: r[IDX["company"] + len(CTX)]), 60)

    out_xlsx = os.path.join(BASE, f"MA_US_Remote_{SNAP}.xlsx")
    wb.save(out_xlsx)
    print(f"wrote {out_xlsx} ({os.path.getsize(out_xlsx) / 1e6:.0f} MB)",
          flush=True)


if __name__ == "__main__":
    main()
