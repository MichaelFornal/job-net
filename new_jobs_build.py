#!/usr/bin/env python3
"""
new_jobs_build.py — every job in the refreshed all-jobs master that was NOT in
the previous snapshot, as its own workbook.

  python3 new_jobs_build.py [--baseline FILE] [--since YYYY-MM-DD]

The diff key is `job_key`, the canonical-ATS key all_jobs_build.py already
writes into the master.  Its docstring is explicit that the key is stable
across snapshots ("so flags can be carried to the next monolith"), which is
exactly the property a new-since-last-run diff needs: a job that merely moved
position, gained a source, or had its URL cosmetically rewritten keeps the same
key and is therefore NOT reported as new.

Two kinds of row end up in the diff, and conflating them would overstate how
much genuinely new hiring happened, so they are split into a `new_type`
column you can filter on:

  Posted since last run      real posted date on/after the previous snapshot.
                             Genuinely new hiring activity.
  Newly discovered           an older posted date — the job existed before, we
                             just had not seen it (new board, board that
                             errored last run, pagination we did not reach).
  Newly discovered (no date) no posted date at all.  Per all_jobs_build, blank
                             means the source never publishes dates, not that
                             the job is old.

Two further tabs slice the same new rows down to what is actually actionable:

  US Remote + MA   US-eligible remote roles, plus on-site/hybrid roles in
                   Massachusetts.  MA only — RI and South FL are not included.
  Startup AI       the intersection of that geo slice with a startup tier and
                   an AI tier: startup-shaped employers hiring AI builders.

Outputs New_Jobs_<date>.xlsx (New Jobs [+ spill tabs] / US Remote + MA /
Startup AI / Summary) and New_Jobs_<date>.csv.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.formatting.rule import FormulaRule
from openpyxl.utils import get_column_letter

import all_jobs_build as ajb
# The US test.  Deliberately NOT the US_HINT regex that ai_ma_remote.py and
# startup_build.py both carry: that one puts [A-Z]{2} inside a (?i) group,
# so ", on" (Ontario), ", bc" and ", nsw" all read as US.  us_basis is
# case-sensitive on abbreviations, checks membership in the real state set, and
# returns WHY a row qualified so the sheet can show its own evidence.
from remote_us_classify import us_basis
# The only importable, pure AI tiering/banding in the repo — ai_jobs_filter's
# own version is inline in its main().  band_of is ASCENDING (1 = strongest);
# do not mix it with vet_build.ai_band, which is a different regex set numbered
# descending.
from ai_ma_remote import band_of, tier_of
import startup_boards as sb
import startup_build as sbd

BASE = os.path.dirname(os.path.abspath(__file__))
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
MASTER = os.path.join(BASE, "all_jobs_master.csv")


def _argval(flag, default):
    if flag in sys.argv:
        j = sys.argv.index(flag)
        if j + 1 < len(sys.argv):
            return sys.argv[j + 1]
    return default


# JSON-escaped, one key per line.  A plain one-key-per-line file is WRONG
# here: 11 tier-3 keys (company+title, used when a row has no parseable ATS
# URL) carry newlines straight out of the source title — Freelancer.com,
# Arbeitsagentur and RemoteOK all publish multi-line titles.  Those keys split
# across lines, never matched, and reported themselves as new every run.
BASELINE = _argval("--baseline", os.path.join(
    BASE, "baseline_job_keys_2026-07-30.jsonl"))
# Previous snapshot's build date: the boundary between "posted since last run"
# and "we simply had not seen it".
SINCE = _argval("--since", "2026-07-29")

# The sources re-fetched this run.  Only these can carry genuinely new
# postings; every other source kept the JSONL it already had on disk.
#
# These are `source` COLUMN values, not run_all family names.  The distinction
# is not cosmetic: run_all's "portals" family is one rotation unit but fans out
# to ~18 distinct sources (the 8 ats_fetch platforms, phenom, bullhorn, and
# every remote board boards_fetch carries).  Listing the family name here
# instead of its sources counted 40,507 legitimately-new rows as re-keying
# artifacts on 2026-08-03 — a 17x overstatement.
#
# Derived from the distinct `source` values in the JSONLs this run rotated:
#   contract_norm/{boards_all,phenom_all,bullhorn_all,ats_all_*}.jsonl
#   plus the eight family masters.
# Re-derive it when the refresh scope changes; pass --refreshed a,b,c to override.
REFRESHED = {s.strip() for s in _argval(
    "--refreshed",
    "arbeitnow,ashby,bamboohr,braintrust,breezy,bullhorn,dover,freelancer,"
    "getro,greenhouse,himalayas,jazzhr,jobicy,lever,personio,phenom,recruitee,"
    "remoteok,remotive,rippling,smartrecruiters,teamtailor,weworkremotely,"
    "workable,workingnomads,yc"
).split(",") if s.strip()}

COLUMNS = ["new_type"] + ajb.COLUMNS
WIDTHS = [26] + ajb.WIDTHS
IDX = ajb.IDX          # index into the ORIGINAL master row (no new_type)
XLSX_CAP = ajb.XLSX_CAP

# ---- the two derived tabs -------------------------------------------------
# Both are built from `out` rows, which are [new_type] + master row — so every
# master lookup below is IDX[name] + 1.
GEO_CTX = ["bucket", "us_basis", "new_type"]
GEO_COLUMNS = GEO_CTX + ajb.COLUMNS
GEO_WIDTHS = [12, 15, 26] + ajb.WIDTHS

AI_CTX = ["ai_band", "ai_tier", "startup_tier", "evidence", "bucket",
          "new_type"]
AI_COLUMNS = AI_CTX + ajb.COLUMNS
AI_WIDTHS = [26, 17, 14, 42, 12, 26] + ajb.WIDTHS

BUCKET_RANK = {"MA local": 0, "Remote": 1}


def is_remote(row):
    """ajb.is_remote, shifted for the leading new_type column."""
    return (row[IDX["remote"] + 1] == "True"
            or row[IDX["work_mode"] + 1] == "Remote"
            or "remote" in (row[IDX["location"] + 1] or "").lower())


def geo_of(row):
    """(bucket, us_basis) for the US-remote + MA slice, or None.

    Two legs, deliberately asymmetric:
      MA local  on-site/hybrid physically in Massachusetts.  regions_of can
                also return RI / South FL / statewide-FL; those are dropped
                here — this cut is Massachusetts only.
      Remote    remote anywhere that passes the US test.  Without that test
                the leg fills with "Remote, Toronto" and "Remote, Sydney".
    """
    loc = row[IDX["location"] + 1] or ""
    state = row[IDX["state"] + 1] or ""
    country = row[IDX["country"] + 1] or ""
    remote = is_remote(row)

    in_ma = "MA" in ajb.regions_of(loc, state, country)
    # regions_of already refuses any non-US country, so an MA hit is US by
    # construction; us_basis just names which signal carried it.
    if in_ma:
        return "MA local", us_basis(country, state, loc) or "state"
    if remote:
        basis = us_basis(country, state, loc)
        if basis:
            return "Remote", basis
    return None                          # on-site outside MA, or remote non-US


def ai_of(row):
    """(band_n, band_label, ai_tier) or None."""
    title = row[IDX["title"] + 1] or ""
    shim = {"title": title, "skills": row[IDX["skills"] + 1] or ""}
    t = tier_of(shim)
    if not t:
        return None
    tier = t[0]
    n, label = band_of(title, tier)
    return n, label, tier


def startup_tier_of(row, verdicts, co_all):
    """(tier, evidence) or None — startup_build.py, adapted to a
    positional row.  Every gate here is load-bearing; see startup_build for the
    measurements behind them.
    """
    src = row[IDX["source"] + 1]
    if src in sbd.EXCL_SRC:                       # enterprise ATS + aggregators
        return None
    stage = (row[IDX["company_stage"] + 1] or "").strip()
    hc = (row[IDX["company_size"] + 1] or "").strip()
    if stage in sbd.LATE:                         # IPO / acquired / PE
        return None
    if hc == sbd.BIG_HC:                          # 1000+ staff
        return None

    # Unprefixed board tokens are Getro board names; "source:board" tokens are
    # portal boards and carry no VC signal.
    board_toks = [b.strip() for b in (row[IDX["boards"] + 1] or "").split(";")
                  if b.strip() and ":" not in b]
    known = [b for b in board_toks if b in verdicts]
    mine = [b for b in known if verdicts[b] == "vc"]
    on_vc = bool(mine)
    only_bad = bool(known) and not mine

    tier = evidence = ""
    if stage in sbd.FUNDED and hc in sbd.FUNDED_HC:
        tier = "1 Funded"
        evidence = (f"{stage} · "
                    f"{'<=500' if hc in sbd.SMALL_HC else '501-1000'} staff")
    # "Pre Seed" is Getro's junk DEFAULT, not a round — 46k of its 62k rows are
    # 1000+ employers (Walgreens, AWS, JPMorganChase). It needs two
    # corroborating signals before it counts as funded.
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
        return None

    # Junk/ecosystem boards are not evidence. A row whose only known board is
    # one of those is dropped regardless of tier.
    if only_bad:
        return None

    # Bulk posters: no company at startup scale has 200 open requisitions, and
    # more than 4 openings per employee is a staffing agency or a franchise.
    co = (row[IDX["company"] + 1] or "").strip().lower()
    n_all = co_all.get(co, 0)
    mid = sbd.HC_MID.get(hc)
    cap = min(sbd.BULK_MAX,
              max(sbd.BULK_FLOOR, sbd.BULK_PER_HEAD * mid)) if mid \
        else sbd.BULK_MAX
    if n_all > cap:
        return None
    if sbd.JUNK_TITLE.search(row[IDX["title"] + 1] or ""):
        return None
    return tier, evidence


def collapse_dups(table, ctx_len):
    """Drop repeat (company, title) pairs, keeping the first — so call this
    AFTER sorting and the survivor is the best-ranked copy.

    One job reached by two sources gets two job_keys and is genuinely two new
    rows on the New Jobs tab.  On these action tabs it is one job you look at
    once.  Keying on company+title rather than location is deliberate: the
    Getro and ATS copies of a row disagree about where it is (`Lehi, UT` vs
    `Southborough, MA` for the same job), so location in the key defeats the
    dedupe entirely.
    """
    seen, out, n = set(), [], 0
    for row in table:
        k = ((row[ajb.IDX["company"] + ctx_len] or "").strip().lower(),
             (row[ajb.IDX["title"] + ctx_len] or "").strip().lower())
        if k in seen:
            n += 1
            continue
        seen.add(k)
        out.append(row)
    return out, n


def write_tab(wb, base_name, table, columns, widths, bold, green, nt_col):
    """Write `table` across '<name>', '<name> 2', ... under the Excel row cap.

    nt_col is the 1-based column holding new_type, so the green
    "posted since last run" rule lands on the right column in every tab.
    """
    last = get_column_letter(len(columns))
    nt = get_column_letter(nt_col)
    freeze = f"{get_column_letter(len(columns) - len(ajb.COLUMNS) + 1)}2"
    chunks = [table[i:i + XLSX_CAP]
              for i in range(0, len(table), XLSX_CAP)] or [[]]
    for n, chunk in enumerate(chunks, 1):
        name = base_name if n == 1 else f"{base_name} {n}"
        ws = wb.create_sheet(name)
        for j, wd in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(j)].width = wd
        ws.freeze_panes = freeze
        ws.auto_filter.ref = f"A1:{last}{len(chunk) + 1}"
        ws.conditional_formatting.add(
            f"A2:{last}{len(chunk) + 1}",
            FormulaRule(formula=[f'${nt}2="Posted since last run"'],
                        fill=green, stopIfTrue=False))
        ws.append(ajb.hdr(ws, columns, bold))
        for row in chunk:
            ws.append([ajb.safe(v) for v in row])
    if len(chunks) > 1:
        print(f"{base_name}: split across {len(chunks)} tabs (Excel row cap)",
              flush=True)
    return chunks


def main():
    csv.field_size_limit(10 ** 9)

    if not os.path.exists(BASELINE):
        sys.exit(f"baseline key snapshot missing: {BASELINE}")
    print(f"loading baseline keys from {os.path.basename(BASELINE)} ...",
          flush=True)
    with open(BASELINE, encoding="utf-8") as f:
        seen = {json.loads(ln) for ln in f if ln.strip()}
    seen.discard("")
    print(f"  {len(seen):,} baseline job_keys", flush=True)

    # ---- stream the refreshed master, keep only unseen keys
    # The same pass also counts openings per company across the WHOLE master —
    # that is the input to the startup bulk-poster cap. startup_build gets it
    # from pass1(), which costs a second 1.1 GB read; here it is free.
    kept, total, nokey = [], 0, 0
    co_all = {}
    with open(MASTER, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        hdr_row = next(r)
        if hdr_row != ajb.COLUMNS:
            sys.exit(f"master schema changed; expected {len(ajb.COLUMNS)} "
                     f"cols, got {len(hdr_row)}")
        ki = IDX["job_key"]
        ci = IDX["company"]
        for row in r:
            total += 1
            if len(row) <= ki:
                continue
            co = (row[ci] or "").strip().lower()
            if co:
                co_all[co] = co_all.get(co, 0) + 1
            k = row[ki]
            if not k:
                nokey += 1
                continue
            if k not in seen:
                kept.append(row)
    print(f"master: {total:,} rows, {len(kept):,} new, "
          f"{nokey:,} with no key", flush=True)

    # A diff this large would mean the key stopped being stable, not that the
    # world posted a million jobs overnight.  Say so rather than shipping it
    # silently as "new".
    if total and len(kept) / total > 0.60:
        print(f"WARNING: {len(kept) / total:.0%} of the master is 'new' — "
              f"that points at job_key instability, not new hiring. "
              f"Check before trusting this sheet.", flush=True)

    # ---- classify
    pi = IDX["posted"]
    n_new, n_disc, n_nodate = 0, 0, 0
    out = []
    for row in kept:
        p = (row[pi] or "").strip()
        if not p:
            t = "Newly discovered (no date)"
            n_nodate += 1
        elif p >= SINCE:
            t = "Posted since last run"
            n_new += 1
        else:
            t = "Newly discovered"
            n_disc += 1
        out.append([t] + row[:len(ajb.COLUMNS)])

    # Successive stable sorts, least-significant first: the LAST sort is the
    # primary one.  Final order = new_type, then newest posted, then company,
    # then title.
    ci, ti = IDX["company"] + 1, IDX["title"] + 1
    out.sort(key=lambda r: (r[ti] or "").lower())
    out.sort(key=lambda r: (r[ci] or "").lower())
    out.sort(key=lambda r: (r[pi + 1] or ""), reverse=True)
    rank = {"Posted since last run": 0, "Newly discovered": 1,
            "Newly discovered (no date)": 2}
    out.sort(key=lambda r: rank[r[0]])

    print(f"  {n_new:,} posted since {SINCE} | {n_disc:,} newly discovered "
          f"| {n_nodate:,} no date", flush=True)

    # ---- derived slice 1: US-remote + Massachusetts
    geo = []           # (bucket_rank, -posted, [ctx] + master row)
    for row in out:
        g = geo_of(row)
        if not g:
            continue
        bucket, basis = g
        geo.append((BUCKET_RANK[bucket], row[pi + 1] or "",
                    [bucket, basis, row[0]] + row[1:]))
    geo.sort(key=lambda t: t[1], reverse=True)      # newest first
    geo.sort(key=lambda t: t[0])                    # MA local before Remote
    geo_table, n_geo_dup = collapse_dups([t[2] for t in geo], len(GEO_CTX))
    n_ma = sum(1 for r in geo_table if r[0] == "MA local")
    print(f"  US Remote + MA: {len(geo_table):,} "
          f"({n_ma:,} MA, {len(geo_table) - n_ma:,} US remote; "
          f"{n_geo_dup:,} duplicate company+title collapsed)", flush=True)

    # ---- derived slice 2: startup AI, inside that same geo slice
    verdicts = sb.load()
    if verdicts is None:
        print("WARNING: startup_boards.csv missing — Tier 2 (VC-backed) "
              "cannot be assigned. Run startup_boards.py first.", flush=True)
        verdicts = {}
    else:
        print(f"  {len(verdicts):,} board verdicts loaded", flush=True)

    ai = []            # (band_n, tier, -posted, [ctx] + master row)
    for bucket_rank, posted, grow in geo:
        row = [grow[2]] + grow[3:]          # back to [new_type] + master
        a = ai_of(row)
        if not a:
            continue
        st = startup_tier_of(row, verdicts, co_all)
        if not st:
            continue
        band_n, band_label, ai_tier = a
        tier, evidence = st
        ai.append((band_n, tier, posted,
                   [band_label, ai_tier, tier, evidence, grow[0], grow[2]]
                   + grow[3:]))
    ai.sort(key=lambda t: t[2], reverse=True)       # newest first
    ai.sort(key=lambda t: (t[0], t[1]))             # band, then startup tier
    # Deduped independently of the geo tab, not derived from its survivors: two
    # copies of one job can carry different startup evidence (the Getro copy
    # has the funding round, the ATS copy has none), so each tab must keep its
    # own best-ranked representative.
    ai_table, n_ai_dup = collapse_dups([t[3] for t in ai], len(AI_CTX))
    print(f"  Startup AI: {len(ai_table):,} "
          f"({n_ai_dup:,} duplicate company+title collapsed)", flush=True)
    n_off = sum(1 for row in out if row[IDX["source"] + 1] not in REFRESHED)
    print(f"  {n_off:,} credit a source that was not re-fetched "
          f"(re-keying artifact, not new postings)", flush=True)

    # ---- summaries
    def tally(fn):
        d = {}
        for row in out:
            k = fn(row) or "(none)"
            d[k] = d.get(k, 0) + 1
        return d

    by_source = tally(lambda r: r[IDX["source"] + 1])
    by_type = tally(lambda r: r[0])
    by_wm = tally(lambda r: r[IDX["work_mode"] + 1])
    by_date = tally(lambda r: r[IDX["posted"] + 1])
    by_co = tally(lambda r: r[IDX["company"] + 1])
    by_feed = {}
    for row in out:
        for s in (row[IDX["sources"] + 1] or "").split(";"):
            s = s.strip()
            if s:
                by_feed[s] = by_feed.get(s, 0) + 1
    n_remote = sum(1 for row in out if row[IDX["remote"] + 1] == "True")

    def tally_of(table, fn):
        d = {}
        for row in table:
            k = fn(row) or "(none)"
            d[k] = d.get(k, 0) + 1
        return d

    # Offsets: geo rows are GEO_CTX + master, ai rows are AI_CTX + master.
    g_off, a_off = len(GEO_CTX), len(AI_CTX)
    by_basis = tally_of(geo_table, lambda r: r[1])
    by_geo_co = tally_of(geo_table, lambda r: r[IDX["company"] + g_off])
    by_band = tally_of(ai_table, lambda r: r[0])
    by_atier = tally_of(ai_table, lambda r: r[1])
    by_stier = tally_of(ai_table, lambda r: r[2])
    by_ai_co = tally_of(ai_table, lambda r: r[IDX["company"] + a_off])

    # Rows crediting a source that was NOT re-fetched cannot be new postings:
    # that source's JSONL never changed.  They are a re-keying artifact —
    # all_jobs_build gives row identity to the FIRST loader in ingest order, so
    # when a Getro-hosted duplicate of (say) a Workday job disappears, the
    # surviving Workday copy re-keys from a tier-2 (company,title,url) key to a
    # canonical ATS key.  New job_key, same underlying job.  Quantified here
    # rather than silently left in the count.
    n_offsource = sum(1 for row in out
                      if row[IDX["source"] + 1] not in REFRESHED)

    # ---- CSV
    out_csv = os.path.join(BASE, f"New_Jobs_{SNAP}.csv")
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
    last_col = get_column_letter(len(COLUMNS))

    chunks = [out[i:i + XLSX_CAP] for i in range(0, len(out), XLSX_CAP)] \
        or [[]]
    for n, chunk in enumerate(chunks, 1):
        name = "New Jobs" if n == 1 else f"New Jobs {n}"
        ws = wb.create_sheet(name)
        for j, wd in enumerate(WIDTHS, 1):
            ws.column_dimensions[get_column_letter(j)].width = wd
        ws.freeze_panes = "B2"
        ws.auto_filter.ref = f"A1:{last_col}{len(chunk) + 1}"
        # green = genuinely new hiring, not merely newly discovered
        ws.conditional_formatting.add(
            f"A2:{last_col}{len(chunk) + 1}",
            FormulaRule(formula=['$A2="Posted since last run"'], fill=green,
                        stopIfTrue=False))
        ws.append(ajb.hdr(ws, COLUMNS, bold))
        for row in chunk:
            ws.append([ajb.safe(v) for v in row])
    if len(chunks) > 1:
        print(f"split across {len(chunks)} tabs (Excel row cap)", flush=True)

    # new_type sits in column 3 on the geo tab and column 6 on the AI tab —
    # write_tab anchors the green rule there rather than assuming column A.
    write_tab(wb, "US Remote + MA", geo_table, GEO_COLUMNS, GEO_WIDTHS,
              bold, green, len(GEO_CTX))
    write_tab(wb, "Startup AI", ai_table, AI_COLUMNS, AI_WIDTHS,
              bold, green, len(AI_CTX))

    ws = wb.create_sheet("Summary")
    ws.column_dimensions["A"].width = 46
    ws.column_dimensions["B"].width = 16

    def sec(title, pairs, limit=None):
        ws.append([])
        c = ajb.WriteOnlyCell(ws, value=title)
        c.font = bold
        ws.append([c])
        items = sorted(pairs.items(), key=lambda kv: -kv[1])
        for k, v in (items[:limit] if limit else items):
            ws.append([ajb.safe(k), v])

    c = ajb.WriteOnlyCell(ws, value="NEW JOBS — added since the last run")
    c.font = bold
    ws.append([c])
    ws.append(["Snapshot (UTC)", SNAP])
    ws.append(["Compared against", os.path.basename(BASELINE)])
    ws.append(["Baseline job_keys", len(seen)])
    ws.append(["Master rows this run", total])
    ws.append(["NEW jobs", len(out)])
    ws.append(["  remote", n_remote])
    ws.append([])
    ws.append(["Sources re-fetched this run", ", ".join(sorted(REFRESHED))])
    ws.append(["Sources NOT re-fetched",
               "everything else — they keep last run's data on disk, so they "
               "cannot contribute genuinely new postings"])
    ws.append([])
    c = ajb.WriteOnlyCell(ws, value="Read the counts this way")
    c.font = bold
    ws.append([c])
    ws.append(["Posted since last run",
               "real posted date on/after the previous snapshot — genuinely "
               "new hiring. Highlighted green on the New Jobs tab."])
    ws.append(["Newly discovered",
               "older posted date: the job existed before, we had not seen "
               "it. New/recovered boards, not new hiring."])
    ws.append(["Newly discovered (no date)",
               "source never publishes dates; blank means unknown, not old."])
    ws.append(["Re-keying artifact (approx.)", n_offsource])
    ws.append(["  what it is",
               "rows crediting a source that was NOT re-fetched, so they "
               "cannot be new postings. all_jobs_build gives row identity to "
               "the first loader in ingest order; when a Getro-hosted "
               "duplicate disappears, the surviving copy re-keys and looks "
               "new. Filter the source column to the re-fetched five to "
               "exclude them."])

    sec("By new_type", by_type)
    sec("By posted date (top 40)", by_date, 40)
    sec("By source (first loader to emit the row)", by_source)
    sec("By feed (counts a job once per feed carrying it)", by_feed)
    sec("By work mode", by_wm)
    sec("Top 60 companies", by_co, 60)

    # ---- the two derived tabs
    ws.append([])
    c = ajb.WriteOnlyCell(ws, value="US REMOTE + MA tab")
    c.font = bold
    ws.append([c])
    ws.append(["Rows", len(geo_table)])
    ws.append(["  MA (on-site, hybrid or remote-in-MA)", n_ma])
    ws.append(["  US remote (outside MA)", len(geo_table) - n_ma])
    ws.append(["  duplicate company+title collapsed", n_geo_dup])
    ws.append(["How the US test works",
               "remote_us_classify.us_basis — country, then state, then "
               "unambiguous US wording in the location. The us_basis column "
               "shows which one fired. Case-sensitive on state abbreviations, "
               "so ', ON' and ', BC' do not read as US."])
    ws.append(["Scope note",
               "Massachusetts only. regions_of can also return RI, South FL "
               "and statewide-FL; those are excluded from this cut."])
    sec("US Remote + MA — by bucket",
        {"MA local": n_ma, "Remote": len(geo_table) - n_ma})
    sec("US Remote + MA — by us_basis", by_basis)
    sec("US Remote + MA — top 40 companies", by_geo_co, 40)

    ws.append([])
    c = ajb.WriteOnlyCell(ws, value="STARTUP AI tab")
    c.font = bold
    ws.append([c])
    ws.append(["Rows", len(ai_table)])
    ws.append(["  duplicate company+title collapsed", n_ai_dup])
    ws.append(["What it is",
               "the US Remote + MA slice narrowed to rows that carry BOTH a "
               "startup tier and an AI tier."])
    ws.append(["Dedupe note",
               "both action tabs collapse repeat company+title pairs, keeping "
               "the best-ranked copy. One job reached by two sources is two "
               "job_keys and two rows on the New Jobs tab, but one job to "
               "look at here."])
    ws.append(["Board verdicts loaded", len(verdicts)])
    ws.append(["ai_band scale",
               "1 is strongest: Claude/agentic/FDE, then AI engineer/LLM/"
               "GenAI, then other core AI, then AI-ML adjacent, then AI "
               "tooling matched in the skills column only."])
    ws.append(["Startup gates applied",
               "enterprise ATS + aggregators excluded; IPO/acquired/PE "
               "excluded; 1000+ headcount excluded; rows whose only known "
               "board is junk/ecosystem excluded; bulk posters capped at "
               "min(200, max(25, 4 x headcount)) openings master-wide; "
               "expert-network and talent-pool titles excluded."])
    ws.append(["'Pre Seed' caveat",
               "Getro's junk default, not a round — it only reaches Tier 1 "
               "with <=500 staff AND a VC-portfolio board."])
    sec("Startup AI — by ai_band", by_band)
    sec("Startup AI — by startup tier", by_stier)
    sec("Startup AI — by ai_tier", by_atier)
    sec("Startup AI — top 40 companies", by_ai_co, 40)

    out_xlsx = os.path.join(BASE, f"New_Jobs_{SNAP}.xlsx")
    wb.save(out_xlsx)
    print(f"wrote {out_xlsx}", flush=True)

    # CSV sidecars: the two derived cuts are the ones people actually filter
    # and re-sort, and a 30-column CSV opens in anything.
    for fname, cols, table in (
            (f"New_Jobs_US_Remote_MA_{SNAP}.csv", GEO_COLUMNS, geo_table),
            (f"New_Jobs_Startup_AI_{SNAP}.csv", AI_COLUMNS, ai_table)):
        p = os.path.join(BASE, fname)
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(table)
        print(f"wrote {p} ({len(table):,} rows)", flush=True)


if __name__ == "__main__":
    main()
