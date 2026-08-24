#!/usr/bin/env python3
"""all_jobs_master.csv -> US_Corpus_<date>.csv : the wide, gated scout corpus.

WHY A SECOND CORPUS BUILDER.  ma_remote_build.py admits a row only if
geo_of() buckets it MA local / Remote / South FL -- 424,442 of 3,610,444 master
rows, collapsing to 266,280 live.  Everything US but not MA and not remote is
not merely unranked, it is UNSEARCHABLE.  This builder keeps the US gate and
drops the market gate.

THREE GATES, AND NOTHING ELSE.  A gate is a filter with no points attached;
everything softer is a weight in score_profile.py that you can slide to zero.

  1. us_basis(country, state, location)   -- NOT the `country` column, which is
     blank on 1,905,439 of 3,610,444 rows (53%).  remote_us_classify parses the
     free text and is the same gate ma_remote_build's remote leg uses; the
     US_HINT regex that ai_ma_remote/startup_build carry is the broken one
     (`[A-Z]{2}` inside a `(?i)` group makes ", on" (Ontario) read as US).
  2. geo_lane.local_killed_by  -- vocab's kill filter widened with the
     RN/LPN/CNA/barista/cashier vocabulary WRONG_ROLE lacks.  Clinical, lab,
     defense and retail work is not a preference you can slide.
  3. level.rung_src(...)[0] <= --rung-max (default 4).  104 of 239
     applications sent carried a senior-or-above token; Director/VP/C-level is
     not an on-target role, so it is a gate, while Senior (rung 4) stays in and
     is scored down.

WHAT IT KEEPS THAT triage_load DROPS.  city / state / country / board_count /
n_sources / source_type.  triage_load.FIELDS drops all six, and geo_lane.py
exists ONLY because `city` never survives that load and towns have to be
re-read out of free-text `location`.

TWO PASSES, and the second is small.  co_all -- openings per company across the
WHOLE master -- is what startup_of's bulk-poster cap needs, and it is not known
until the master has been read once.  Pass 1 gates and writes a temp file while
counting; pass 2 rewrites it with the six CTX columns.  Reading the 1.6 GB
master twice would cost another three minutes for nothing.
"""
import argparse
import csv
import datetime
import os
import sys

import all_jobs_build as AJB
import level as LV
import geo_lane as LOC
import ma_remote_build as MRB
import startup_boards as SB
from remote_us_classify import us_basis

HERE = os.path.dirname(os.path.abspath(__file__))
MASTER = os.path.join(HERE, "all_jobs_master.csv")
IDX = AJB.IDX
CTX = list(MRB.CTX)                      # bucket, us_basis, startup_tier, ...
COLUMNS = list(AJB.COLUMNS) + CTX        # appended, so master indices stay valid


def out_path(day=None):
    day = day or datetime.date.today().isoformat()
    return os.path.join(HERE, f"US_Corpus_{day}.csv")


def gates(row, rung_max, allow_worldwide, skip_kill=False):
    """('' , basis) if the row is admitted, else (reason, '')."""
    country = row[IDX["country"]]
    state = row[IDX["state"]]
    loc = row[IDX["location"]]
    basis = us_basis(country, state, loc)
    if not basis:
        return "not-us", ""
    if basis == "worldwide-open" and not allow_worldwide:
        return "worldwide", ""
    title = row[IDX["title"]] or ""
    if not skip_kill and LOC.local_killed_by(title, row[IDX["skills"]] or ""):
        return "killed", ""
    if LV.rung_src(title, row[IDX["seniority"]] or "")[0] > rung_max:
        return "over-level", ""
    return "", basis


def bucket_of(row, basis):
    """The market label, for a geo facet.  geo_of() answers this for the three
    markets it knows; everything else is US work outside them, which is the
    whole point of widening the corpus, so it gets a label rather than a drop."""
    g = MRB.geo_of(row)
    if g:
        return g[0]
    return "US remote" if AJB.is_remote(row) else "US other"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="report survival per gate, write nothing")
    ap.add_argument("--rung-max", type=int, default=4)
    ap.add_argument("--no-worldwide", action="store_true",
                    help="drop 'anywhere/worldwide' listings")
    ap.add_argument("--limit", type=int, default=0, help="stop after N master rows")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    csv.field_size_limit(10 ** 9)

    if not os.path.exists(MASTER):
        sys.exit(f"master missing: {MASTER}")
    dest = a.out or out_path()
    tmp = dest + ".pass1"

    verdicts = SB.load()
    if verdicts is None:
        print("WARNING: startup_boards.csv missing -- tier 2 (VC-backed) cannot "
              "be assigned; the `co` bonus axis will be sparser than it should be.")
        verdicts = {}

    size = os.path.getsize(MASTER) / 1e9
    print(f"pass 1: streaming all_jobs_master.csv ({size:.2f} GB), "
          f"rung_max={a.rung_max} worldwide={'no' if a.no_worldwide else 'yes'}",
          flush=True)

    drop = {"short-row": 0, "not-us": 0, "worldwide": 0, "killed": 0,
            "over-level": 0}
    total = kept = 0
    co_all = {}
    fh_in = open(MASTER, newline="", encoding="utf-8", errors="replace")
    rd = csv.reader(fh_in)
    hdr = next(rd)
    AJB.check_header(hdr) if hasattr(AJB, "check_header") else None
    n_cols = len(hdr)
    fh_out = None if a.dry_run else open(tmp, "w", newline="", encoding="utf-8")
    wr = None if a.dry_run else csv.writer(fh_out)

    for row in rd:
        total += 1
        if a.limit and total > a.limit:
            total -= 1
            break
        if len(row) < n_cols:
            drop["short-row"] += 1
            continue
        co = (row[IDX["company"]] or "").strip().lower()
        if co:
            co_all[co] = co_all.get(co, 0) + 1
        why, _ = gates(row, a.rung_max, not a.no_worldwide)
        if why:
            drop[why] += 1
            continue
        kept += 1
        if wr:
            wr.writerow(row[:n_cols])
        if total % 500000 == 0:
            print(f"  .. {total:,} read, {kept:,} kept", flush=True)
    fh_in.close()
    if fh_out:
        fh_out.close()

    surv = total
    print(f"\n  master rows            {total:>10,}")
    for k in ("short-row", "not-us", "worldwide", "killed", "over-level"):
        surv -= drop[k]
        print(f"  - {k:<20s} {drop[k]:>10,}   -> {surv:>10,}")
    print(f"  ADMITTED               {kept:>10,}"
          f"   ({100.0 * kept / max(total, 1):.1f}% of master)")
    if a.dry_run:
        print("\n(dry run -- nothing written)")
        return 0

    print(f"\npass 2: adding {len(CTX)} context columns over {kept:,} rows",
          flush=True)
    n = 0
    with open(tmp, newline="", encoding="utf-8") as fin, \
            open(dest, "w", newline="", encoding="utf-8") as fout:
        wr = csv.writer(fout)
        wr.writerow(COLUMNS)
        for row in csv.reader(fin):
            basis = us_basis(row[IDX["country"]], row[IDX["state"]],
                             row[IDX["location"]])
            st, ev = MRB.startup_of(row, verdicts, co_all)
            band, tier = MRB.ai_of(row)      # (ai_band label, ai_tier label)
            wr.writerow(row + [bucket_of(row, basis), basis, st, ev,
                               band, tier])
            n += 1
            if n % 200000 == 0:
                print(f"  .. {n:,}", flush=True)
    os.remove(tmp)
    mb = os.path.getsize(dest) / 1e6
    print(f"\nwrote {os.path.basename(dest)}: {n:,} rows, {mb:,.0f} MB, "
          f"{len(COLUMNS)} columns")
    return 0


if __name__ == "__main__":
    sys.exit(main())
