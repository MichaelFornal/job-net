#!/usr/bin/env python3
"""lanes_build.py — assign every main-feed candidate to ONE lane, and tag it.

    python3 lanes_build.py --dry-run --show-killed    # read the kill list FIRST
    python3 lanes_build.py                            # assign and write
    python3 lanes_build.py --explain <job_key>        # why did this row land there

This is the only thing that writes `lane_tags` on the main feed. Everything else reads.

Why one writer
--------------
The twelve main-feed lanes — `picks` plus the eleven F-swaths — share ONE `lane_tags`
column and ONE `score` column on `jobs`. `vocab.py` clears with

    UPDATE jobs SET lane_tags='', score=0, score_why='' WHERE feed='main' AND lane_tags LIKE '%,picks,%'

which blanks the whole column, not just its own tag. Twelve independent builders would each
do that to the others' rows, and every overlapping row would end up carrying whichever tag
was written last with whichever score belonged to a different lane. So there is one pass:
score each row against all twelve, give it to its single best-scoring lane, write one tag and
one score.

Disjointness is not tidiness, it is correctness
-----------------------------------------------
`lanes.py` documents the invariant: a job is one row with one `job_key`, and deciding it in
any lane clears it everywhere. If a row carried two lane tags it would be shown twice, and
two verdicts about the same job is exactly the failure the two contract lanes and `picks`
were designed to avoid. One row, one lane, one verdict.

Inherited operational trap
--------------------------
`triage_load.py --feed main` upserts `lane_tags`/`score`/`score_why` from a CSV that has
none of those columns, so **every main load silently empties all twelve lanes.** This script
is the one command that restores them. It belongs in the full-refresh runbook after
`triage_load.py`, and `triage/selftest.py` §7 asserts the round trip.
"""
import argparse
import datetime as dt
import os
import sqlite3
import sys
from collections import Counter

BASE = os.path.dirname(os.path.abspath(__file__))
if BASE not in sys.path:
    sys.path.insert(0, BASE)

import swath_vocab as V                                        # noqa: E402
import swath_build as S                                        # noqa: E402
import vocab as P                                        # noqa: E402

DB = os.path.join(BASE, "triage.db")

# Lane priority for exact score ties, and the order the report prints in. `picks` leads
# because it is the established lane and its vocabulary is the profile's own target_titles;
# a row that is genuinely both a `picks` row and a swath row belongs in `picks`.
LANE_ORDER = ["picks"] + list(V.SWATHS)

# Every tag this script owns. The clear is scoped to these AND to feed='main', exactly as
# `triage_load.retire()` scopes its own: an unscoped wipe would strip `,contract,` and
# `,fastmoney,` off the contract pool and silently empty two lanes this file knows nothing
# about.
OWNED_TAGS = LANE_ORDER


def label_of(key):
    return "Picks · worth applying to" if key == "picks" else V.label_of(key)


def score_for_all(r, today):
    """{lane_key: (score, why)} — every lane this row is a candidate for.

    A row is a candidate for `picks` only if it clears picks' own title gate, and for a
    swath only if that swath's title pattern matches. Neither is a domain-only ticket in:
    that is the defect (a desk noun scoring as a job title) that put a Legal Counsel and a
    Summer Analyst at the top of `picks`.
    """
    out = {}
    title = r["title"] or ""

    dom = bool(P.DOMAIN.search(f"{title} {r['skills'] or ''} "
                               f"{r['company_industry'] or ''}"))
    tpts, _ = P.title_fit(title, dom)
    if tpts:
        out["picks"] = P.score_row(r, today)

    for k in V.all_matching(title):
        out[k] = S.score_row(k, r, today)
    return out


def assign(rows, today, per_company=S.PER_COMPANY, top=0):
    """(assignments, stats). One lane per row, best score wins."""
    best, killed, kill_examples = {}, Counter(), []
    contested = []
    considered = 0

    for r in rows:
        title = r["title"] or ""
        cands = score_for_all(r, today)
        if not cands:
            continue
        considered += 1
        why_kill = P.killed_by(title, r["skills"] or "")
        if why_kill:
            killed[why_kill] += 1
            kill_examples.append((why_kill, r["company"], title))
            continue
        # argmax, ties broken by LANE_ORDER so a rerun on unchanged data is identical
        key = min(cands, key=lambda k: (-cands[k][0], LANE_ORDER.index(k)))
        score, why = cands[key]
        # A tie means two lanes want this row equally and the winner came from LANE_ORDER
        # rather than from the evidence. Measured at 67 rows in ~2,300, most of them a
        # swath beating a sibling swath correctly (RevOps titles going to F1 over F9).
        # The dozen that involve `picks` split both ways — "Forward Deployment Engineer"
        # belongs in `picks`, "AI Enablement Specialist" belongs in F8 — so no blanket
        # rule gets them all right. Reported rather than resolved: a hidden coin-flip is
        # worse than a visible one, and the list is short enough to read.
        tied = [k for k, v in cands.items() if v[0] == score]
        if len(tied) > 1:
            contested.append((r["company"], r["title"], key, sorted(tied)))
        best.setdefault(key, []).append((score, r, why, sorted(cands)))

    stats = {"considered": considered, "killed": killed, "contested": contested,
             "kill_examples": kill_examples, "capped": Counter(), "trimmed": Counter()}

    for key in list(best):
        best[key].sort(key=lambda t: (-t[0], t[1]["posted"] or "", t[1]["job_key"]))
        if per_company:
            seen, capped = Counter(), []
            for row in best[key]:
                co = row[1]["company_norm"] or row[1]["job_key"]
                if seen[co] >= per_company:
                    stats["capped"][key] += 1
                    continue
                seen[co] += 1
                capped.append(row)
            best[key] = capped
        if top and len(best[key]) > top:
            stats["trimmed"][key] = len(best[key]) - top
            best[key] = best[key][:top]
    return best, stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=S.DAYS)
    ap.add_argument("--per-company", type=int, default=S.PER_COMPANY,
                    help="cap rows per company per lane (0 = no cap). Default 3: without "
                         "it Prolific alone is 52%% of the F3 pool.")
    ap.add_argument("--top", type=int, default=0,
                    help="cap rows per lane (0 = no cap, the default). These lanes exist "
                         "to be burned down; a silent cap means rows never surface.")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-killed", action="store_true",
                    help="print what the lab/defense/anti filter excluded — read this "
                         "before trusting the filter")
    ap.add_argument("--explain", help="a job_key: print every lane it scored for")
    args = ap.parse_args()

    today = dt.date.today()
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    rows, cutoff = S.candidates(con, args.days, today)
    print(f"{len(rows):,} undecided, live, employer-direct rows since {cutoff}")

    if args.explain:
        r = con.execute(S.CAND_SQL.replace("AND posted >= ?", "AND job_key = ?"),
                        (args.explain,)).fetchone()
        if not r:
            sys.exit(f"{args.explain} is not a live undecided main-feed row")
        print(f"\n{r['company']} — {r['title']}\n")
        cands = score_for_all(r, today)
        kill = P.killed_by(r["title"] or "", r["skills"] or "")
        if kill:
            print(f"  KILLED: {kill}\n")
        for k in sorted(cands, key=lambda k: -cands[k][0]):
            code = "picks" if k == "picks" else V.code_of(k)
            print(f"  {cands[k][0]:>6.1f}  {code:<6} {cands[k][1]}")
        if cands:
            win = min(cands, key=lambda k: (-cands[k][0], LANE_ORDER.index(k)))
            print(f"\n  -> assigned to {win}")
        return 0

    best, stats = assign(rows, today, args.per_company, args.top)
    print(f"{stats['considered']:,} matched at least one lane; "
          f"{sum(stats['killed'].values()):,} excluded by the lab/defense/anti filter")

    if args.show_killed:
        print("\n--- excluded (verify these are all genuinely wrong) ---")
        for reason, co, title in sorted(stats["kill_examples"])[:80]:
            print(f"  {reason:<30} {(co or '?')[:24]:<24} {title[:52]}")
        print(f"--- {sum(stats['killed'].values())} excluded ---")
        for reason, n in stats["killed"].most_common():
            print(f"  {n:>5}  {reason}")

    print(f"\n{'lane':<11} {'code':<5} {'rows':>6} {'top':>6} {'capped':>7} "
          f"{'busiest employer':<26}")
    print("-" * 78)
    total = 0
    for key in LANE_ORDER:
        rank = best.get(key, [])
        total += len(rank)
        code = "—" if key == "picks" else V.code_of(key)
        co = Counter((r["company"] or "?") for _, r, _, _ in rank).most_common(1)
        busiest = f"{co[0][0][:20]} x{co[0][1]}" if co else "—"
        print(f"{key:<11} {code:<5} {len(rank):>6,} "
              f"{(rank[0][0] if rank else 0):>6.1f} {stats['capped'][key]:>7,} "
              f"{busiest:<26}")
    print(f"{'TOTAL':<11} {'':<5} {total:>6,}")

    if stats["contested"]:
        print(f"\n{len(stats['contested'])} row(s) tied between two lanes and were broken "
              f"by lane order, not by evidence:")
        for co, title, won, tied in stats["contested"][:8]:
            others = ", ".join(t for t in tied if t != won)
            print(f"   -> {won:<11} over {others:<24} {(co or '?')[:18]:<20} "
                  f"{(title or '')[:40]}")

    dropped = sum(stats["capped"].values()) + sum(stats["trimmed"].values())
    if dropped:
        print(f"\n{dropped:,} rows dropped by the per-company cap "
              f"(--per-company 0 to disable). These are overwhelmingly one employer "
              f"fanning one job out per city; NICHE-MAP-2026 §6 has the measurement.")

    if not total:
        sys.exit("no rows assigned — refusing to blank all twelve lanes")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return 0

    like = " OR ".join(["lane_tags LIKE ?"] * len(OWNED_TAGS))
    params = [f"%,{t},%" for t in OWNED_TAGS]
    with con:
        con.execute(f"UPDATE jobs SET lane_tags='', score=0, score_why='' "
                    f"WHERE feed='main' AND ({like})", params)
        con.executemany(
            "UPDATE jobs SET lane_tags=?, score=?, score_why=? WHERE job_key=?",
            [(f",{key},", s, why, r["job_key"])
             for key, rank in best.items() for s, r, why, _ in rank])

    n = con.execute("SELECT COUNT(*) FROM jobs WHERE feed='main' "
                    "AND lane_tags <> ''").fetchone()[0]
    dupes = con.execute(
        "SELECT COUNT(*) FROM jobs WHERE feed='main' "
        "AND LENGTH(lane_tags) - LENGTH(REPLACE(lane_tags, ',', '')) > 2").fetchone()[0]
    print(f"\ntagged {n:,} main-feed rows across {len(best)} lanes")
    print(f"rows carrying more than one tag: {dupes}  "
          f"{'(disjoint ✓)' if dupes == 0 else '*** NOT DISJOINT ***'}")
    print("restart triage/server.py to pick it up")
    return 0 if dupes == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
