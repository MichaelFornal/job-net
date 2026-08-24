#!/usr/bin/env python3
"""swath_build.py — score main-feed rows against the eleven F-swath lanes.

    python3 swath_build.py --dry-run --show-killed      # read the kill list FIRST
    python3 swath_build.py --swath presales --top 40    # inspect one lane
    python3 swath_build.py                              # score all eleven, report only

**This module does not write.** `lanes_build.py` does, because the twelve main-feed lanes
share one `lane_tags` column and one `score` column: eleven independent writers would stomp
each other's tags on every overlapping row. This file supplies the scorer; the orchestrator
decides which single lane each row belongs to. Run this directly to inspect a lane's ranking
without touching the database.

Scoring is `vocab.py`'s model, one lane at a time:

    title fit   40 / 24 / 0     this swath's pattern · a sibling swath's · neither
    domain fit  20 / 10 / 0     employer's product matches the swath · adjacent · no
    company     12 / 9 / 5      startup tier 1 / 2 / 3
    geo         10 / 7 / 4      MA local · proven US remote · inferred US remote
    freshness   10 * e^(-d/5)   a 20-day-old posting is worth a fifth of a same-day one
    comp         8 / 6 / 2      >=150k / >=110k / below

Two defects are carried forward from `picks` as design rules, and `selftest.py` asserts both:

  1. **A domain word is never a job title.** Scoring desk nouns as title fit put a Legal
     Counsel, an Administrative Assistant and a 2027 Summer Analyst at the top of `picks`.
     `swath_vocab` anchors every title pattern to a role word; the desk noun earns its 20
     points here in `domain_fit`, on a row that already looks like the right job.
  2. **Blocklists match titles, not the employer's product.** `skills` describes what the
     company sells, so killing on it drops a full-stack engineer at a biotech and a
     business-development lead at a robotics company. `vocab.killed_by` judges the two
     separately and exempts commercial titles — imported, not restated.

A third defect is specific to these eleven and was measured while building them
(NICHE-MAP-2026 §6): **per-company fan-out**. Prolific publishes 1,873 rows with 1,873
DISTINCT titles because the title carries the city and the language, and it alone is 52% of
the F3 pool. Bank of America was 12% of F2 until an ambiguous role word came out of the
pattern. Hence `--per-company`, defaulting ON at 3 — a lane that is one employer's sitemap
is not a lane.
"""
import argparse
import datetime as dt
import math
import os
import sqlite3
import sys
from collections import Counter

BASE = os.path.dirname(os.path.abspath(__file__))
if BASE not in sys.path:
    sys.path.insert(0, BASE)

import swath_vocab as V                                       # noqa: E402
# Imported, never copied. vocab's filter is the same filter these lanes need, and a
# second transcription is a second thing to keep in sync — the mistake its own header
# documents.
from vocab import (killed_by, company_fit, geo_fit,      # noqa: E402
                         fresh_fit, comp_fit)

DB = os.path.join(BASE, "triage.db")
DAYS = 21
PER_COMPANY = 3

# Rows a lane may consider at all: live, undecided, employer-direct, inside the window.
# Identical in shape to vocab.CAND_SQL — the ct_key clause is what stops a job
# reappearing in a swath lane after it was decided under a different key.
CAND_SQL = """
SELECT job_key, ct_key, company, company_norm, title, skills, company_industry,
       startup_tier, bucket, basis_weak, posted, salary_max, source
  FROM jobs
 WHERE feed = 'main' AND live = 1 AND is_agg = 0
   AND posted >= ?
   AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)
   AND NOT EXISTS (SELECT 1 FROM decisions d2
                    WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '')
"""


def title_fit(key, title):
    """40 for this swath's own pattern, 24 for a sibling's, 0 otherwise.

    The sibling tier exists because the eleven genuinely overlap — a "Solutions Architect,
    Payments" is real, and F2 and F9 should both be able to see it. Disjoint assignment in
    `lanes_build.py` decides which one keeps it; here it only has to be visible to both.
    """
    if V.matches(key, title):
        return 40, "target title"
    if V.all_matching(title):
        return 24, "sibling-swath title"
    return 0, ""


def domain_fit(key, skills, industry):
    """The employer's product, never the title. 20 for this swath, 10 for a sibling."""
    blob = f"{skills or ''} {industry or ''}"
    if V.domain_of(key).search(blob):
        return 20, "domain match"
    for other, pat in V.DOMAINS.items():
        if other != key and pat.search(blob):
            return 10, "adjacent domain"
    return 0, ""


def score_row(key, r, today):
    """(score, why) for one row in one swath. Every point traceable to a phrase in `why`."""
    parts, total = [], 0.0
    for pts, label in (
        title_fit(key, r["title"] or ""),
        domain_fit(key, r["skills"], r["company_industry"]),
        company_fit(r["startup_tier"] or ""),
        geo_fit(r["bucket"] or "", r["basis_weak"]),
        fresh_fit(r["posted"] or "", today),
        comp_fit(r["salary_max"]),
    ):
        if pts:
            total += pts
            parts.append(f"{label} +{pts:g}")
    return round(total, 1), " · ".join(parts)


def candidates(con, days=DAYS, today=None):
    today = today or dt.date.today()
    cutoff = (today - dt.timedelta(days=days)).isoformat()
    return con.execute(CAND_SQL, (cutoff,)).fetchall(), cutoff


def score_all(rows, today=None, per_company=PER_COMPANY):
    """{swath_key: [(score, row, why), ...]} — ranked, kill-filtered, company-capped.

    A row can appear under several swaths here. That is intentional and is what makes
    disjoint assignment in `lanes_build.py` a real choice rather than a first-match race.
    """
    today = today or dt.date.today()
    out, killed = {k: [] for k in V.SWATHS}, Counter()
    kill_examples = []
    for r in rows:
        title = r["title"] or ""
        hits = V.all_matching(title)
        if not hits:
            continue
        why_kill = killed_by(title, r["skills"] or "")
        if why_kill:
            killed[why_kill] += 1
            kill_examples.append((why_kill, r["company"], title))
            continue
        for k in hits:
            s, why = score_row(k, r, today)
            out[k].append((s, r, why))

    for k in out:
        out[k].sort(key=lambda t: (-t[0], t[1]["posted"] or "", t[1]["job_key"]))
        if per_company:
            seen, capped = Counter(), []
            for s, r, why in out[k]:
                co = r["company_norm"] or r["job_key"]
                if seen[co] >= per_company:
                    continue
                seen[co] += 1
                capped.append((s, r, why))
            out[k] = capped
    return out, killed, kill_examples


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=DAYS)
    ap.add_argument("--per-company", type=int, default=PER_COMPANY,
                    help="cap rows per company per lane (0 = no cap). Defaults to 3: "
                         "Prolific alone is 52%% of the F3 pool without it.")
    ap.add_argument("--swath", help="inspect one lane by key "
                                    f"({', '.join(V.SWATHS)})")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--show-killed", action="store_true")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--dry-run", action="store_true",
                    help="accepted for symmetry with vocab; this module never "
                         "writes in any case — lanes_build.py does")
    args = ap.parse_args()

    today = dt.date.today()
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    rows, cutoff = candidates(con, args.days, today)
    print(f"{len(rows):,} undecided, live, employer-direct rows since {cutoff}\n")

    scored, killed, examples = score_all(rows, today, args.per_company)

    if args.show_killed:
        print("--- excluded (verify these are all genuinely wrong) ---")
        for reason, co, title in sorted(examples)[:60]:
            print(f"  {reason:<30} {(co or '?')[:24]:<24} {title[:52]}")
        print(f"--- {sum(killed.values())} excluded ---")
        for reason, n in killed.most_common():
            print(f"  {n:>5}  {reason}")
        print()

    if args.swath:
        k = args.swath
        if k not in V.SWATHS:
            sys.exit(f"no such swath: {k} (have: {', '.join(V.SWATHS)})")
        rank = scored[k]
        print(f"{V.code_of(k)} · {V.label_of(k)} — {len(rank):,} candidates\n")
        for s, r, why in rank[:args.top]:
            print(f"  {s:>5.1f}  {(r['company'] or '?')[:24]:<24} "
                  f"{(r['title'] or '')[:50]:<50} {r['posted'] or ''}")
            print(f"         {why}")
        return 0

    print(f"{'code':<5} {'swath':<38} {'cands':>7} {'top':>7} {'busiest employer':<28}")
    print("-" * 92)
    for k in V.SWATHS:
        rank = scored[k]
        top = rank[0][0] if rank else 0
        co = Counter((r["company"] or "?") for _, r, _ in rank).most_common(1)
        busiest = f"{co[0][0][:22]} x{co[0][1]}" if co else "—"
        print(f"{V.code_of(k):<5} {V.label_of(k)[:38]:<38} {len(rank):>7,} "
              f"{top:>7.1f} {busiest:<28}")
    print(f"\n{sum(killed.values()):,} rows excluded by the lab/defense/anti filter")
    print("nothing written — run lanes_build.py to assign and tag")
    return 0


if __name__ == "__main__":
    sys.exit(main())
