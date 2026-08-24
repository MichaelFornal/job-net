#!/usr/bin/env python3
"""
report_source_mix.py — what is actually IN all_jobs_master.csv, by source.

The yardstick from the job-source-mix-and-yield memory: a source is worth
keeping for how many jobs it contributes that survive cross-source dedupe, not
for how many rows its API advertises.  all_jobs_build.py already collapses
duplicates onto one row carrying a `sources` list, so this reads the finished
master rather than re-deriving anything:

  rows_sole   rows where this source is the ONLY source  -> its net-new
  rows_any    rows where it appears at all               -> its raw reach
  remote      of rows_sole, how many are remote

Usage: python3 report_source_mix.py [all_jobs_master.csv]
"""
import csv
import os
import sys
from collections import Counter

BASE = os.path.dirname(os.path.abspath(__file__))


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        BASE, "all_jobs_master.csv")
    if not os.path.exists(path):
        raise SystemExit(f"missing {path}")
    csv.field_size_limit(1 << 24)

    sole, any_, remote_sole, total = Counter(), Counter(), Counter(), 0
    remote_total = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        rd = csv.reader(f)
        head = next(rd)
        i_src = head.index("sources") if "sources" in head else None
        i_one = head.index("source")
        i_rem = head.index("remote") if "remote" in head else None
        for row in rd:
            total += 1
            is_remote = bool(i_rem is not None and i_rem < len(row)
                             and row[i_rem].strip())
            remote_total += is_remote
            srcs = []
            if i_src is not None and i_src < len(row) and row[i_src].strip():
                srcs = [s.strip() for s in row[i_src].split(",") if s.strip()]
            if not srcs and i_one < len(row):
                srcs = [row[i_one].strip()]
            for s in set(srcs):
                any_[s] += 1
            if len(set(srcs)) == 1:
                s = srcs[0]
                sole[s] += 1
                if is_remote:
                    remote_sole[s] += 1

    print(f"{os.path.basename(path)}: {total:,} rows, "
          f"{remote_total:,} remote ({remote_total / max(total, 1):.1%})\n")
    print(f"{'source':24}{'rows_sole':>11}{'rows_any':>10}"
          f"{'remote_sole':>12}{'%_of_master':>12}")
    for s, n in sole.most_common():
        print(f"{s[:23]:24}{n:11,}{any_[s]:10,}{remote_sole[s]:12,}"
              f"{n / max(total, 1):11.1%}")
    multi = total - sum(sole.values())
    print(f"\n{'(multi-source rows)':24}{multi:11,}   "
          f"— held by 2+ sources, i.e. successfully deduped across pipes")


if __name__ == "__main__":
    main()
