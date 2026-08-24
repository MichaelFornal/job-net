#!/usr/bin/env python3
"""
extract_baseline_keys.py — snapshot the job_key column of all_jobs_master.csv
so the NEXT run can diff against it.

  python3 extract_baseline_keys.py [--master FILE] [--out FILE]

RUN THIS BEFORE ANY REBUILD.  all_jobs_build.py opens all_jobs_master.csv with
"w"; there is no .prev copy and no rotation anywhere in the pipeline, so the
moment a refresh reaches its build tail the previous snapshot's key set is gone
and new_jobs_build.py has nothing to diff against.

Output format is JSON-escaped, one key per line — the format new_jobs_build.py
reads with `json.loads(ln)`.  A plain `key\\n` file is silently WRONG: tier-3
keys (company|title|url, used when a row has no parseable ATS URL) carry
newlines straight out of the source title — Freelancer.com, Arbeitsagentur and
RemoteOK all publish multi-line titles.  Those keys split across lines, never
match, and report themselves as new on every subsequent run.

The self-check below is what catches that class of bug: a baseline file can
never hold more lines than the master holds rows.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone

import all_jobs_build as ajb

BASE = os.path.dirname(os.path.abspath(__file__))
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _argval(flag, default):
    if flag in sys.argv:
        j = sys.argv.index(flag)
        if j + 1 < len(sys.argv):
            return sys.argv[j + 1]
    return default


def main():
    csv.field_size_limit(10 ** 9)

    master = _argval("--master", os.path.join(BASE, "all_jobs_master.csv"))
    out = _argval("--out", os.path.join(BASE, f"baseline_job_keys_{SNAP}.jsonl"))

    if not os.path.exists(master):
        sys.exit(f"master missing: {master}")
    if os.path.abspath(out) == os.path.abspath(master):
        sys.exit("refusing to write the baseline over the master")

    print(f"reading {os.path.basename(master)} "
          f"({os.path.getsize(master) / 1e9:.2f} GB) ...", flush=True)

    ki = ajb.IDX["job_key"]
    rows = written = blank = short = 0
    seen = set()

    tmp = out + ".tmp"
    with open(master, newline="", encoding="utf-8") as f, \
            open(tmp, "w", encoding="utf-8") as w:
        r = csv.reader(f)
        hdr_row = next(r)
        # A schema drift here would silently snapshot the wrong column.
        try:
            ajb.check_header(hdr_row, master)
        except SystemExit:
            os.unlink(tmp)
            raise
        for row in r:
            rows += 1
            if len(row) <= ki:
                short += 1
                continue
            k = row[ki]
            if not k:
                blank += 1
                continue
            seen.add(k)
            w.write(json.dumps(k) + "\n")
            written += 1
            if rows % 500_000 == 0:
                print(f"  {rows:,} rows ...", flush=True)
    os.replace(tmp, out)

    lines = sum(1 for _ in open(out, encoding="utf-8"))

    print(f"master rows        {rows:,}", flush=True)
    print(f"blank job_key      {blank:,}", flush=True)
    print(f"short rows         {short:,}", flush=True)
    print(f"keys written       {written:,}", flush=True)
    print(f"distinct keys      {len(seen):,}", flush=True)
    print(f"lines in file      {lines:,}", flush=True)
    print(f"wrote {out} ({os.path.getsize(out) / 1e6:.0f} MB)", flush=True)

    # The newline-in-key detector.  json.dumps escapes embedded newlines to \n,
    # so one key is always exactly one line; if the file has more lines than the
    # master has rows, the escaping did not happen and the baseline is unusable.
    if lines > rows:
        sys.exit(f"FAIL: {lines:,} lines > {rows:,} master rows — keys are not "
                 f"JSON-escaped; this baseline would report split keys as new "
                 f"on every run")
    if lines != written:
        sys.exit(f"FAIL: wrote {written:,} keys but file has {lines:,} lines")
    print("self-check OK: one line per key, JSON-escaped", flush=True)


if __name__ == "__main__":
    main()
