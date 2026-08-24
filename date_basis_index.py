#!/usr/bin/env python3
"""date_basis_index.py — recover, per job, whether its `posted` date was real.

    python3 date_basis_index.py [--out date_basis_index.json]

date_basis.py classifies a date by SOURCE.  That is the whole answer for 59 of
the 67 sources, but eight of them are mixed: they publish a real date when the
board has one and fall back to our crawl date when it does not.  Guessing at
the source level for those would be the same sin this whole change exists to
fix, so they are resolved per ROW here.

The answer already exists.  Every *_posted_ledger.json carries a `precision`
per job id; the pipeline just never carries it forward (`precision` is not a
column in all_jobs_master.csv).  This script re-joins the ledgers to the
normalized JSONLs and writes out the set of apply_urls whose date was
fabricated.  triage_load.py reads it; nothing else changes.

WHY JOIN THROUGH THE JSONL AND NOT THE apply_url
  The ledger key is the fetcher's own (board, job_id), and every one of the
  eight builds its apply_url differently — atstail alone has six shapes
  (atstail_fetch.py,538,619,859,1075,1146).  Reversing a URL back to a job
  id would be eight guesses that break silently the next time a URL template
  changes.  contract_norm/*.jsonl still carries `job_id` and `board` verbatim,
  so this joins on the fetcher's own key and measures the hit rate.

TWO LEDGER SEMANTICS, DO NOT MIX THEM UP
  workday, icims   every job is written to the ledger.  Fabricated is
                   precision == "first_seen".  (workday also has "30plus",
                   which is today-30 — an unbounded floor, not a real date, so
                   it counts as fabricated too.)
  atstail          posted_for() returns a real date WITHOUT touching the
                   ledger and only writes when the date was missing, so
                   PRESENCE IN THE LEDGER IS ITSELF THE ANSWER.  Measured:
                   10,184 of 40,000 sampled rows are in it; the other 29,816
                   had real dates.  Reading this ledger the workday way would
                   mark every one of those 29,816 fabricated.
"""
import json
import os
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
NORM = os.path.join(BASE, "contract_norm")
OUT = os.path.join(BASE, "date_basis_index.json")

# (ledger file, jsonl file, key template, "precision" | "presence")
#
# `presence` sources are listed by the SOURCE name they emit, not by the
# fetcher file: atstail.jsonl carries six different `source` values and the
# ledger key uses that value, not the filename.
LEDGERS = [
    ("workday_posted_ledger.json", "workday.jsonl", "{board}:{job_id}",
     "precision"),
    ("icims_posted_ledger.json", "icims.jsonl", "{board}:{job_id}",
     "precision"),
    ("atstail_posted_ledger.json", "atstail.jsonl", "{source}|{job_id}",
     "presence"),
    # 2026-08-20.  Key template verified against the real files, 400/400
    # sampled rows matching, not assumed: manatal keys on its board slug,
    # radancy on the career HOST, dayforce on "{client}/{site}" -- and all
    # three set row["board"] to exactly that, so one template covers them.
    ("manatal_posted_ledger.json", "manatal.jsonl", "{board}:{job_id}",
     "precision"),
    ("radancy_posted_ledger.json", "radancy.jsonl", "{board}:{job_id}",
     "precision"),
    ("dayforce_posted_ledger.json", "dayforce.jsonl", "{board}:{job_id}",
     "precision"),
]

# A date that is not the employer's.  "30plus" is workday's "Posted 30+ Days
# Ago" -> today-30: the NEWEST date consistent with what the board said, not a
# measurement.  workday_fetch.py calls it "an unbounded floor" and it is
# 244,269 of 718,483 ledger entries, so it cannot be waved through as real.
FABRICATED_PRECISION = {"first_seen", "30plus", "unknown", ""}


def load_ledger(path):
    try:
        with open(path) as f:
            d = json.load(f)
    except FileNotFoundError:
        return None
    return d if isinstance(d, dict) else None


def build(verbose=True):
    fabricated = set()
    stats = {}
    for ledger_name, jsonl_name, keyfmt, mode in LEDGERS:
        led = load_ledger(os.path.join(BASE, ledger_name))
        jsonl = os.path.join(NORM, jsonl_name)
        if led is None or not os.path.exists(jsonl):
            # A pipe that has not run yet must not silently mark its whole
            # source real -- say so and let the caller decide.
            stats[jsonl_name] = {"skipped": "ledger or jsonl missing"}
            if verbose:
                print(f"  {jsonl_name:16s} SKIPPED (missing "
                      f"{'ledger' if led is None else 'jsonl'})")
            continue
        hit = miss = noid = fab = 0
        with open(jsonl) as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                jid = str(r.get("job_id") or "")
                if not jid:
                    noid += 1
                    continue
                key = keyfmt.format(board=r.get("board") or "",
                                    job_id=jid,
                                    source=r.get("source") or "")
                rec = led.get(key)
                if rec is None:
                    miss += 1
                    # `presence` ledgers: absent == the fetcher had a real date.
                    continue
                hit += 1
                if mode == "presence":
                    bad = True
                else:
                    bad = str((rec or {}).get("precision", "")) in \
                        FABRICATED_PRECISION
                if bad:
                    url = (r.get("apply_url") or "").strip()
                    if url:
                        fabricated.add(url)
                        fab += 1
        stats[jsonl_name] = {"rows": hit + miss + noid, "ledger_hit": hit,
                             "ledger_miss": miss, "no_job_id": noid,
                             "fabricated": fab}
        if verbose:
            print(f"  {jsonl_name:16s} rows={hit + miss + noid:>8,}  "
                  f"hit={hit:>8,}  miss={miss:>8,}  no_id={noid:>6,}  "
                  f"fabricated={fab:>8,}")
    return fabricated, stats


def main():
    out = OUT
    if "--out" in sys.argv:
        out = sys.argv[sys.argv.index("--out") + 1]
    print("joining posted-date ledgers to contract_norm/*.jsonl ...")
    fabricated, stats = build()
    payload = {"fabricated_apply_urls": sorted(fabricated), "stats": stats}
    with open(out, "w") as f:
        json.dump(payload, f)
    print(f"\n  -> {os.path.basename(out)}  "
          f"{len(fabricated):,} apply_urls with a fabricated date "
          f"({os.path.getsize(out) / 1e6:.0f} MB)")


if __name__ == "__main__":
    main()
