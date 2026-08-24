#!/usr/bin/env python3
"""demo_corpus.py — build a small scout corpus from the synthetic fixture.

The scout half of the app (the weights panel, FTS search, the explore slice)
needs corpus.db and scout_index.npz.  Both are outputs of a multi-hour pipeline
run, so on a fresh clone `server.py` degrades those routes to a message and the
~28 SQL lanes carry on without them -- correct behaviour, but it means the most
interesting screen in the app is the one a reader cannot see.

This reuses the same path the real build takes -- corpus_load.build() then
scout_index.build() -- on fixture rows instead of the master CSV, so the demo
exercises the real code rather than a mock of it.

    python3 tools/demo_corpus.py [--rows N]
"""
import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import corpus_build as CB      # noqa: E402  -- the corpus column contract
import corpus_load as CL       # noqa: E402
import scout_index as SI       # noqa: E402
from triage import fixture as FX  # noqa: E402

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rows", type=int, default=3000)
    a = ap.parse_args()

    src = {c: i for i, c in enumerate(FX.HDR)}
    out = os.path.join(HERE, "fixtures", "US_Corpus_demo.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)

    n = 0
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(CB.COLUMNS)
        for r in FX.rows(a.rows):
            d = {c: "" for c in CB.COLUMNS}
            for c in CB.COLUMNS:
                if c in src:
                    d[c] = r[src[c]]
            # The corpus gate wants an explicit country; the triage feed carries
            # the bucket instead, and every fixture row is domestic by
            # construction.
            d["country"] = "US"
            d["remote"] = "True" if r[src["remote"]] == "true" else ""
            w.writerow([d[c] for c in CB.COLUMNS])
            n += 1

    db = os.path.join(HERE, "corpus.db")
    kept = CL.build(out, db, progress=0)
    print(f"corpus: {kept:,} of {n:,} rows survived the gate -> {db}")

    npz = os.path.join(HERE, "scout_index.npz")
    SI.build(db, npz, progress=0)
    print(f"index : {npz}")
    print("\nthe scout routes will now answer; restart the server to pick them up")
    return 0


if __name__ == "__main__":
    sys.exit(main())
