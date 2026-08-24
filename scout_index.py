#!/usr/bin/env python3
"""corpus.db -> scout_index.npz : the matrix the server re-ranks in memory.

THIS IS THE RESCORE.  Not of one ranking -- of every ranking.  The matrix holds
what is TRUE about each row (score_features.py); a weight profile turns that
into an order at query time, so changing a slider re-ranks the whole corpus with
one matrix-vector product instead of a rebuild.  Nothing here knows any weight.

WHY NOT JUST READ corpus.db.  Pulling ~875k x 64 integers out of SQLite through
the Python driver is 56M round-tripped values, tens of seconds every time the
server starts.  The same numbers as a contiguous int16 array load in well under
a second, and one matrix-vector product over them is ~10-30ms -- the difference
between a slider that responds and a slider that does not.

NO STRING ARRAYS.  numpy pads unicode arrays to the longest element, so keeping
875k job_keys in the npz would cost hundreds of MB for columns only ~60 rows
per fetch ever need.  Instead the matrix row order IS corpus rowid order, so a
row index is a database key; ct_key and company are carried as stable 64-bit
digests, which is all a mask or a per-company cap needs.  Digests are blake2b,
not hash(), because hash() is randomised per process and this file outlives the
process that wrote it -- the same PYTHONHASHSEED trap that made level
print a different `why` on every run.
"""
import argparse
import datetime
import hashlib
import os
import sqlite3
import sys

import numpy as np

import corpus_load as CL
import score_features as F

HERE = os.path.dirname(os.path.abspath(__file__))
NPZ = os.path.join(HERE, "scout_index.npz")


def h64(s):
    """Stable signed 64-bit digest of a string.  '' -> 0, so a blank ct_key
    (962 rows in triage.db have one, from a blank company) can never collide
    with a real key and can never be matched by a decision."""
    if not s:
        return 0
    return int.from_bytes(
        hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest(),
        "big", signed=True)


def build(dbpath=CL.DB, out=NPZ, progress=200000):
    con = sqlite3.connect(dbpath)
    n = con.execute("SELECT COUNT(*) FROM corpus").fetchone()[0]
    print(f"reading {n:,} rows x {len(F.INDEXED)} features", flush=True)

    feat = np.zeros((n, len(F.INDEXED)), dtype=np.int16)
    coverage = np.zeros(n, dtype=np.int8)
    family = np.zeros(n, dtype=np.int8)
    dup = np.zeros(n, dtype=np.int16)
    ct_h = np.zeros(n, dtype=np.int64)
    co_h = np.zeros(n, dtype=np.int64)
    rowid = np.zeros(n, dtype=np.int32)

    cols = ",".join(f'f."{c}"' for c in F.INDEXED)
    q = (f"SELECT c.rowid, c.ct_key, c.company_norm, c.dup_rank, "
         f"f.coverage, f.family, {cols} "
         f"FROM corpus c JOIN corpus_feat f ON f.job_key = c.job_key "
         f"ORDER BY c.rowid")
    i = 0
    for rec in con.execute(q):
        rowid[i] = rec[0]
        ct_h[i] = h64(rec[1] or "")
        co_h[i] = h64(rec[2] or "")
        dup[i] = min(rec[3] or 0, 32767)
        coverage[i] = rec[4] or 0
        family[i] = rec[5] or 0
        feat[i] = rec[6:]
        i += 1
        if progress and i % progress == 0:
            print(f"  .. {i:,}", flush=True)
    con.close()
    if i != n:
        feat, coverage, family = feat[:i], coverage[:i], family[:i]
        dup, ct_h, co_h, rowid = dup[:i], ct_h[:i], co_h[:i], rowid[:i]

    np.savez(out, feat=feat, coverage=coverage, family=family, dup_rank=dup,
             ct_hash=ct_h, co_hash=co_h, rowid=rowid,
             names=np.array(F.INDEXED),
             built=np.array([datetime.date.today().isoformat()]))
    return i, os.path.getsize(out)


def load(path=NPZ):
    """Load the index and refuse a stale one.

    A feature appended to SCHEMA shifts every column after it.  Silently scoring
    the old matrix with the new weight vector would not error -- it would just
    rank on the wrong numbers -- so the schema is stored and compared.
    """
    z = np.load(path, allow_pickle=False)
    names = [str(x) for x in z["names"]]
    if names != F.INDEXED:
        raise SystemExit(
            f"{os.path.basename(path)} is stale: {len(names)} features, schema "
            f"has {len(F.INDEXED)}. Re-run scout_index.py.")
    return z


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=CL.DB)
    ap.add_argument("--out", default=NPZ)
    a = ap.parse_args()
    if not os.path.exists(a.db):
        sys.exit(f"no {a.db} -- run corpus_load.py first")
    t0 = datetime.datetime.now()
    n, size = build(a.db, a.out)
    dt = (datetime.datetime.now() - t0).total_seconds()
    print(f"\nwrote {os.path.basename(a.out)}: "
          f"{n:,} x {len(F.INDEXED)} int16, {size / 1e6:,.0f} MB, {dt:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
