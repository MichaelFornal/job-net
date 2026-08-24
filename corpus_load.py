#!/usr/bin/env python3
"""US_Corpus_<date>.csv -> corpus.db : rows, features, and an FTS5 index.

A SEPARATE FILE ON PURPOSE.  triage.db holds `jobs`, `decisions`,
`applications` and everything thirty lanes rank on.  Widening the corpus inside
it would either reshape every existing lane's counts or add a second feed whose
rows are cross-feed twins of the first -- a problem already measured at 46
ct_keys with two live decision rows and documented as growing.  Keeping the
scout corpus in its own file makes "alongside" a fact: triage.db is not opened
for writing by anything here, and corpus.db can be deleted and rebuilt without
touching a single decision.

DECISIONS ARE JOINED ON ct_key, NOT job_key.  A job decided in ai12 is already
gone from the scout lane, and a job decided here disappears from ai12 on the
next carry_forward.  That is the same key triage_load.carry_forward() uses and
for the same reason.

NO COMPANY+TITLE COLLAPSE.  ma_remote_build collapses 424,442 rows to 264,379
by keeping whichever source copy wins the sort, which is precisely what makes
job_key unstable across rebuilds and forces the whole ct_key carry-forward
machinery.  A search corpus wants every copy -- so all of them are kept, the
master's own job_key stays stable, and `dup_rank` (0 for the best copy of a
ct_key) lets the card lane show one while search can still reach the rest.
"""
import argparse
import csv
import datetime
import glob
import os
import sqlite3
import sys

import score_features as F

HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(HERE, "corpus.db")
BATCH = 5000

# Kept out of `corpus` deliberately: `boards` and `sources` are long
# semicolon-joined strings that would roughly double the table for a field
# nothing ranks on.  board_count / n_sources carry the part that is scored.
SKIP_COLS = ("boards", "sources", "equity", "department",
             "job_key")          # declared separately as the primary key

DDL = """
CREATE TABLE IF NOT EXISTS corpus (
  job_key TEXT PRIMARY KEY,
  ct_key TEXT, company_norm TEXT, dup_rank INTEGER DEFAULT 0,
  {cols},
  fabricated INTEGER DEFAULT 0,
  loaded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_corpus_ct   ON corpus(ct_key);
CREATE INDEX IF NOT EXISTS idx_corpus_co   ON corpus(company_norm);
CREATE INDEX IF NOT EXISTS idx_corpus_dup  ON corpus(dup_rank);
CREATE INDEX IF NOT EXISTS idx_corpus_post ON corpus(posted);
"""


def row_cols(header):
    return [c for c in header if c not in SKIP_COLS]


def norm_co(s):
    import triage_load as TL
    return TL.norm_co(s)


def latest_csv():
    got = sorted(glob.glob(os.path.join(HERE, "US_Corpus_*.csv")))
    return got[-1] if got else ""


def build(path, dbpath=DB, limit=0, progress=200000):
    csv.field_size_limit(10 ** 9)
    today = datetime.date.today().isoformat()
    fh = open(path, newline="", encoding="utf-8", errors="replace")
    rd = csv.reader(fh)
    header = next(rd)
    cols = row_cols(header)
    hidx = {c: i for i, c in enumerate(header)}

    if os.path.exists(dbpath):
        os.remove(dbpath)
    for ext in ("-wal", "-shm"):
        if os.path.exists(dbpath + ext):
            os.remove(dbpath + ext)
    con = sqlite3.connect(dbpath)
    con.executescript("PRAGMA journal_mode=off; PRAGMA synchronous=off;")
    con.executescript(DDL.format(
        cols=", ".join(f'"{c}" TEXT' for c in cols)))
    feat_cols = ", ".join(f'"{n}" INTEGER DEFAULT 0' for n in F.INDEXED)
    con.execute(f"CREATE TABLE IF NOT EXISTS corpus_feat ("
                f"job_key TEXT PRIMARY KEY, coverage INTEGER, family INTEGER, "
                f"{feat_cols})")
    con.execute("CREATE VIRTUAL TABLE IF NOT EXISTS corpus_fts USING fts5("
                "title, company, skills, location, company_industry, "
                "content='', tokenize='porter unicode61')")

    ins = (f"INSERT OR REPLACE INTO corpus "
           f"(job_key, ct_key, company_norm, {','.join(chr(34)+c+chr(34) for c in cols)}, loaded_at) "
           f"VALUES ({','.join('?' * (len(cols) + 4))})")
    finsc = ",".join(f'"{n}"' for n in F.INDEXED)
    finsq = ",".join("?" * (len(F.INDEXED) + 3))
    fins = (f"INSERT OR REPLACE INTO corpus_feat (job_key, coverage, family, {finsc}) "
            f"VALUES ({finsq})")
    ftsins = ("INSERT INTO corpus_fts(rowid, title, company, skills, location, "
              "company_industry) VALUES (?,?,?,?,?,?)")

    nrows = 0
    rows, feats, fts = [], [], []
    con.execute("BEGIN")
    for rec in rd:
        if len(rec) < len(header):
            continue
        d = {c: rec[hidx[c]] for c in header}
        jk = d.get("job_key") or ""
        if not jk:
            continue
        co = (d.get("company") or "").strip()
        ct = f"{norm_co(co)}|{(d.get('title') or '').strip().casefold()}" if co else ""
        rows.append([jk, ct, norm_co(co)] + [d.get(c, "") for c in cols] + [today])
        vec = F.vector(d, today)
        feats.append([jk, F.coverage(d), F.family(d.get("title", ""))] + vec)
        nrows += 1
        fts.append((nrows, d.get("title", ""), d.get("company", ""),
                    d.get("skills", ""), d.get("location", ""),
                    d.get("company_industry", "")))
        if len(rows) >= BATCH:
            con.executemany(ins, rows)
            con.executemany(fins, feats)
            con.executemany(ftsins, fts)
            rows, feats, fts = [], [], []
        if progress and nrows % progress == 0:
            print(f"  .. {nrows:,}", flush=True)
        if limit and nrows >= limit:
            break
    if rows:
        con.executemany(ins, rows)
        con.executemany(fins, feats)
        con.executemany(ftsins, fts)
    con.execute("COMMIT")
    fh.close()

    # dup_rank: 0 for the best copy of each ct_key.  Prefer an employer-published
    # date over an aggregator's, then the newest, then a stable key so two runs
    # over the same corpus agree.  `fts.rowid` is the load order, which is why
    # the FTS row for a job maps to its corpus rowid and not the other way round.
    print("  ranking duplicates ...", flush=True)
    con.execute("""
      WITH r AS (
        SELECT c.rowid AS rid,
               ROW_NUMBER() OVER (
                 PARTITION BY c.ct_key
                 ORDER BY f."date.employer" DESC, f."src.direct" DESC,
                          c.posted DESC, c.job_key
               ) - 1 AS rk
        FROM corpus c JOIN corpus_feat f ON f.job_key = c.job_key
        WHERE c.ct_key <> ''
      )
      UPDATE corpus SET dup_rank = (SELECT rk FROM r WHERE r.rid = corpus.rowid)
      WHERE ct_key <> ''""")
    con.commit()
    print("  ANALYZE ...", flush=True)
    con.execute("ANALYZE")            # partial/compound indexes are inert without it
    con.commit()
    con.close()
    return nrows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--db", default=DB)
    a = ap.parse_args()
    path = a.csv or latest_csv()
    if not path or not os.path.exists(path):
        sys.exit("no US_Corpus_*.csv -- run corpus_build.py first")
    print(f"loading {os.path.basename(path)} "
          f"({os.path.getsize(path) / 1e6:,.0f} MB) -> {os.path.basename(a.db)}",
          flush=True)
    n = build(path, a.db, a.limit)
    con = sqlite3.connect(a.db)
    dups = con.execute("SELECT COUNT(*) FROM corpus WHERE dup_rank > 0").fetchone()[0]
    con.close()
    mb = os.path.getsize(a.db) / 1e6
    print(f"\ncorpus.db: {n:,} rows ({dups:,} duplicate copies, "
          f"{n - dups:,} distinct jobs), {mb:,.0f} MB, "
          f"{len(F.INDEXED)} feature columns + FTS5")
    return 0


if __name__ == "__main__":
    sys.exit(main())
