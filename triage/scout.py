#!/usr/bin/env python3
"""The scout lane: gate -> score -> explore -> cards, all in memory.

ONE CUSTOMISABLE LANE.  The other thirty are a predicate plus a fixed ORDER BY,
each ranking on a column some builder wrote.  This one holds the whole gated
corpus and takes its order from a weight profile the page can change between
two fetches, so "score it on different parameters" is a slider rather than a
rebuild.  Search is folded into the same ranking rather than filtering it: a
query contributes bm25 relevance as one more weighted term, so a strong match
rises to the top and the good rows the query missed are still there.

WHERE THE STATE LIVES.  corpus.db is read-only here and triage.db is where
every decision already is; this module never writes to either.  The decided
mask is built from triage.db.decisions ON ct_key -- the same key
triage_load.carry_forward() uses, and the reason a job judged in ai12 does not
come back here under a different job_key.

THE EXPLORE SLICE.  Ranking by score alone means the lane can only ever show
more of what it already ranks highly, so 15% of every batch is drawn from the
MIDDLE of the distribution instead of the top -- between the 40th and 95th
percentile of eligible rows, capped at one per company and two per title family
so a batch cannot spend its whole exploration budget on one employer.  There is
no model here and therefore no uncertainty estimate; this is the honest version
of "show me something I would not otherwise see".
"""
import os
import sqlite3
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import corpus_load as CL          # noqa: E402
import score_features as F        # noqa: E402
import triage_load as TL          # noqa: E402
import score_profile as SP        # noqa: E402
import scout_index as SI          # noqa: E402

EXPLORE = 0.15
# Explore draws from the rows ranked JUST BELOW the ones you are already being
# shown -- ranks n_top .. n_top+EXPLORE_POOL -- not from a percentile band.
# Measured on the real corpus with the default weights: 539,899 eligible rows,
# max score 129, mean 20.8, and the 40th-95th percentile is 14.9 to 51.3.  A
# percentile band therefore spent the whole exploration budget on rows the
# ranking had already judged worthless (the first one drawn was a highway
# construction manager at 16.0).  Rank-relative asks the question that is
# actually worth asking -- "what is good that I am not seeing" -- and it
# behaves the same on a 45-row test corpus as on a 540k one, where a percentile
# band collapses to nothing.
EXPLORE_POOL = 5000
EXPLORE_PER_CO, EXPLORE_PER_FAM = 1, 2
LABEL = "Scout · full corpus"


class Scout:
    def __init__(self, npz=SI.NPZ, corpus_db=CL.DB, triage_con=None):
        self.z = SI.load(npz)
        # int16 on disk, float32 in memory, converted ONCE.  A matrix-vector
        # product against an int16 array upcasts a fresh copy every call; at
        # 925,577 x 64 that is the whole cost of a fetch.  237 MB resident is
        # the price of a slider that answers in single-digit milliseconds.
        self.feat16 = self.z["feat"]
        self.feat = np.ascontiguousarray(self.feat16, dtype=np.float32)
        self.rowid = self.z["rowid"]
        self.ct_hash = self.z["ct_hash"]
        self.co_hash = self.z["co_hash"]
        self.family = self.z["family"]
        self.coverage = self.z["coverage"]
        self.dup_rank = self.z["dup_rank"]
        self.n = len(self.rowid)
        self.corpus_db = corpus_db
        self._con = None
        # Everything masked OUT: decided elsewhere, company killed, or served
        # in this session and not yet judged.
        self.blocked = np.zeros(self.n, dtype=bool)
        self.decided = np.zeros(self.n, dtype=bool)
        self.served = np.zeros(self.n, dtype=bool)
        if triage_con is not None:
            self.refresh(triage_con)

    # ------------------------------------------------------------- lifecycle
    def con(self):
        if self._con is None:
            import sqlite3
            self._con = sqlite3.connect(f"file:{self.corpus_db}?mode=ro",
                                        uri=True, check_same_thread=False)
            self._con.row_factory = sqlite3.Row
        return self._con

    def refresh(self, triage_con):
        """Rebuild decided/blocked from triage.db.  ~3.7k + 2 rows hashed."""
        dec = [SI.h64(r[0]) for r in triage_con.execute(
            "SELECT DISTINCT ct_key FROM decisions WHERE ct_key <> ''")]
        blk = [SI.h64(r[0]) for r in triage_con.execute(
            "SELECT company_norm FROM blocked_companies")]
        self.decided = np.isin(self.ct_hash, np.array(dec, dtype=np.int64)) \
            if dec else np.zeros(self.n, dtype=bool)
        self.blocked = np.isin(self.co_hash, np.array(blk, dtype=np.int64)) \
            if blk else np.zeros(self.n, dtype=bool)
        self.served[:] = False
        return int(self.decided.sum()), int(self.blocked.sum())

    def mark(self, ct_key):
        """A decision just landed; hide every copy of that job immediately.

        By ct_key, not job_key, because the corpus deliberately keeps every
        duplicate copy of a posting -- deciding one has to retire all of them or
        the same job comes back under the next source's row.
        """
        h = SI.h64(ct_key or "")
        if h:
            self.decided |= (self.ct_hash == h)

    # ---------------------------------------------------------------- search
    def query_values(self, q, limit=20000):
        """0..100 relevance per row for an FTS5 query, or None.

        bm25() is negative and smaller-is-better; it is flipped and scaled onto
        the same 0..100 range every other feature uses so the query slider is
        directly comparable to the others.  A syntax error in a half-typed
        query (`ai AND`) must not 500 the page -- it reads as no query.
        """
        q = (q or "").strip()
        if not q:
            return None, 0
        try:
            rows = self.con().execute(
                "SELECT rowid, bm25(corpus_fts) AS b FROM corpus_fts "
                "WHERE corpus_fts MATCH ? ORDER BY b LIMIT ?",
                (q, limit)).fetchall()
        except Exception:
            try:                       # treat it as a bag of words, not syntax
                safe = " OR ".join(f'"{w}"' for w in q.split() if w)
                rows = self.con().execute(
                    "SELECT rowid, bm25(corpus_fts) AS b FROM corpus_fts "
                    "WHERE corpus_fts MATCH ? ORDER BY b LIMIT ?",
                    (safe, limit)).fetchall() if safe else []
            except Exception:
                return None, 0
        if not rows:
            return None, 0
        rid = np.array([r["rowid"] for r in rows], dtype=np.int64)
        b = -np.array([r["b"] for r in rows], dtype=np.float32)
        lo, hi = float(b.min()), float(b.max())
        v = np.full(len(b), 100.0, dtype=np.float32) if hi <= lo else \
            (b - lo) * (100.0 / (hi - lo))
        out = np.zeros(self.n, dtype=np.float32)
        idx = np.searchsorted(self.rowid, rid)
        ok = (idx < self.n) & (self.rowid[np.clip(idx, 0, self.n - 1)] == rid)
        out[idx[ok]] = v[ok]
        return out, int(ok.sum())

    # ----------------------------------------------------------------- cards
    def eligible(self, searching):
        """Rows this lane may show.  Duplicate copies are hidden unless you are
        searching, where finding every copy is the point."""
        m = ~self.decided & ~self.blocked & ~self.served
        if not searching:
            m &= (self.dup_rank == 0)
        return m

    def pick(self, profile, q="", n=60, explore=EXPLORE):
        """(indices, scores, qvals, explore_flags) for the next n cards."""
        qv, nhit = self.query_values(q)
        score = SP.score_matrix(self.feat, profile, qv)
        elig = self.eligible(qv is not None)
        ne = int(elig.sum())
        if not ne:
            return np.array([], dtype=int), score, qv, set(), 0, nhit
        if qv is not None:
            elig = elig & (qv > 0) if (elig & (qv > 0)).any() else elig

        idx = np.flatnonzero(elig)
        sc = score[idx]
        n_exp = int(round(n * explore)) if explore > 0 else 0
        n_top = max(0, n - n_exp)

        # ONE partition serves both slices.  Pull the best (n_top + pool) rows
        # in O(n), sort just those, then read the exploit cards off the front
        # and sample the explore cards from the tail behind them.  Sorting all
        # 539,899 eligible rows to show sixty was ~50ms for an order whose
        # tail nobody reads.
        want = min(n_top + EXPLORE_POOL, len(sc))
        if want < len(sc):
            part = np.argpartition(-sc, want - 1)[:want]
        else:
            part = np.arange(len(sc))
        part = part[np.argsort(-sc[part], kind="stable")]

        top = idx[part[:min(n_top, len(part))]]
        exp = self._explore(idx, part[n_top:], set(top.tolist()), n_exp)
        order = list(top) + exp
        return np.array(order, dtype=int), score, qv, set(exp), ne, nhit

    def _explore(self, idx, pool, taken, want):
        """Sample the ranks just below the exploit slice, spread across
        companies and title families so one employer cannot take the budget."""
        if want <= 0 or not len(pool):
            return []
        # Deterministic: the same corpus, profile and query give the same
        # explore cards, so a card you skip past is still there on a re-fetch.
        # Seeded off the pool's own contents, never the clock.
        rng = np.random.default_rng(int(pool[0]) * 131 + len(pool))
        order = pool if len(pool) <= max(want * 400, 64) else \
            pool[np.unique(rng.integers(0, len(pool),
                                        size=max(want * 400, 64)))]
        order = order[rng.permutation(len(order))]
        out, nco, nfam = [], {}, {}
        for b in order:
            i = int(idx[b])
            if i in taken:
                continue
            c, f = int(self.co_hash[i]), int(self.family[i])
            if nco.get(c, 0) >= EXPLORE_PER_CO or nfam.get(f, 0) >= EXPLORE_PER_FAM:
                continue
            nco[c] = nco.get(c, 0) + 1
            nfam[f] = nfam.get(f, 0) + 1
            out.append(i)
            if len(out) >= want:
                break
        return out

    def refs(self, rows, tcon=None):
        """{rowid: ref} for the ~60 rows about to be returned.

        A lookup rather than a column, because there is nowhere to put a column:
        corpus.db is rebuilt from scratch by corpus_load.py on every run, so
        anything written there is destroyed, and its rowids renumber completely
        when it is.  The ledger lives in triage.db and this reads it.

        Keyed on TL.ct_key(company, title) computed HERE, never on the stored
        corpus.ct_key -- corpus_load.py omits the whitespace collapse that
        ct_key() applies and the two disagree on 7,839 of 978,443 rows, which
        would hand those jobs a different ref from their triage twin.
        """
        want = {}
        for rid, r in rows.items():
            k = TL.ct_key(r["company"], r["title"])
            want[rid] = k or ("jk:" + (r["job_key"] or ""))
        if not want:
            return {}
        own = tcon is None
        con = TL.connect() if own else tcon
        try:
            keys = sorted(set(want.values()))
            found = {}
            # Chunked: SQLITE_MAX_VARIABLE_NUMBER is 32k on modern builds but
            # 999 on older ones, and a batch is only ever ~60-120 rows anyway.
            for i in range(0, len(keys), 500):
                part = keys[i:i + 500]
                marks = ",".join("?" * len(part))
                found.update(con.execute(
                    f"SELECT ref_key, ref FROM refs WHERE ref_key IN ({marks})",
                    part))
        except sqlite3.OperationalError:
            # No `refs` table yet -- it is minted by a full load, so a database
            # that has never been loaded does not have one.  A ref is a display
            # convenience; every card renders without it.  Degrade to "no ref"
            # rather than raise, the same call server.py makes when corpus.db
            # and the index are missing.
            found = {}
        finally:
            if own:
                con.close()
        return {rid: found.get(k, 0) for rid, k in want.items()}

    def cards(self, profile, q="", n=60, explore=EXPLORE, tcon=None):
        order, score, qv, expset, ne, nhit = self.pick(profile, q, n, explore)
        if not len(order):
            return {"cards": [], "eligible": ne, "matched": nhit}
        rids = [int(self.rowid[i]) for i in order]
        marks = ",".join("?" * len(rids))
        rows = {r["rowid"]: r for r in self.con().execute(
            f"SELECT rowid, * FROM corpus WHERE rowid IN ({marks})", rids)}
        refs = self.refs(rows, tcon)
        out = []
        for i in order:
            r = rows.get(int(self.rowid[i]))
            if r is None:
                continue
            fv = {nm: int(v) for nm, v in zip(F.INDEXED, self.feat16[i])}
            qval = float(qv[i]) if qv is not None else 0.0
            d = {k: r[k] for k in r.keys() if k != "rowid"}
            d["ref"] = refs.get(int(self.rowid[i]), 0)
            d["score"] = round(float(score[i]), 1)
            d["coverage"] = int(self.coverage[i])
            d["family"] = F.FAMILIES[int(self.family[i])]
            d["explore"] = int(i) in expset
            d["why"] = SP.contributions(fv, profile, qval)
            out.append(d)
            self.served[i] = True
        return {"cards": out, "eligible": ne, "matched": nhit}


# --------------------------------------------------------------------- manual
def main():
    import argparse
    import sqlite3
    import time
    ap = argparse.ArgumentParser()
    ap.add_argument("-q", default="")
    ap.add_argument("-n", type=int, default=10)
    ap.add_argument("--explore", type=float, default=EXPLORE)
    ap.add_argument("--profile", default="default")
    ap.add_argument("--bench", action="store_true")
    a = ap.parse_args()

    import triage_load as TL
    t0 = time.time()
    s = Scout(triage_con=TL.connect())
    print(f"loaded {s.n:,} rows in {time.time() - t0:.2f}s  "
          f"decided={int(s.decided.sum()):,} blocked={int(s.blocked.sum()):,} "
          f"dup={int((s.dup_rank > 0).sum()):,}")
    p = SP.load_all().get(a.profile) or SP.default_profile()

    if a.bench:
        for label, qq in (("no query", ""), ("query", a.q or "forward deployed")):
            t = time.time()
            for _ in range(5):
                s.served[:] = False
                s.pick(p, qq, 60)
            print(f"  {label:9s} {(time.time() - t) / 5 * 1000:6.1f} ms / fetch")
        return 0

    t = time.time()
    res = s.cards(p, a.q, a.n, a.explore)
    ms = (time.time() - t) * 1000
    print(f"{res['eligible']:,} eligible, {res['matched']:,} query hits, "
          f"{ms:.0f} ms\n")
    for c in res["cards"]:
        tag = " [explore]" if c["explore"] else ""
        print(f"{c['score']:7.1f}{tag}  {(c['title'] or '')[:52]:52s} "
              f"{(c['company'] or '')[:22]:22s} {(c['location'] or '')[:26]}")
        print(f"          cov={c['coverage']}% {c['posted'] or '-'} "
              f"{c['source']} · "
              + ", ".join(f"{k} {v:+g}" for k, v in c["why"][:6]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
