#!/usr/bin/env python3
"""Invariants for the scout half, on a synthetic corpus it builds itself.

triage/selftest.py runs against a sandbox triage.db; the scout lane ranks
corpus.db through a numpy index, so it needs a corpus of its own rather than a
bolt-on to that file.  Everything here is built in a temp directory and torn
down, so it can run on a machine that has never built the real corpus.

    python3 triage/scout_selftest.py
"""
import csv
import os
import shutil
import sqlite3
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np                                     # noqa: E402

import all_jobs_build as AJB                           # noqa: E402
import corpus_build as CB                              # noqa: E402
import corpus_load as CL                               # noqa: E402
import score_features as F                             # noqa: E402
import score_profile as SP                             # noqa: E402
import scout_index as SI                               # noqa: E402
from triage import scout as SC                         # noqa: E402

fails = []


def ok(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}" + (f"  {detail}" if detail else ""))
    fails.append(not cond)
    return cond


def row(**kw):
    d = {c: "" for c in CB.COLUMNS}
    d.update(kw)
    d.setdefault("job_key", "k" + str(abs(hash(d["title"] + d["company"])) % 10 ** 9))
    return d


# A corpus with: three copies of one job, an over-levelled row, a killed row,
# a foreign row, and enough distinct companies for the explore caps to bite.
ROWS = [
    row(title="Forward Deployed Engineer", company="Anthropic", country="US",
        location="Remote - US", remote="True", work_mode="Remote",
        posted="2026-08-19", source="greenhouse", salary_max="180000",
        salary_period="Year", skills="LLM agents evals",
        employment_type="Full-time", job_key="a1"),
    row(title="Forward Deployed Engineer", company="Anthropic", country="US",
        location="Remote - US", remote="True", posted="2026-08-10",
        source="himalayas", job_key="a2"),
    row(title="Forward Deployed Engineer", company="Anthropic", country="US",
        location="Remote - US", remote="True", posted="2026-08-01",
        source="careerjet", job_key="a3"),
    row(title="Solutions Architect", company="Vega", country="US",
        location="Boston, MA", posted="2026-08-18", source="lever",
        job_key="b1"),
    row(title="Analytics Engineer", company="Ondo", country="US",
        location="Austin, TX, USA", posted="2026-08-15", source="ashby",
        job_key="c1"),
]
for i in range(40):
    ROWS.append(row(title=f"Implementation Consultant {i}", company=f"Co{i}",
                    country="US", location="Austin, TX", posted="2026-07-20",
                    source="workday", job_key=f"z{i}"))

DROPPED = [
    ("over-level", row(title="Director of Engineering", company="Acme",
                       country="US", location="Austin, TX")),
    ("killed", row(title="Registered Nurse", company="Hosp", country="US",
                   location="Boston, MA")),
    ("not-us", row(title="AI Engineer", company="Gmbh", country="Germany",
                   location="Berlin, Germany")),
]


def main():
    tmp = tempfile.mkdtemp(prefix="scout_selftest_")
    try:
        csvp = os.path.join(tmp, "US_Corpus_test.csv")
        with open(csvp, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(CB.COLUMNS)
            for d in ROWS:
                w.writerow([d[c] for c in CB.COLUMNS])
        db = os.path.join(tmp, "corpus.db")
        n = CL.build(csvp, db, progress=0)
        print(f"\n[1] corpus: {n} rows")

        # -- gates ---------------------------------------------------------
        print("\n[2] gates -- corpus_build and the lane must agree")
        for why, d in DROPPED:
            r = [d[c] if c in d else "" for c in AJB.COLUMNS]
            got, _ = CB.gates(r, 4, True)
            ok(f"{why!r} row is gated out", got == why, f"got {got!r}")
        r = [ROWS[0][c] if c in ROWS[0] else "" for c in AJB.COLUMNS]
        ok("a target row is admitted", CB.gates(r, 4, True)[0] == "")

        # -- numpy vs SQL --------------------------------------------------
        print("\n[3] scorer parity -- numpy path vs the SQL reference")
        p = SP.default_profile()
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        cols = ",".join(f'"{c}"' for c in F.INDEXED)
        rows = con.execute(
            f"SELECT {cols}, {SP.sql_expr(p, prefix='')} AS s "
            f"FROM corpus_feat ORDER BY job_key").fetchall()
        M = np.array([[r[c] for c in F.INDEXED] for r in rows], dtype=np.int16)
        ref = np.array([r["s"] for r in rows], dtype=np.float32)
        d = float(np.max(np.abs(SP.score_matrix(M, p) - ref)))
        ok("numpy == SQL", d < 1e-3, f"max drift {d:.6f}")

        # a non-default profile too: a cap that actually bites
        p2 = SP.normalise({"bonus_cap": 5, "axis": {"role": 0.5, "fresh": 2.0}})
        d2 = float(np.max(np.abs(
            SP.score_matrix(M, p2)
            - np.array([SP.score_row({c: r[c] for c in F.INDEXED}, p2)
                        for r in rows], dtype=np.float32))))
        ok("numpy == definition under a biting bonus cap", d2 < 1e-3,
           f"max drift {d2:.6f}")

        # -- why sums to the score ----------------------------------------
        print("\n[4] `why` is a decomposition, not a caption")
        worst = 0.0
        for r in rows:
            f = {c: r[c] for c in F.INDEXED}
            for prof in (p, p2):
                worst = max(worst, abs(sum(v for _, v in SP.contributions(f, prof))
                                       - SP.score_row(f, prof)))
        ok("contributions sum to the score", worst < 0.05, f"max {worst:.4f}")

        # -- the index -----------------------------------------------------
        print("\n[5] index + lane")
        npz = os.path.join(tmp, "scout_index.npz")
        SI.build(db, npz, progress=0)
        s = SC.Scout(npz=npz, corpus_db=db)
        ok("index row count matches the corpus", s.n == n, f"{s.n} vs {n}")

        dups = int((s.dup_rank > 0).sum())
        ok("duplicate copies are ranked", dups >= 2, f"{dups} copies")
        elig = int(s.eligible(False).sum())
        ok("duplicates are hidden when not searching", elig == n - dups,
           f"{elig} eligible of {n}")
        ok("duplicates are reachable when searching",
           int(s.eligible(True).sum()) == n)

        # -- ct_key retires every copy ------------------------------------
        con2 = sqlite3.connect(db)
        ct = con2.execute("SELECT ct_key FROM corpus WHERE job_key='a1'").fetchone()[0]
        before = int(s.decided.sum())
        s.mark(ct)
        after = int(s.decided.sum())
        ok("deciding one copy retires all of them", after - before == 3,
           f"{after - before} rows hidden")

        # -- explore caps --------------------------------------------------
        print("\n[6] explore slice")
        s2 = SC.Scout(npz=npz, corpus_db=db)
        idx, score, qv, expset, ne, _ = s2.pick(p, "", 20, 0.5)
        exp = list(expset)
        cos = [int(s2.co_hash[i]) for i in exp]
        fams = [int(s2.family[i]) for i in exp]
        ok("explore cards are drawn", len(exp) > 0, f"{len(exp)} of {len(idx)}")
        ok("at most one explore card per company",
           all(cos.count(c) <= SC.EXPLORE_PER_CO for c in cos))
        ok("at most two explore cards per family",
           all(fams.count(f) <= SC.EXPLORE_PER_FAM for f in fams))
        ok("explore cards are not also top-ranked",
           not (expset & set(idx[:len(idx) - len(exp)].tolist())))

        # -- search --------------------------------------------------------
        print("\n[7] search boosts, it does not filter")
        s3 = SC.Scout(npz=npz, corpus_db=db)
        qv3, nhit = s3.query_values("forward deployed")
        ok("FTS finds the phrase", nhit >= 1, f"{nhit} hits")
        ok("relevance is on the 0..100 scale others use",
           qv3 is not None and 0 <= float(qv3.max()) <= 100)
        s3.served[:] = False
        res = s3.cards(p, "forward deployed", 5)
        ok("a query returns cards", len(res["cards"]) > 0)
        ok("the top card is a match",
           "forward deployed" in (res["cards"][0]["title"] or "").lower(),
           res["cards"][0]["title"])
        s3.served[:] = False
        ok("a nonsense query does not empty the lane",
           len(s3.cards(p, "zzzzzznotaword", 5)["cards"]) > 0)
        s3.served[:] = False
        ok("a half-typed query does not raise",
           isinstance(s3.cards(p, "ai AND", 5)["cards"], list))

        # -- stale index ---------------------------------------------------
        print("\n[8] a stale index is refused, not silently mis-scored")
        z = dict(np.load(npz))
        z["names"] = np.array(F.INDEXED[:-1])
        bad = os.path.join(tmp, "stale.npz")
        np.savez(bad, **z)
        try:
            SI.load(bad)
            ok("stale index rejected", False, "it loaded")
        except SystemExit:
            ok("stale index rejected", True)

        # -- profile round trip -------------------------------------------
        print("\n[9] profiles")
        pp = os.path.join(tmp, "profiles.json")
        SP.save_all({"default": SP.default_profile(),
                     "x": SP.normalise({"axis": {"role": 0.25}})}, pp)
        back = SP.load_all(pp)
        ok("a saved profile round-trips", back["x"]["axis"]["role"] == 0.25)
        ok("a profile missing a weight takes the schema default, not zero",
           SP.normalise({"axis": {}})["points"]["role.target"]
           == F.DEFAULTS["role.target"])
        ok("gates are recorded on the profile",
           SP.default_profile()["gates"]["rung_max"] == 4)
        con.close(); con2.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'ALL PASS' if not any(fails) else str(sum(fails)) + ' FAILED'}"
          f"  ({len(F.INDEXED)} features)")
    return 1 if any(fails) else 0


if __name__ == "__main__":
    sys.exit(main())
