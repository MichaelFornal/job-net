#!/usr/bin/env python3
"""Weights, gates, and the score itself -- in two implementations that must agree.

score_features.py says what is TRUE about a row.  This says what it is WORTH,
and keeps that entirely out of the index so changing a weight re-ranks 700k rows
without rebuilding anything.

    score = SUM_axis  W_axis * SUM_{i in axis} P_i * f_i / 100
            with the BONUS axes' subtotal capped at `bonus_cap`

THE CAP IS THE POINT.  Bonus axes (domain / comp / company) need data 11-34% of
rows have.  Uncapped, a row that happens to disclose salary AND carry a startup
tier AND publish skills outranks a better-matched row that publishes none of
them -- not because it is a better job but because it is a better-documented
one.  Capping the bonus subtotal bounds how far documentation alone can carry a
row, and `coverage` on the card says when a score rested on thin evidence.

TWO IMPLEMENTATIONS.  The numpy path is what the server uses: one matrix-vector
product over the whole corpus, ~10-30ms, so a slider is interactive.  The SQL
path is the readable reference.  `--verify` asserts they agree exactly on real
rows; if they ever diverge, the SQL one is right by definition.

GATES ARE NOT WEIGHTS.  A gate is a filter with no points attached.  The current
builders give four different answers to that question -- local_build scores a
killed title DOWN (keeps proximity, caps at 12), ff_build is a strict
conjunction, picks requires a title hit, sfl admits killed rows at 0.  Here:
US basis, kill filter and the rung ceiling are gates, applied once in
corpus_build.py; everything else is a signed weight you can slide to zero.
"""
import argparse
import json
import os
import sys

import score_features as F

HERE = os.path.dirname(os.path.abspath(__file__))
PROFILES = os.path.join(HERE, "score_profiles.json")

# The gates corpus_build.py applies.  Recorded in the profile so the lane and
# the builder cannot disagree about what is in the corpus -- selftest asserts it.
GATE_DEFAULTS = {
    "us_only": True,          # remote_us_classify.us_basis, not the country column
    "allow_worldwide": True,  # 'anywhere/worldwide' listings are US-eligible
    "exclude_killed": True,   # picks_build.killed_by + local_build.LOCAL_OFF
    "rung_max": 4,            # level_build.rung_src: drop Director/VP/C-level
}


def default_profile(name="default"):
    return {
        "name": name,
        "axis": {a: 1.0 for a in sorted(set(F.AXIS.values()))},
        "points": dict(F.DEFAULTS),
        "bonus_cap": 42,
        "gates": dict(GATE_DEFAULTS),
    }


def normalise(p):
    """Fill in anything a saved profile is missing, drop anything unknown.

    A profile saved before a feature was added must keep working: an absent
    weight takes the schema default rather than reading as zero, because zero
    is a real thing a slider can say and 'not mentioned' is not.
    """
    d = default_profile(p.get("name", "default"))
    d["axis"].update({k: float(v) for k, v in (p.get("axis") or {}).items()
                      if k in d["axis"]})
    d["points"].update({k: float(v) for k, v in (p.get("points") or {}).items()
                        if k in d["points"]})
    d["gates"].update({k: v for k, v in (p.get("gates") or {}).items()
                       if k in d["gates"]})
    if p.get("bonus_cap") is not None:
        d["bonus_cap"] = float(p["bonus_cap"])
    return d


def load_all(path=PROFILES):
    if not os.path.exists(path):
        return {"default": default_profile()}
    with open(path) as fh:
        raw = json.load(fh)
    return {k: normalise(v) for k, v in raw.items()} or {"default": default_profile()}


def save_all(profiles, path=PROFILES):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(profiles, fh, indent=1, sort_keys=True)
    os.replace(tmp, path)      # atomic: a killed write must not truncate the file


# ------------------------------------------------------------------ the weights
def weights(p):
    """{feature: effective points} = axis multiplier x per-feature points."""
    return {n: p["axis"].get(F.AXIS[n], 1.0) * p["points"].get(n, F.DEFAULTS[n])
            for n in F.NAMES}


def weight_vectors(p):
    """(w_hard, w_bonus) -- two FULL-WIDTH float32 vectors over INDEXED.

    Full width, with the other half zeroed, rather than a pair of column
    slices.  `M[:, hard_cols]` looks equivalent and is not: fancy indexing
    COPIES, so at corpus scale each call materialised ~185 MB before it
    multiplied anything, and one slider drag cost 654 ms.  Two masked
    matrix-vector products over the same contiguous matrix hit BLAS directly
    and cost single-digit ms.  Measured, not assumed -- see --bench.
    """
    import numpy as np
    w = weights(p)
    wh = np.zeros(len(F.INDEXED), dtype=np.float32)
    wb = np.zeros(len(F.INDEXED), dtype=np.float32)
    for i, n in enumerate(F.INDEXED):
        (wb if F.AXIS[n] in F.BONUS_AXES else wh)[i] = w[n] / 100.0
    return wh, wb


# ----------------------------------------------------------------- numpy path
def score_matrix(M, p, qvals=None):
    """Score every row of an (n, len(INDEXED)) matrix.  Returns float32 (n,).

    `qvals` is an optional 0..100 float array of query relevance, weighted by
    the q.bm25 slider.  It is not in the matrix because it changes per search.
    """
    import numpy as np
    wh, wb = weight_vectors(p)
    if M.dtype != np.float32:
        # Callers that hand over the int16 matrix straight from the npz still
        # work; the server converts once at startup so this never runs there.
        M = M.astype(np.float32)
    base = M @ wh
    bonus = M @ wb
    np.minimum(bonus, np.float32(p["bonus_cap"]), out=bonus)
    out = base + bonus
    if qvals is not None:
        w = weights(p)["q.bm25"] / 100.0
        out = out + np.asarray(qvals, dtype=np.float32) * np.float32(w)
    return out


def score_row(feats, p, qval=0.0):
    """Score one {name: 0..100} dict.  The definition the other two follow."""
    w = weights(p)
    base = sum(w[n] * feats.get(n, 0) / 100.0
               for n in F.INDEXED if F.AXIS[n] not in F.BONUS_AXES)
    bonus = sum(w[n] * feats.get(n, 0) / 100.0
                for n in F.INDEXED if F.AXIS[n] in F.BONUS_AXES)
    return base + min(bonus, p["bonus_cap"]) + w["q.bm25"] * qval / 100.0


# ------------------------------------------------------------------- SQL path
def sql_expr(p, prefix="f."):
    """The same score as a SQLite expression over corpus_feat columns."""
    w = weights(p)
    def term(names):
        parts = [f'{w[n] / 100.0:.10g}*{prefix}"{n}"' for n in names
                 if abs(w[n]) > 1e-12]
        return "(" + (" + ".join(parts) if parts else "0") + ")"
    hard = term([n for n in F.INDEXED if F.AXIS[n] not in F.BONUS_AXES])
    bonus = term([n for n in F.INDEXED if F.AXIS[n] in F.BONUS_AXES])
    return f"({hard} + MIN({bonus}, {float(p['bonus_cap']):.10g}))"


# --------------------------------------------------------------------- the why
def contributions(feats, p, qval=0.0):
    """[(name, points), ...] sorted by |points| desc -- what made the score.

    `why` is DERIVED here rather than stored, which is why it can update as a
    slider moves.  The repo's existing invariant is "every point traceable to a
    phrase in why" (picks_build.py:338); computing the string from the same
    numbers the sort uses makes that true by construction instead of by
    discipline.  The bonus cap is attributed proportionally so the parts still
    sum to the whole -- selftest asserts that.
    """
    w = weights(p)
    hard, bonus = [], []
    for n in F.INDEXED:
        v = feats.get(n, 0)
        if not v:
            continue
        pts = w[n] * v / 100.0
        if abs(pts) < 1e-9:
            continue
        (bonus if F.AXIS[n] in F.BONUS_AXES else hard).append([n, pts])
    raw = sum(x[1] for x in bonus)
    cap = p["bonus_cap"]
    if raw > cap and raw > 0:
        scale = cap / raw
        for x in bonus:
            x[1] *= scale
    out = hard + bonus
    if qval:
        out.append(["q.bm25", w["q.bm25"] * qval / 100.0])
    out.sort(key=lambda x: (-abs(x[1]), x[0]))     # name breaks ties: determinism
    return [(n, round(v, 2)) for n, v in out]


# ------------------------------------------------------------------- verifying
def _verify(n=10000):
    import random
    import sqlite3
    import numpy as np
    import corpus_load as CL

    if not os.path.exists(CL.DB):
        print(f"no {CL.DB} yet -- verifying on synthetic rows instead")
        rows, feats = [], []
        rnd = random.Random(7)
        for _ in range(n):
            f = {k: (rnd.choice([0, 100]) if F.KIND[k] != F.CONT
                     else rnd.randrange(101)) for k in F.INDEXED}
            feats.append(f)
        M = np.array([[f[k] for k in F.INDEXED] for f in feats], dtype=np.int16)
        p = default_profile()
        vec = score_matrix(M, p)
        ref = np.array([score_row(f, p) for f in feats], dtype=np.float32)
        d = float(np.max(np.abs(vec - ref)))
        print(f"synthetic {n} rows: max |numpy - definition| = {d:.6f}")
        assert d < 1e-3, "numpy path disagrees with score_row"
        print("OK")
        return 0

    con = sqlite3.connect(CL.DB)
    con.row_factory = sqlite3.Row
    p = default_profile()
    cols = ",".join(f'"{c}"' for c in F.INDEXED)
    rows = con.execute(
        f"SELECT rowid, {cols}, {sql_expr(p, prefix='')} AS s "
        f"FROM corpus_feat ORDER BY rowid LIMIT ?", (n,)).fetchall()
    M = np.array([[r[c] for c in F.INDEXED] for r in rows], dtype=np.int16)
    vec = score_matrix(M, p)
    ref = np.array([r["s"] for r in rows], dtype=np.float32)
    d = float(np.max(np.abs(vec - ref))) if len(rows) else 0.0
    print(f"{len(rows):,} real rows: max |numpy - SQL| = {d:.6f}")
    assert d < 1e-3, "numpy path disagrees with SQL"

    # contributions must sum to the score
    worst = 0.0
    for r in rows[:2000]:
        f = {c: r[c] for c in F.INDEXED}
        tot = sum(v for _, v in contributions(f, p))
        worst = max(worst, abs(tot - score_row(f, p)))
    print(f"contributions vs score: max drift = {worst:.4f}")
    assert worst < 0.05, "why does not sum to the score"
    print("OK")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--init", action="store_true",
                    help="write score_profiles.json with the default profile")
    a = ap.parse_args()
    if a.init:
        cur = load_all()
        cur.setdefault("default", default_profile())
        save_all(cur)
        print(f"wrote {PROFILES}: {', '.join(sorted(cur))}")
        return 0
    if a.show:
        p = load_all().get("default", default_profile())
        w = weights(p)
        for axis in sorted(set(F.AXIS.values())):
            print(f"[{axis}]  x{p['axis'].get(axis, 1.0)}")
            for nme in F.NAMES:
                if F.AXIS[nme] == axis:
                    print(f"   {nme:20s} {w[nme]:+7.1f}")
        print(f"bonus_cap {p['bonus_cap']}   gates {p['gates']}")
        return 0
    if a.verify:
        return _verify()
    print(f"{F.NFEAT} features, {len(F.INDEXED)} indexed. "
          f"--show | --verify | --init")
    return 0


if __name__ == "__main__":
    sys.exit(main())
