#!/usr/bin/env python3
"""Fingerprint every lane's queue so a change can prove it changed nothing.

The scout work adds a corpus and a lane *alongside* the existing thirty.  The
only way to make "alongside" a fact rather than an intention is to hash what
every lane returns before the change and assert the same hash after it.

    python3 triage/lane_snapshot.py --write baseline.json
    ...make changes...
    python3 triage/lane_snapshot.py --check baseline.json

`cutoff` is recorded and REPLAYED on --check.  Three lanes (`fresh` at 72h,
`ma`/`maold` at RECENT_DAYS) bind a date computed at call time, so a baseline
compared across midnight would report a diff that is just the clock.  Replaying
the recorded cutoff removes the clock from the comparison; `fresh` recomputes
its own 3-day window internally and is therefore reported separately as
`clock_bound` rather than silently failing.
"""
import argparse, hashlib, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lanes as L                      # noqa: E402
import triage_load as TL               # noqa: E402

HEAD = 20          # job_keys per lane folded into the fingerprint
CLOCK_BOUND = {"fresh"}   # lanes whose window is recomputed inside lanes()


def snapshot(con, cutoff=None):
    cutoff = cutoff or L.recent_cutoff()
    out = {"cutoff": cutoff, "lanes": {}}
    for key, label, sql, params in L.lanes(cutoff):
        n = con.execute(L.queue_sql(sql, "COUNT(*)", order=False),
                        params).fetchone()[0]
        rows = con.execute(
            L.queue_sql(sql, "job_key", order=L.order_for(key), limit=HEAD),
            params).fetchall()
        keys = [r[0] for r in rows]
        out["lanes"][key] = {
            "label": label,
            "group": L.group_of(key),
            "remaining": n,
            "head": hashlib.sha256("\n".join(keys).encode()).hexdigest()[:16],
            "clock_bound": key in CLOCK_BOUND,
        }
    return out


def diff(old, new):
    bad, soft = [], []
    ok = sorted(set(old["lanes"]) | set(new["lanes"]))
    for k in ok:
        a, b = old["lanes"].get(k), new["lanes"].get(k)
        if a is None:
            soft.append(f"{k}: NEW lane ({b['remaining']} rows)")
            continue
        if b is None:
            bad.append(f"{k}: LANE DISAPPEARED")
            continue
        if a["remaining"] != b["remaining"] or a["head"] != b["head"]:
            msg = (f"{k}: {a['remaining']} -> {b['remaining']} rows, "
                   f"head {a['head']} -> {b['head']}")
            (soft if a.get("clock_bound") else bad).append(msg)
    return bad, soft


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--write", metavar="FILE")
    p.add_argument("--check", metavar="FILE")
    a = p.parse_args()
    con = TL.connect()
    if a.write:
        snap = snapshot(con)
        with open(a.write, "w") as fh:
            json.dump(snap, fh, indent=1, sort_keys=True)
        tot = sum(v["remaining"] for v in snap["lanes"].values())
        print(f"wrote {a.write}: {len(snap['lanes'])} lanes, "
              f"{tot:,} undecided rows, cutoff {snap['cutoff']}")
        return 0
    if a.check:
        old = json.load(open(a.check))
        new = snapshot(con, old["cutoff"])       # replay the recorded cutoff
        bad, soft = diff(old, new)
        for m in soft:
            print("  ~ " + m)
        for m in bad:
            print("  ! " + m)
        if bad:
            print(f"FAIL: {len(bad)} lane(s) changed")
            return 1
        print(f"OK: {len(new['lanes'])} lanes unchanged"
              + (f" ({len(soft)} clock-bound/new noted)" if soft else ""))
        return 0
    p.error("need --write or --check")


if __name__ == "__main__":
    sys.exit(main())
