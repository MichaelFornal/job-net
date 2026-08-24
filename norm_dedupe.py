#!/usr/bin/env python3
"""
norm_dedupe.py — collapse duplicate rows in contract_norm/*.jsonl in place.

  python3 norm_dedupe.py [--dry-run] [--min-dups N] [file.jsonl ...]

With no file arguments it processes every *.jsonl in contract_norm/ that
bh_build_combined.py lists in INCLUDE.

WHY THIS EXISTS
Most of the tail fetchers write through contract_common.Writer, which is
unconditionally append mode.  Several of them re-fetch tenants they have
already covered — either because a refresh flag (JOBVITE_REFRESH, GEM_REFRESH,
COMEET_REFRESH, SMALLREMOTE_FORCE) forces it, or because the fetcher keeps no
skip state at all (arbeitsagentur has no state file; intlmisc has no skip
state; four of intlboards' six sources re-append in full on every run).  So the
same posting accumulates a fresh copy per run.  Measured duplicate rates before
this script existed: eightfold 41%, gem 39%, paycom 36%, adp 35%, jobvite 34%,
comeet 33%, oracle 29%, smallremote 28%, icims 25%.

This is a DISK and BUILD-TIME problem, not a correctness problem —
bh_build_combined.py already collapses on (source, board, job_id) keeping
max(posted), so the master is right either way.  But all_jobs_build.py streams
every one of those bytes at 3-5 GB RSS on a 16 GB box whose monolith build
already needs a 6 GiB disk guard, so the duplicates are not free.

WHAT IT DOES NOT DO
It does not truncate and re-fetch.  That is the nicheboards failure mode: a
fetcher that discards its output up front turns one refusing upstream source
into permanent data loss.  Collapsing an append-only file is always safe;
emptying one to refill it is not.

THREE RULES, each taken from something already in this repo:

1. KEY = (source, board, str(job_id)), identical to bh_build_combined.py.
   Board-scoped on purpose — bamboohr and bullhorn reuse per-board integer ids,
   so (source, job_id) alone over-merges across agencies.

2. KEEP THE LAST occurrence.  bh_build_combined.merge() already resolves
   collisions toward max(posted), and first-seen dates are preserved
   independently in the *_posted_ledger.json files, so the later read is
   strictly the better one.  This makes the collapse invisible downstream.

3. ANYTHING WE CANNOT KEY PASSES THROUGH UNTOUCHED — no job_id, unparseable,
   whatever.  workday_fetch.dedupe_out() counts those as "unusable" and DROPS
   them, which is correct for workday (every row has an apply_url) and wrong in
   general: bullhorn rows carry no apply URL at all.  bh_build_combined keeps
   them via `by_id[id(r)]`, and so do we.  A dedupe tool must never be the
   thing that loses a row it did not understand.
"""

import json
import os
import shutil
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
NORM = os.path.join(BASE, "contract_norm")


def _argval(flag, default=None):
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def key_of(d):
    """The bh_build_combined key, or None when the row cannot be keyed."""
    jid = d.get("job_id")
    if jid in (None, ""):
        return None
    return (d.get("source"), d.get("board"), str(jid))


def scan(path):
    """One streaming pass. -> (n_lines, n_keyed, n_distinct, n_unkeyable)."""
    seen = set()
    n = keyed = unkeyable = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue
            n += 1
            try:
                k = key_of(json.loads(line))
            except ValueError:
                unkeyable += 1
                continue
            if k is None:
                unkeyable += 1
            else:
                keyed += 1
                seen.add(k)
    return n, keyed, len(seen), unkeyable


def dedupe(path, dry=False, min_dups=1):
    """Collapse duplicates in place.  -> dict describing what happened."""
    name = os.path.basename(path)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return {"file": name, "action": "skipped", "reason": "missing/empty"}

    n, keyed, distinct, unkeyable = scan(path)
    dups = keyed - distinct
    res = {"file": name, "rows": n, "dups": dups, "unkeyable": unkeyable}

    # Free on the self-deduping files (careerjet/themuse/jobgether/web3career/
    # nicheboards/atstail all rebuild a seen-set from their own output and
    # measure 0% duplicate), so this is safe to point at everything.
    if dups < max(1, min_dups):
        res["action"] = "clean"
        return res
    if dry:
        res["action"] = "would-dedupe"
        return res

    # The tmp file doubles the footprint while it is being written.
    need = os.path.getsize(path) * 2
    free = shutil.disk_usage(BASE).free
    if free < need:
        res["action"] = "refused"
        res["reason"] = (f"needs {need / 1e9:.1f} GB free "
                         f"(tmp doubles it), have {free / 1e9:.1f} GB")
        return res

    best, order, out_n = {}, [], 0
    tmp = path + ".tmp"
    try:
        with open(path, encoding="utf-8", errors="replace") as f, \
                open(tmp, "w", encoding="utf-8") as w:
            for line in f:
                if not line.strip():
                    continue
                line = line.rstrip("\n")
                try:
                    k = key_of(json.loads(line))
                except ValueError:
                    k = None
                if k is None:
                    # Rule 3: passes through immediately, position preserved.
                    w.write(line + "\n")
                    out_n += 1
                    continue
                if k not in best:
                    order.append(k)
                    best[k] = line
                else:
                    # FIRST-wins identity, max(posted) — exactly what
                    # bh_build_combined does: `by_id[k] = r` keeps the first
                    # row, and merge() only unions sources and takes the later
                    # date.  It never overwrites company/title.
                    #
                    # Matching that is not pedantry.  company+title IS the
                    # tier-2/3 job_key, so letting a later copy's spelling win
                    # re-keys the row and it reports as newly-discovered in the
                    # next new_jobs diff.  Measured on the 2026-08-14 corpus,
                    # last-wins moved 6,405 (company,title) keys and shifted the
                    # portals master by 220 rows; first-wins is a no-op.
                    #
                    # (workday_fetch.dedupe_out uses last-wins, which is right
                    # for one fetcher's own file and wrong across the corpus.)
                    try:
                        keep = json.loads(best[k])
                        cur = json.loads(line)
                    except ValueError:
                        continue
                    if (cur.get("posted") or "") > (keep.get("posted") or ""):
                        keep["posted"] = cur["posted"]
                        best[k] = json.dumps(keep, ensure_ascii=False)
            for k in order:
                w.write(best[k] + "\n")
                out_n += 1
    except OSError as e:
        # Never leave a half-written tmp lying next to the real file.
        if os.path.exists(tmp):
            os.remove(tmp)
        res["action"] = "failed"
        res["reason"] = str(e)
        return res

    # Refuse to install a result that lost rows we did not intend to drop.
    expected = distinct + unkeyable
    if out_n != expected:
        os.remove(tmp)
        res["action"] = "refused"
        res["reason"] = f"wrote {out_n} rows, expected {expected} — not installing"
        return res

    os.replace(tmp, path)          # atomic
    res.update(action="deduped", out=out_n)
    return res


def include_files():
    """The files bh_build_combined actually reads, so we never touch a stray."""
    try:
        import bh_build_combined as bh
        names = list(bh.INCLUDE)
    except Exception:                                   # noqa: BLE001
        names = [f for f in os.listdir(NORM) if f.endswith(".jsonl")]
    out = []
    for n in names:
        p = n if os.path.isabs(n) else os.path.join(NORM, n)
        if ".prev." not in os.path.basename(p) and os.path.exists(p):
            out.append(p)
    return sorted(set(out))


def main():
    dry = "--dry-run" in sys.argv
    min_dups = int(_argval("--min-dups", "1"))
    args = [a for a in sys.argv[1:]
            if not a.startswith("--") and a != _argval("--min-dups", "\0")]
    paths = [os.path.abspath(a) for a in args] or include_files()

    print(f"norm_dedupe: {len(paths)} files{'  (dry run)' if dry else ''}",
          flush=True)
    total_saved = touched = 0
    for p in paths:
        r = dedupe(p, dry=dry, min_dups=min_dups)
        act = r.get("action")
        if act in ("deduped", "would-dedupe"):
            touched += 1
            total_saved += r.get("dups", 0)
            print(f"  {r['file']:34} {r['rows']:>9,} rows "
                  f"-> {r.get('out', r['rows'] - r['dups']):>9,}  "
                  f"({r['dups']:,} dup)", flush=True)
        elif act in ("refused", "failed"):
            # Loud: this is the case a human needs to see.
            print(f"  !! {r['file']:31} {act.upper()}: {r.get('reason')}",
                  flush=True)
        elif act == "clean" and r.get("unkeyable"):
            print(f"  {r['file']:34} clean "
                  f"({r['unkeyable']:,} unkeyable kept)", flush=True)
    print(f"norm_dedupe: {touched} file(s) {'would collapse' if dry else 'collapsed'}, "
          f"{total_saved:,} duplicate rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
