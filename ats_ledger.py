#!/usr/bin/env python3
"""
ats_ledger.py — per-board yield ledger + refresh tiering for ats_fetch.py.

  python3 ats_ledger.py --bootstrap [--dry-run]   seed the ledger from the JSONLs
  python3 ats_ledger.py --plan [--date YYYY-MM-DD] what today's run would fetch
  python3 ats_ledger.py --cycle-check [--days 10]  prove the slices cover everything
  python3 ats_ledger.py --verify <platform>        audit a live file against the ledger

Why this exists.  ats_fetch's state file answers exactly one question — "have I
ever fetched this board?" — and once the answer is yes it is yes forever
(ats_fetch.work() short-circuits on == "done").  That was right while the
~70,500-board queue was being drained for the first time: run_all deliberately
never rotated the file (run_all.rotate_family) so successive runs chipped away
at the tail instead of restarting from zero.

On 2026-08-16 the backlog finished draining.  All 71,592 keys read "done", the
seven-platform chain fetched 88 rows, and its eight JSONLs came out
byte-identical to their .prev copies.  The pipes had quietly become a one-time
crawl.  That matters more than the row count suggests: those sources are 17.9%
of the master (568,403 of 3,172,240 rows) and 21,239 of 115,443 companies —
18.4% — are reachable through them and nothing else.

The fix is not "expire everything and re-crawl".  A blind sweep costs ~70k
boards, and 40,290 of those have never produced a single row in the corpus's
lifetime, while 8,943 boards — 12.3% — carry 80% of every row we have.  So the
ledger records what each board is actually WORTH, and eligibility is bought
with that:

    HOT      in the smallest set covering 80% of its platform's rows   every run
    WARM     has produced rows, but is not hot                         1 run in 3
    COLD     has never produced a row                                  1 run in 10
    RETIRED  auto-retired after repeated failure, or confirmed gone    never

Full-corpus coverage lands in ~10 runs while a typical run queues ~21k boards
instead of ~70k.

Two rules that are load-bearing, not preferences:

  * Slice membership is crc32(key) % cycle == date.toordinal() % cycle.
    NOT hash() — str.__hash__ is salted per process by PYTHONHASHSEED, so
    hash(key) % cycle picks a different slice on every attempt of every run and
    coverage would never close.  And it is DATE-derived rather than a counter
    in the ledger, so all of run_all's supervised retries of one run hit the
    same slice: attempt 2 is a cheap resume, not a fresh sweep.

  * `rows` is only ever written by an AUTHORITATIVE fetch (see ats_fetch).  A
    board that answers empty once keeps its old `rows` and only bumps
    empty_streak.  Letting one bad day zero the count would demote a live board
    out of HOT and into a 1-in-10 cadence, which is the freeze in miniature.

Ledger files are per-chain, selected by ATS_LEDGER, for the same reason
ats_fetch has ATS_STATE: run_all runs the seven-platform chain and the workable
chain CONCURRENTLY, and each saves its whole dict at exit.  On 2026-08-10 the
two sharing one state file left it holding 1,194 keys of the 24,836 the main
chain had just finished.  One file per chain, always.
"""

import collections
import datetime
import glob
import json
import os
import sys
import zlib

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
PREFIX = "ats_all_"

# Same eight names, same order, as ats_fetch.PLATFORMS.
PLATFORMS = ["ashby", "smartrecruiters", "recruitee", "breezy", "bamboohr",
             "workable", "jazzhr", "teamtailor"]

LEDGER = os.path.join(BASE, os.environ.get("ATS_LEDGER")
                      or "ats_yield_ledger.json")
STATE = os.path.join(BASE, os.environ.get("ATS_STATE")
                     or "ats_all_fetch_state.json")

# Fraction of a platform's rows the HOT tier must cover.  0.80 is not tuned; it
# is where the measured concentration puts the knee — smartrecruiters reaches
# 80% of 564,504 rows in 273 of its 4,766 boards.
HOT_SHARE = float(os.environ.get("ATS_HOT_SHARE") or 0.80)
WARM_CYCLE = int(os.environ.get("ATS_WARM_CYCLE") or 3)
COLD_CYCLE = int(os.environ.get("ATS_COLD_CYCLE") or 10)

# Boards with no ledger entry at all are newly discovered slugs.  Always
# eligible, but capped so a big discovery run cannot swamp a refresh run.
NEW_CAP = int(os.environ.get("ATS_NEW_CAP") or 5000)

# workable is excluded from tier expiry until its FIRST crawl finishes.
# 2026-08-16: 1,877 of 21,370 slugs settled, 19,385 never fetched, ~780 boards
# per 30-min run.  Re-fetching what it has done would consume the budget it
# needs to reach what it has not.  coverage_report shows it at 9.3%; revisit
# above ~95%.
FIRST_CRAWL = {x for x in (os.environ.get("ATS_FIRST_CRAWL")
                           or "workable").split(",") if x}

# ---------------------------------------------------------------- io


def load(path=None):
    """Ledger dict.  cc.load_state returns {} for missing OR torn files, which
    is the right failure mode here: a lost ledger costs one bootstrap, and
    bootstrap reads only data already on disk."""
    return cc.load_state(path or LEDGER)


def save(led, path=None):
    cc.save_state(path or LEDGER, led)      # tmp + os.replace, already atomic


def meta(led):
    m = led.get("__meta__")
    if not isinstance(m, dict):
        m = {}
        led["__meta__"] = m
    return m


def boards(led):
    """Every real entry, skipping the __meta__ sidecar."""
    return {k: v for k, v in led.items() if k != "__meta__"}


def _today(s=None):
    if s:
        return datetime.date.fromisoformat(s)
    return datetime.datetime.now(datetime.timezone.utc).date()


# ---------------------------------------------------------------- bootstrap


def census(platforms=None):
    """board -> row count, straight off the live JSONLs.

    Free: no network, and measured at 7.7s for all 1,027,224 rows.  `board` is
    populated on 100% of rows in all sixteen files and equals the slug every
    fetcher was called with, and each ats_all_<plat>.jsonl carries exactly one
    `source`, so the JSONL joins to the "platform:slug" state key with no
    mapping table.

    Counts come from the ACCUMULATED file, so a board that went quiet in June
    still shows June's rows.  That is wanted: it means "worth revisiting".  The
    number is replaced by the truth on that board's next authoritative fetch.
    """
    want = set(platforms or PLATFORMS)
    out = {}
    for f in sorted(glob.glob(os.path.join(cc.NORM_DIR, f"{PREFIX}*.jsonl"))):
        if f.endswith(".prev.jsonl"):
            continue
        plat = os.path.basename(f)[len(PREFIX):-len(".jsonl")]
        if plat not in want:
            continue
        c = collections.Counter()
        bad = 0
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    b = json.loads(line).get("board")
                except Exception:
                    bad += 1
                    continue
                if b:
                    c[b] += 1
        out[plat] = (c, sum(c.values()), bad)
    return out


def bootstrap(platforms=None, dry=False, ledger_path=None, state_path=None):
    cen = census(platforms)
    led = load(ledger_path) if not dry else load(ledger_path)
    st = cc.load_state(state_path or STATE)

    print(f"{'platform':18s} {'rows':>10s} {'boards':>8s} {'hot80':>7s} "
          f"{'zero-row':>9s} {'unparsed':>9s}")
    t_rows = t_boards = t_hot = t_zero = 0
    for plat in (platforms or PLATFORMS):
        if plat not in cen:
            continue
        c, rows, bad = cen[plat]
        hot = hot_set(c)
        # Boards the state knows about but that have never emitted a row.
        known = {k.split(":", 1)[1] for k in st
                 if k.split(":", 1)[0] == plat and ":" in k}
        zero = len(known - set(c))
        print(f"  {plat:16s} {rows:>10,} {len(c):>8,} {len(hot):>7,} "
              f"{zero:>9,} {bad:>9,}")
        t_rows += rows
        t_boards += len(c)
        t_hot += len(hot)
        t_zero += zero
        if dry:
            continue
        for slug, n in c.items():
            e = led.setdefault(f"{plat}:{slug}", {})
            e["rows"] = n
        for slug in known - set(c):
            e = led.setdefault(f"{plat}:{slug}", {})
            e.setdefault("rows", 0)
    print(f"  {'TOTAL':16s} {t_rows:>10,} {t_boards:>8,} {t_hot:>7,} "
          f"{t_zero:>9,}")
    if dry:
        print("\n--dry-run: nothing written")
        return led
    m = meta(led)
    m["bootstrapped"] = _today().isoformat()
    m.setdefault("cycle", {"warm": WARM_CYCLE, "cold": COLD_CYCLE})
    save(led, ledger_path)
    print(f"\nwrote {ledger_path or LEDGER} "
          f"({len(boards(led)):,} boards, "
          f"{os.path.getsize(ledger_path or LEDGER) / 1e6:.1f} MB)")
    return led


def hot_set(counter):
    """Smallest set of boards covering HOT_SHARE of the rows in `counter`.

    Ties broken by (-rows, slug) so membership is deterministic across
    bootstraps — otherwise a board could drift in and out of HOT between runs
    for no reason and its cadence would be unstable.
    """
    total = sum(counter.values())
    if not total:
        return set()
    target = total * HOT_SHARE
    cum = 0
    out = set()
    for slug, n in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])):
        out.add(slug)
        cum += n
        if cum >= target:
            break
    return out


# ---------------------------------------------------------------- tiering


def _retired(e):
    return bool(e.get("retired") or e.get("gone"))


def tiers(led, platforms=None):
    """platform -> {slug: tier}.  Computed per platform so a big platform
    cannot starve a small one — the same reasoning as ats_fetch's per-platform
    RPS budgets."""
    out = {}
    for plat in (platforms or PLATFORMS):
        pref = plat + ":"
        counter = collections.Counter()
        entries = {}
        for k, e in led.items():
            if k == "__meta__" or not k.startswith(pref):
                continue
            slug = k[len(pref):]
            entries[slug] = e
            if not _retired(e):
                counter[slug] = int(e.get("rows") or 0)
        counter = collections.Counter({s: n for s, n in counter.items() if n})
        hot = hot_set(counter)
        t = {}
        for slug, e in entries.items():
            if _retired(e):
                t[slug] = "retired"
            elif slug in hot:
                t[slug] = "hot"
            elif int(e.get("rows") or 0) > 0:
                t[slug] = "warm"
            else:
                t[slug] = "cold"
        out[plat] = t
    return out


def _in_slice(key, cycle, day):
    """Stable, process-independent, date-derived slice membership.

    crc32 rather than hash(): str.__hash__ is salted per process, so hash-based
    slices would differ between run_all's supervised attempts of the SAME run.
    """
    if cycle <= 1:
        return True
    return zlib.crc32(key.encode("utf-8")) % cycle == day.toordinal() % cycle


def eligible(led, platforms=None, day=None, state=None):
    """[(platform, slug, tier)] to fetch today.

    `state` is optional and only used to find NEW boards — slugs the fetcher
    knows about that the ledger has never seen.  ats_fetch passes its own slug
    universe instead, which is the authoritative source.
    """
    day = day or _today()
    tt = tiers(led, platforms)
    out = []
    for plat, t in tt.items():
        first_crawl = plat in FIRST_CRAWL
        for slug, tier in t.items():
            if tier == "retired":
                continue
            if first_crawl:
                # Nothing already fetched is re-queued; ats_fetch still picks
                # up everything absent from the ledger as NEW.
                continue
            if tier == "hot":
                out.append((plat, slug, tier))
            elif tier == "warm" and _in_slice(f"{plat}:{slug}", WARM_CYCLE, day):
                out.append((plat, slug, tier))
            elif tier == "cold" and _in_slice(f"{plat}:{slug}", COLD_CYCLE, day):
                out.append((plat, slug, tier))
    return out


def plan(led, platforms=None, day=None):
    """Counts per platform per tier for a given day.  What --plan prints, and
    what ats_fetch prints at the top of a run."""
    day = day or _today()
    el = eligible(led, platforms, day)
    per = collections.Counter()
    for plat, _slug, tier in el:
        per[(plat, tier)] += 1
    return per, len(el)


# ---------------------------------------------------------------- cli


def cmd_plan(platforms, day):
    led = load()
    if not boards(led):
        sys.exit(f"{LEDGER} is empty — run --bootstrap first")
    per, total = plan(led, platforms, day)
    tt = tiers(led, platforms)
    print(f"eligible for {day.isoformat()}  "
          f"(warm 1-in-{WARM_CYCLE}, cold 1-in-{COLD_CYCLE})\n")
    print(f"{'platform':18s} {'hot':>7s} {'warm':>7s} {'cold':>7s} "
          f"{'total':>8s} {'retired':>8s} {'first-crawl':>12s}")
    for plat in (platforms or PLATFORMS):
        if plat not in tt:
            continue
        r = sum(1 for v in tt[plat].values() if v == "retired")
        sub = sum(per[(plat, x)] for x in ("hot", "warm", "cold"))
        print(f"  {plat:16s} {per[(plat,'hot')]:>7,} {per[(plat,'warm')]:>7,} "
              f"{per[(plat,'cold')]:>7,} {sub:>8,} {r:>8,} "
              f"{'yes' if plat in FIRST_CRAWL else '':>12s}")
    print(f"  {'TOTAL':16s} {'':>7s} {'':>7s} {'':>7s} {total:>8,}")


def cmd_cycle_check(platforms, days):
    """Prove the slices actually cover the corpus.

    The whole tiering rests on 'full coverage in ~10 runs'.  That is an
    assertion about crc32 residues, and it is cheap to check, so check it
    rather than believing it.
    """
    led = load()
    if not boards(led):
        sys.exit(f"{LEDGER} is empty — run --bootstrap first")
    tt = tiers(led, platforms)
    start = _today()
    seen = collections.Counter()
    for i in range(days):
        for plat, slug, _t in eligible(led, platforms, start +
                                       datetime.timedelta(days=i)):
            seen[f"{plat}:{slug}"] += 1
    want = {"hot": [], "warm": [], "cold": []}
    for plat, t in tt.items():
        if plat in FIRST_CRAWL:
            continue
        for slug, tier in t.items():
            if tier in want:
                want[tier].append(seen[f"{plat}:{slug}"])
    print(f"over {days} consecutive days:\n")
    ok = True
    for tier, exp in (("hot", days), ("warm", days // WARM_CYCLE),
                      ("cold", days // COLD_CYCLE)):
        v = want[tier]
        if not v:
            continue
        lo, hi = min(v), max(v)
        missed = sum(1 for x in v if x == 0)
        good = missed == 0 and lo >= max(1, exp - 1)
        ok = ok and good
        print(f"  {tier:6s} {len(v):>7,} boards   fetched {lo}-{hi}x "
              f"(expected ~{exp}x)   never fetched: {missed:,}   "
              f"{'ok' if good else '!! GAP'}")
    print(f"\n{'PASS' if ok else 'FAIL'}: every non-retired board is reached "
          f"within {days} runs")
    return 0 if ok else 1


def cmd_verify(plat):
    """Audit a live JSONL against the ledger: does what is on disk match what
    the ledger claims?  Run after a fetch to catch a merge that dropped rows it
    should have kept."""
    led = load()
    f = os.path.join(cc.NORM_DIR, f"{PREFIX}{plat}.jsonl")
    if not os.path.exists(f):
        sys.exit(f"missing {f}")
    c = collections.Counter()
    with open(f, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                b = json.loads(line).get("board")
            except Exception:
                continue
            if b:
                c[b] += 1
    mism = miss = 0
    for slug, n in c.items():
        e = led.get(f"{plat}:{slug}")
        if e is None:
            miss += 1
        elif int(e.get("rows") or 0) != n and e.get("last_ok"):
            mism += 1
    ghost = [k for k in led if k.startswith(plat + ":")
             and int((led[k] or {}).get("rows") or 0) > 0
             and k.split(":", 1)[1] not in c]
    print(f"{plat}: {sum(c.values()):,} rows / {len(c):,} boards on disk")
    print(f"  boards not in ledger            {miss:,}")
    print(f"  row-count mismatch (fetched)    {mism:,}")
    print(f"  ledger claims rows, file has 0  {len(ghost):,}")
    return 0 if not ghost else 1


def main():
    a = sys.argv[1:]
    if not a:
        sys.exit(__doc__)
    dry = "--dry-run" in a
    plats = None
    if "--platforms" in a:
        plats = [x for x in a[a.index("--platforms") + 1].split(",") if x]
        bad = [x for x in plats if x not in PLATFORMS]
        if bad:
            sys.exit(f"unknown platform(s) {bad}; choose from "
                     f"{', '.join(PLATFORMS)}")
    day = _today(a[a.index("--date") + 1]) if "--date" in a else _today()

    if "--bootstrap" in a:
        bootstrap(plats, dry=dry)
    elif "--plan" in a:
        cmd_plan(plats, day)
    elif "--cycle-check" in a:
        d = int(a[a.index("--days") + 1]) if "--days" in a else 10
        sys.exit(cmd_cycle_check(plats, d))
    elif "--verify" in a:
        sys.exit(cmd_verify(a[a.index("--verify") + 1]))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
