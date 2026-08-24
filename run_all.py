#!/usr/bin/env python3
"""
run_all.py — ONE rerunable entry point for every job pipeline: Getro, Lever,
and the FT+contract portals (Bullhorn OSCP, Ashby/SmartRecruiters/Recruitee/
Breezy/BambooHR, Phenom, marketplaces).  Ends by building every master CSV and
the single monolithic workbook All_Jobs_<date>.xlsx (all_jobs_build.py).

  python3 run_all.py                     refresh all jobs + rebuild everything
                                         (hours — the Getro re-fetch dominates)
  python3 run_all.py --build-only        no network; rebuild masters + monolith
                                         from the JSONL data already on disk
  python3 run_all.py --discover          also re-run board/tenant discovery
                                         first (Getro id sweep, Lever wayback +
                                         probe, Bullhorn token hunts — long)
  python3 run_all.py --resume            continue an interrupted refresh: keep
                                         state files + JSONLs so fetchers skip
                                         boards already done
  python3 run_all.py --only getro,lever  refresh only these families
  python3 run_all.py --skip portals      refresh all but these
  python3 run_all.py --dry-run           print the full plan, execute nothing

Families: getro · lever · portals.  The build stage always runs every builder
(each is an idempotent local transform), so masters stay consistent even when
only one family was refreshed.

Refresh semantics: fetchers append to their JSONL and skip boards marked done
in their *_fetch_state.json, so a true refresh must rotate first.  Unless
--resume is given, each selected family's JSONLs move to *.prev.jsonl (one
generation kept as a safety net) and its state files are deleted.  Stateless
appenders (boards_fetch, phenom_fetch) instead truncate their own output
before every attempt so retries can't duplicate rows.

Every step is supervised: up to 40 relaunches (each
fetcher resumes from its own per-item state), stdout+stderr appended to
logs/<step>.log, and the step's final summary line echoed to the console.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
NORM = os.path.join(BASE, "contract_norm")
LOGS = os.path.join(BASE, "logs")
PY = sys.executable or "python3"

FAMILIES = ("getro", "lever", "portals", "slugs", "rippling", "personio",
            "yc", "dover", "notion", "zohorecruit",
            # --- the corpus-mined tail, wired 2026-08-14 ---------------------
            # A family is the unit of ROTATION POLICY and user selection; a
            # chain is the unit of parallelism.  `portals` already fans out to
            # five chains, so grouping ~20 fetchers into four families by their
            # refresh policy follows the existing precedent.  Twenty-one
            # families would be twenty-one near-identical rows in rotate_family
            # — twenty-one chances to delete the wrong thing.
            "tail",    # tenants/*.csv driven; BOARD-keyed state; append-only.
                       # state_refresh drops the done keys so a re-run actually
                       # re-fetches (these have no refresh flag and silently
                       # no-op otherwise — how 8 sources sat 16 days stale).
            "remote",  # aggregators.  Their *_fetch_state.json holds CURSORS,
                       # not board keys, so state_refresh would erase the
                       # resume point (themuse's query watermark, jobgether's
                       # 4,465-company map = 8,977s to rebuild).  Resume only.
            # greenhouse and ashby were the gap this file documented in
            # refresh_gh_ashby.sh: the build tail ran greenhouse_build_sheet
            # and ashby_build_sheet, but NOTHING re-fetched their JSONLs, so a
            # bare run_all rebuilt whichever stale CSV was on disk and logged
            # "wrote Greenhouse_All_Jobs_<today>.xlsx" as a healthy step.  That
            # is 265,068 rows -- the third-largest input to the sheet -- held
            # correct only by the operator remembering to run a second script
            # first.  Refresh only: discovery is a separate --discover chain,
            # because new BOARDS are a Wayback crawl and new POSTINGS are not.
            "greenhouse",
            "ashby",
            "keyed",   # needs an API key present in the env to do anything.
            "intl")    # out of the US scope; reachable via --only, never
                       # default: arbeitsagentur keeps NO state file at all, so
                       # every run appends another complete copy.

# Families that exist for --only but are NOT in a bare `python3 run_all.py`.
DEFAULT_OFF = frozenset({"keyed", "intl"})
ENUM_SLACK = 3000  # --discover: sweep past the highest known collection id

# Per-state-file key scoping for state_refresh.  A file listed here has only
# the keys under these prefixes considered for re-fetch; everything else is
# kept verbatim.  See state_refresh.main_paths.
def _refresh_scopes():
    return {os.path.join(BASE, "atstail_fetch_state.json"):
            ("pinpoint:", "keka:", "join:", "taleo:", "wttj:")}


FETCH_TRIES = 40   # steps that resume from a state file
LOCAL_TRIES = 2    # local transforms / miners (a failure is deterministic)
RETRY_SLEEP = 3

_print_lock = threading.Lock()
_failed = {}       # family -> step name that failed
_discovery_failed = {}  # same, but for the best-effort discovery phase only:
                        # recorded and reported, never fatal (see main())


def say(msg):
    with _print_lock:
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}",
              flush=True)


def p(*parts):
    return os.path.join(BASE, *parts)


# ---------------------------------------------------------------- steps
def _rows(path):
    """Data rows in a CSV, ignoring the header.

    ats_mine_tenants.py writes the header even for a family with zero tenants,
    so os.path.exists() alone reports a 0-tenant family as ready and its
    fetcher starts against an empty list.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return max(0, sum(1 for _ in f) - 1)
    except OSError:
        return 0


class Step:
    def __init__(self, name, argv=None, env=None, needs=(), pre=None,
                 func=None, tries=FETCH_TRIES, soft_needs=(), ok_rc=(0,)):
        self.name = name          # log tag
        self.argv = argv          # subprocess argv (script + args), or None
        self.env = env or {}      # extra env vars
        self.needs = needs        # files that must exist before running
        self.pre = pre            # callable run before EVERY attempt
        self.func = func          # in-process callable instead of a subprocess
        self.tries = tries
        # Missing/empty -> NOOP and SUCCESS, unlike needs= which is a failure.
        # "This family has no tenants yet" is a normal state, not a broken run;
        # with needs= it aborts the chain and exits 1.
        self.soft_needs = soft_needs
        # Exit codes that mean "finished", not "retry".  Four fetchers exit
        # non-zero on PARTIAL success -- nicheboards `sys.exit(1 if failed)`,
        # smallremote `1 if bad`, usajobs `0 if n else 1`, adzuna `2` with no
        # key -- and at tries=40 each one becomes a retry storm.
        self.ok_rc = tuple(ok_rc)

    def describe(self):
        if self.func is not None:
            return f"<internal: {self.name}>"
        e = " ".join(f"{k}={v}" for k, v in self.env.items())
        return (e + " " if e else "") + " ".join(self.argv)


def run_step(step, tag, dry):
    if dry:
        say(f"[{tag}] DRY  {step.name}: {step.describe()}")
        return True
    for f in step.soft_needs:
        if _rows(f) < 1:
            say(f"[{tag}] NOOP {step.name}: no {os.path.basename(f)} yet "
                f"— nothing to fetch (not a failure)")
            return True
    for f in step.needs:
        if not os.path.exists(f):
            say(f"[{tag}] SKIP {step.name}: missing input "
                f"{os.path.basename(f)}")
            return False
    log_path = os.path.join(LOGS, step.name + ".log")
    t0 = time.time()
    for attempt in range(1, step.tries + 1):
        if step.pre:
            # Contained.  An OSError here (disk full — this box has hit 4 GiB)
            # used to propagate out of run_chain PAST done_event.set(), so the
            # run exited 0 with the output missing and every waiter on that
            # event blocked forever.
            try:
                step.pre()
            except OSError as e:
                say(f"[{tag}] FAIL {step.name} pre-hook: {e}")
                return False
        if step.func is not None:
            try:
                step.func()
                say(f"[{tag}] done {step.name} "
                    f"({time.time() - t0:.0f}s)")
                return True
            except Exception as e:  # noqa: BLE001 — supervised retry
                say(f"[{tag}] FAIL {step.name} attempt {attempt}: {e}")
                time.sleep(RETRY_SLEEP)
                continue
        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"=== [{step.name}] attempt {attempt} "
                      f"{datetime.now(timezone.utc).isoformat()} ===\n")
            log.flush()
            say(f"[{tag}] run  {step.name} (attempt {attempt}) "
                f"-> logs/{step.name}.log")
            env = dict(os.environ, PYTHONUNBUFFERED="1", **step.env)
            rc = subprocess.call([PY] + step.argv, cwd=BASE, env=env,
                                 stdout=log, stderr=subprocess.STDOUT)
            log.write(f"=== [{step.name}] attempt {attempt} exit {rc} ===\n")
        if rc in step.ok_rc:
            last = ""
            with open(log_path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("==="):
                        last = line
            if rc:
                say(f"[{tag}] note {step.name} exited {rc} — tolerated "
                    f"(partial-success exit code)")
            say(f"[{tag}] done {step.name} ({time.time() - t0:.0f}s)"
                + (f"  | {last[:110]}" if last else ""))
            return True
        say(f"[{tag}] exit {rc} from {step.name} attempt {attempt}; "
            f"retrying in {RETRY_SLEEP}s")
        time.sleep(RETRY_SLEEP)
    say(f"[{tag}] GIVING UP on {step.name} after {step.tries} attempts "
        f"(see logs/{step.name}.log)")
    return False


class Gate:
    """Weighted admission gate over chain threads.

    A Semaphore cannot express this: the cost of a chain is the number of
    outbound worker threads its SUBPROCESS opens, not one slot.  workday alone
    runs 64 (WD_WORKERS), atstail 48, oracle 24, icims 20 — so counting chains
    would let a dozen "slots" open 200+ sockets from one IP, which is how
    apply.workable.com earned its durable per-IP penalty in the first place.

    Weight 0 (a tag absent from WEIGHT) is UNMETERED and never blocks.
    """

    def __init__(self, cap):
        self.cap = max(1, cap)
        self.free = self.cap
        self.cv = threading.Condition()

    def _w(self, w):
        # Clamped: a chain heavier than the whole cap runs ALONE rather than
        # waiting forever for a permit that can never be granted.
        return min(max(0, int(w)), self.cap)

    def acquire(self, w):
        w = self._w(w)
        if not w:
            return
        with self.cv:
            while self.free < w:
                self.cv.wait()
            self.free -= w

    def release(self, w):
        w = self._w(w)
        if not w:
            return
        with self.cv:
            self.free += w
            self.cv.notify_all()


# Outbound worker threads each chain's subprocess opens.  DELIBERATELY EMPTY
# for now: every existing chain weighs 0 and is unmetered, so the gate is a
# provable no-op until the tail families are wired and given weights.  Bounding
# the 13 chains that already work would be a behaviour change to working code.
WEIGHT = {
    # The 13 pre-existing chains are deliberately ABSENT (weight 0, unmetered):
    # bounding code that already works would be a behaviour change, and getro
    # runs for hours anyway, so the tail fits inside its shadow.
    "workday": 64, "atstail-rest": 48, "oracle": 24, "icims": 20, "adp": 12,
    "eightfold": 10, "paycom": 8, "jobvite": 8, "gem": 8, "comeet": 8,
    "themuse": 6, "remoteleaf": 6, "web3career": 5, "careerjet": 4,
    "jobgether": 4, "nicheboards": 1,
}
GATE = Gate(int(os.environ.get("RUN_ALL_CONC") or 96))


def gated(tag, steps, dry, wait_event=None, done_event=None):
    """Thread body: optional dependency wait, then a weighted permit.

    THE RULE: wait on the Event BEFORE acquiring, never while holding a permit.
    A chain that waits for its producer while holding capacity the producer
    needs is a deadlock, and the release must be in a finally — a leaked permit
    starves every remaining chain for the life of the run.
    """
    def _run():
        if wait_event is not None:
            wait_event.wait()
        w = WEIGHT.get(tag, 0)
        GATE.acquire(w)
        try:
            run_chain(tag, steps, dry, done_event=done_event)
        finally:
            GATE.release(w)
    return _run


def run_chain(tag, steps, dry, done_event=None, fail_soft=False):
    """Run steps in order.  Fail-fast by default: a failed step aborts the rest
    of the chain, because a fetcher's downstream steps have nothing to chew on.

    fail_soft=True is for the build tail, where every step is an INDEPENDENT
    local transform.  There, one family's missing JSONL must not stop the other
    builders — and above all must not stop all_jobs_build, which is the only
    step that produces all_jobs_master.csv.  Without this, a single family that
    has never been fetched (or whose JSONL was rotated away by an interrupted
    run) silently costs you the entire monolith.
    """
    ok = True
    try:
        for st in steps:
            if not run_step(st, tag, dry):
                # Accumulate.  This was `_failed[tag] = st.name`, so a chain
                # that lost two steps reported only the last one.
                prev = _failed.get(tag)
                _failed[tag] = f"{prev},{st.name}" if prev else st.name
                ok = False
                if not fail_soft:
                    break
    except Exception as e:  # noqa: BLE001 — a chain must never take the run down
        _failed[tag] = f"{tag}:{type(e).__name__}"
        say(f"[{tag}] CHAIN ABORTED: {type(e).__name__}: {e}")
        ok = False
    finally:
        # ALWAYS, even on an exception.  Anything waiting on this event would
        # otherwise block until the process is killed.
        if done_event:
            done_event.set()
    return ok


# ---------------------------------------------------------------- helpers
def truncate(*paths):
    def _t():
        for q in paths:
            open(q, "w").close()
    return _t


def rotate_family(fam, dry):
    # .get(fam, []) rather than [fam]: an unguarded lookup turns "somebody
    # added a family and forgot one of these dicts" into a KeyError crash
    # mid-run instead of a harmless no-op rotation.
    jsonls = {
        "getro": [p("getro_all_jobs.jsonl")],
        "lever": [p("lever_all_jobs.jsonl")],
        # The eight ats_all_*.jsonl are deliberately NOT rotated here any more.
        # ats_fetch owns its own generation as of 2026-08-16: it stages this
        # run's rows, rewrites each live file board-scoped (keeping every row
        # whose board it did not authoritatively re-fetch) and writes .prev
        # itself immediately before the atomic install.  Rotating here would
        # hand it an empty live file, so the board-scoped merge would have
        # nothing to preserve — which is exactly the hole backfill_from_prev()
        # existed to paper over.
        "portals": [os.path.join(NORM, x) for x in (
            "boards_all.jsonl", "phenom_all.jsonl", "bullhorn_all.jsonl")],
        "rippling": [p("rippling_all_jobs.jsonl")],
        "personio": [p("personio_all_jobs.jsonl")],
        "yc": [p("yc_all_jobs.jsonl")],
        "dover": [p("dover_all_jobs.jsonl")],
        "notion": [p("notion_all_jobs.jsonl")],
        "greenhouse": [p("greenhouse_all_jobs.jsonl")],
        "ashby": [p("ashby_all_jobs.jsonl")],
        # slugs emits candidate lists, not job rows.
        "slugs": [],
        # Deliberately EMPTY.  zohorecruit is one of six families sharing
        # contract_norm/atstail.jsonl, so rotating that file here would
        # discard the other five (taleo, pinpoint, keka, wttj, join).  The
        # fetcher does its own scoped refresh instead — ATSTAIL_REFRESH
        # rewrites the JSONL keeping every row that is not this family's.
        "zohorecruit": [],
    }.get(fam, [])
    states = {
        "getro": [p("getro_fetch_state.json"), p("getro_pdl_state.json")],
        "lever": [p("lever_fetch_state.json")],
        # ats_all_fetch_state.json is still NOT rotated, but the reason has
        # changed.  It used to be the coverage accumulator for a ~70,500-board
        # backlog no single run could drain: deleting it each run made every run
        # re-reach the same first 25k and leave 45,701 boards never fetched.
        #
        # That backlog finished draining, and the file then became a permanent
        # veto — all 71,592 keys read "done", ats_fetch skipped every one of
        # them, and the seven-platform chain fetched 88 rows on 2026-08-16 while
        # its JSONLs came out byte-identical to their .prev copies.
        #
        # It is not rotated now because expiry is no longer all-or-nothing.
        # ats_ledger decides per board, by measured yield: hot every run, warm
        # 1-in-3, cold 1-in-10, retired never — ~20k boards a run, full coverage
        # in ten.  Deleting the file would still be wrong, and worse than
        # before: it also feeds coverage_report's boards_known denominator, so
        # dropping keys would silently corrupt the coverage percentage rather
        # than just re-crawling.
        "portals": [p("bh_all_fetch_state.json")],
        "rippling": [p("rippling_fetch_state.json")],
        "personio": [p("personio_fetch_state.json")],
        "yc": [p("yc_fetch_state.json")],
        "dover": [p("dover_fetch_state.json")],
        "notion": [p("notion_fetch_state.json")],
        # Rotating the state is what makes the fetch a REFRESH rather than a
        # resume.  gh_run_all.sh without rotation fetched 65 of 8,441 boards on
        # 2026-08-14 -- it resumed a finished run and called it done.
        "greenhouse": [p("greenhouse_fetch_state.json")],
        "ashby": [p("ashby_fetch_state.json")],
        # Deliberately EMPTY.  slug_discovery_state.json holds Wayback CDX
        # pagination that costs hours to rebuild, and the probe-results CSVs
        # hold the dead/retired censuses — none of that should be discarded by
        # an ordinary refresh.  Re-crawl explicitly with --discover instead.
        "slugs": [],
        # Also EMPTY: atstail_fetch_state.json is keyed "{family}:{tenant}"
        # for all six families, so deleting it would re-fetch the other five
        # from scratch.  ATSTAIL_REFRESH drops only the zohorecruit:* keys.
        "zohorecruit": [],
    }.get(fam, [])
    # A THIRD policy, for the tail families: do NOT rotate the JSONL, DO drop
    # the state's "done" keys.  These fetchers append and are collapsed
    # downstream by bh_build_combined on (source, board, job_id), so rotating
    # their output would be actively destructive — they would skip everything
    # still marked done and write a near-empty generation, with no
    # backfill_from_prev() to save them.  What they need is the opposite:
    # forget that the LIVE tenants were done, keep the corpse census.
    refresh_states = {
        "tail": [p(f"{n}_fetch_state.json") for n in (
            "workday", "oracle", "icims", "adp", "paycom", "eightfold",
            "jobvite", "gem", "comeet",
            )]
        # THE 2026-08-20 FAMILIES ARE DELIBERATELY NOT IN THAT LIST.
        # They are append-only and three of them (manatal, dayforce,
        # cornerstone) have no board-scoped merge, so dropping their done keys
        # would re-fetch every board and APPEND a second copy of every row.
        # bh_build_combined dedupes on (source, board, job_id) so the sheet
        # would survive it, but the JSONLs would grow without bound.
        #
        # Not refreshing costs nothing yet: none of them has finished its FIRST
        # pass -- coverage_report reads manatal 25 of 3,670 boards reached,
        # dayforce 55 of 4,556, neogov 130 of 3,010 -- so every run still has
        # thousands of never-fetched boards to drain, and the 60-minute budget
        # is what paces that.  Add them here once they have drained AND carry a
        # board-scoped merge, the same order ats_fetch went through.
        + []
        # atstail, SCOPED to the five families run_all drives as atstail-rest.
        # It was omitted entirely, and the cost was exact: 5,353 keys with no
        # refresh path, so 2026-08-20 ran "pinpoint: 0 pending, keka: 1,
        # join: 1, taleo: 0, wttj: 0 pending" and finished in two seconds
        # while 94,742 rows went stale.  It cannot be refreshed whole --
        # the file is keyed "{family}:{tenant}" and zohorecruit's keys belong
        # to ATSTAIL_REFRESH -- hence the prefix scope below.
        + [p("atstail_fetch_state.json")],
        # DELIBERATELY EMPTY — and this is the sharpest boundary in the file.
        # themuse/careerjet/jobgether/remoteleaf/web3career keep CURSORS in the
        # *_fetch_state.json filename shape: themuse a 539 KB query watermark,
        # jobgether a 4,465-company partition map that costs 8,977s to rebuild,
        # web3career next_page, remoteleaf its sitemap position.  state_refresh
        # drops every key that does not look dead, so running it here erases
        # the resume point and the next run re-crawls from scratch.  These
        # families refresh by RESUMING, not by forgetting.
        "remote": [],
        "keyed": [],
        "intl": [],
    }.get(fam, [])
    if refresh_states and not dry:
        import state_refresh
        for q in refresh_states:
            if not os.path.exists(q):
                continue
            # One generation of rollback.  state_refresh writes <state>.full
            # only if absent, so that file freezes at its first-ever run and is
            # not a rollback point; this one is rewritten every time.
            try:
                shutil.copy2(q, q + ".prev.json")
            except OSError as e:
                say(f"[rotate] WARN could not back up {os.path.basename(q)}: {e}")
        say(f"[rotate] state_refresh {len(refresh_states)} {fam} state file(s) "
            f"— dropping done keys, keeping the dead census")
        state_refresh.main_paths(refresh_states, scopes=_refresh_scopes())
    elif refresh_states:
        say(f"[rotate] DRY state_refresh {len(refresh_states)} {fam} "
            f"state file(s)")
    # The posted-date ledgers (rippling/personio/yc/dover/notion_posted
    # _ledger.json)
    # are never rotated: they exist precisely so a first-seen date survives
    # every refresh.  Deleting one would silently re-date every posting to the
    # day of the next run.
    for q in jsonls:
        if os.path.exists(q):
            prev = q[:-len(".jsonl")] + ".prev.jsonl"
            say(f"[rotate] {os.path.basename(q)} -> {os.path.basename(prev)}"
                + (" (dry)" if dry else ""))
            if not dry:
                os.replace(q, prev)
    for q in states:
        if os.path.exists(q):
            say(f"[rotate] delete {os.path.basename(q)}"
                + (" (dry)" if dry else ""))
            if not dry:
                os.remove(q)


def max_collection_id():
    mx = 0
    path = p("getro_all_collections.jsonl")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    mx = max(mx, int(json.loads(line).get("id", 0)))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
    return mx


def merge_bh_tokens():
    """Rebuild bh_tokens_merged.txt from its three sources (the original was
    assembled by hand; this makes the merge rerunable)."""
    pairs = set()
    for name in ("bh_all_tokens.txt", "bh_final_tokens.txt"):
        q = p(name)
        if os.path.exists(q):
            with open(q, encoding="utf-8") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) == 2:
                        pairs.add((parts[0], parts[1]))
    q = p("bh_github_tokens.json")
    if os.path.exists(q):
        with open(q, encoding="utf-8") as f:
            for key in json.load(f):
                sw, _, tok = key.partition(":")
                if sw and tok:
                    pairs.add((sw, tok))
    if not pairs:
        raise RuntimeError("no bullhorn token sources found")
    with open(p("bh_tokens_merged.txt"), "w", encoding="utf-8") as f:
        for sw, tok in sorted(pairs):
            f.write(f"{sw} {tok}\n")
    print(f"bh_tokens_merged.txt: {len(pairs)} (swimlane, token) pairs",
          flush=True)


def ensure_bh_tokens():
    if not os.path.exists(p("bh_tokens_merged.txt")):
        merge_bh_tokens()


# ---------------------------------------------------------------- plan
def build_plan(args, fams):
    """Returns (discover_chains, refresh_chains, tail_steps).  Chains are
    (tag, [Step...]) run in parallel; the ats chain is keyed to start after
    the getro chain via events wired in main()."""
    discover, refresh = [], []
    # One id per INVOCATION, shared by every supervised attempt of a step.
    # ats_fetch uses it to take .prev once per run rather than once per attempt.
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.discover:
        if "getro" in fams:
            end = max_collection_id() + ENUM_SLACK
            discover.append(("getro-disc", [Step(
                "getro_enumerate_boards",
                ["getro_enumerate_boards.py", "1", str(end),
                 "getro_all_collections.jsonl"])]))
        if "lever" in fams:
            discover.append(("lever-disc", [
                Step("lever_discover_boards", ["lever_discover_boards.py"],
                     needs=(p("getro_all_jobs.jsonl"),)),
                Step("lever_probe_boards", ["lever_probe_boards.py"],
                     needs=(p("lever_candidates.csv"),)),
            ]))
        if "portals" in fams:
            discover.append(("bh-disc", [
                Step("bh_github_discover", ["bh_github_discover.py"],
                     tries=LOCAL_TRIES),
                Step("bh_extract_tokens", ["bh_extract_tokens.py"],
                     needs=(p("bh_urlscan_domains.txt"),)),
                Step("bh_merge_tokens", func=merge_bh_tokens,
                     tries=LOCAL_TRIES),
            ]))
        # The shared Wayback slug sweep.  It emits <fam>_candidates.csv for
        # rippling/personio AND wayback_ats_slugs.json for ats_fetch.py, so
        # main() gates those chains on it finishing.
        if "slugs" in fams:
            discover.append(("slugs", [
                Step("slug_discover_boards", ["slug_discover_boards.py"]),
            ]))
        # Parses the workday-jobs / workday-site rows the slug sweep just
        # cached, probes the new (tenant, pod, site) triples through the CXS
        # API, and merges the live ones into tenants/workday.csv.  It MUST
        # wait on the sweep -- it reads slug_wayback_urls.txt -- hence the
        # entry in `waiters` below.
        if "slugs" in fams:
            discover.append(("workday-disc", [
                Step("workday_discover_boards", ["workday_discover_boards.py"],
                     tries=2, ok_rc=(0, 1)),
            ]))
        if "rippling" in fams:
            discover.append(("rippling-disc", [
                Step("rippling_probe_boards", ["rippling_probe_boards.py"],
                     needs=(p("rippling_candidates.csv"),)),
            ]))
        if "notion" in fams:
            discover.append(("notion-disc", [
                Step("notion_discover_boards", ["notion_discover_boards.py"]),
                Step("notion_probe_boards", ["notion_probe_boards.py"],
                     needs=(p("notion_candidates.csv"),)),
            ]))
        if "dover" in fams:
            discover.append(("dover-disc", [
                Step("dover_discover_boards", ["dover_discover_boards.py"]),
                Step("dover_probe_boards", ["dover_probe_boards.py"],
                     needs=(p("dover_candidates.csv"),)),
            ]))
        # Board discovery for the two families whose fetch just moved into
        # run_all.  Reachable only via --discover on purpose: these are
        # multi-hour Wayback CDX crawls that find new BOARDS, and new POSTINGS
        # come from re-fetching the boards already known -- which is exactly
        # why refresh_gh_ashby.sh skipped them too.
        if "greenhouse" in fams:
            discover.append(("gh-disc", [
                Step("greenhouse_discover_boards",
                     ["greenhouse_discover_boards.py"]),
                Step("greenhouse_probe_boards",
                     ["greenhouse_probe_boards.py"],
                     needs=(p("greenhouse_candidates.csv"),)),
            ]))
        if "ashby" in fams:
            discover.append(("ashby-disc", [
                Step("ashby_discover_boards", ["ashby_discover_boards.py"]),
            ]))
        # Board discovery for the 2026-08-20 families.  Kept on --discover
        # only, like every other discovery chain: these enumerate BOARDS
        # (Wayback CDX, vendor directories, sitemaps) and take hours, while new
        # POSTINGS come from re-fetching the boards already known.
        #
        # Worth running though -- the first pass found far more than the corpus
        # had ever revealed: manatal 3,670 boards against 606 employers visible
        # in the master, dayforce 4,556 against 129, neogov 3,010 against 159.
        if "tail" in fams:
            for _tag, _script in (("ukg", "ukg_discover_boards.py"),
                                  ("manatal", "manatal_discover_boards.py"),
                                  ("dayforce", "dayforce_discover_boards.py"),
                                  ("neogov", "neogov_discover_boards.py"),
                                  ("radancy", "radancy_discover_boards.py"),
                                  ("phenom", "phenom_discover_boards.py")):
                discover.append((_tag + "-disc", [
                    Step(_script[:-3], [_script], tries=2, ok_rc=(0, 1)),
                ]))
        if "yc" in fams:
            discover.append(("yc-disc", [
                Step("yc_discover_boards", ["yc_discover_boards.py"]),
            ]))

    if "getro" in fams:
        refresh.append(("getro", [
            Step("getro_check_live",
                 ["getro_check_live.py", "getro_all_collections.jsonl"],
                 needs=(p("getro_all_collections.jsonl"),),
                 tries=LOCAL_TRIES),
            Step("getro_fetch_jobs", ["getro_fetch_jobs.py"],
                 needs=(p("getro_job_boards.csv"),)),
            Step("getro_fetch_pdl", ["getro_fetch_pdl.py"]),
            Step("contract_mine_getro", ["contract_mine_getro.py"],
                 needs=(p("getro_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        ]))
    if "lever" in fams:
        refresh.append(("lever", [
            Step("lever_fetch_jobs", ["lever_fetch_jobs.py"],
                 needs=(p("lever_job_boards.csv"),)),
            Step("contract_mine_lever", ["contract_mine_lever.py"],
                 needs=(p("lever_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        ]))
    if "rippling" in fams:
        refresh.append(("rippling", [
            Step("rippling_fetch_jobs", ["rippling_fetch_jobs.py"],
                 needs=(p("rippling_job_boards.csv"),)),
        ]))
    if "zohorecruit" in fams:
        # atstail_fetch.py multiplexes six families; ATSTAIL_ONLY scopes the
        # run and ATSTAIL_REFRESH re-fetches this one from scratch without
        # disturbing the other five (see rotate_family above).  The tenant
        # list is mined from the corpus by ats_mine_tenants.py — it is what
        # took this family from 30 boards to 260.  That step now lives in the
        # "prelude" chain (it produces the tenant lists for the whole tail, not
        # just this family) and this chain waits on tenants_ready, so running
        # it here too would repeat a 3-minute scan of the 2.2M-row master.
        refresh.append(("zohorecruit", [
            Step("zoho_fetch", ["atstail_fetch.py"],
                 env={"ATSTAIL_ONLY": "zohorecruit",
                      "ATSTAIL_REFRESH": "zohorecruit"},
                 needs=(os.path.join(BASE, "tenants", "zohorecruit.csv"),)),
        ]))
    if "personio" in fams:
        # personio_fetch_jobs doubles as the probe (see its docstring): the
        # host rate-limits hard enough that a separate probe pass would double
        # the load for no new information.
        refresh.append(("personio", [
            Step("personio_fetch_jobs", ["personio_fetch_jobs.py"],
                 needs=(p("personio_candidates.csv"),)),
        ]))
    if "yc" in fams:
        refresh.append(("yc", [
            Step("yc_fetch_jobs", ["yc_fetch_jobs.py"],
                 needs=(p("yc_candidates.csv"),)),
        ]))
    if "notion" in fams:
        refresh.append(("notion", [
            Step("notion_fetch_jobs", ["notion_fetch_jobs.py"],
                 needs=(p("notion_job_boards.csv"),)),
        ]))
    if "dover" in fams:
        refresh.append(("dover", [
            Step("dover_fetch_jobs", ["dover_fetch_jobs.py"],
                 needs=(p("dover_job_boards.csv"),)),
        ]))
    # Fetch only.  ~8,400 greenhouse boards and ~3,650 ashby boards, both
    # resumable, both independent of everything else in the run -- so they get
    # their own chains and drain in parallel with the rest.  tries mirrors
    # refresh_gh_ashby.sh's retry loop, bounded by ok_rc so a partial fetch
    # does not fail the run.
    if "greenhouse" in fams:
        refresh.append(("greenhouse", [
            Step("greenhouse_fetch_jobs", ["greenhouse_fetch_jobs.py"],
                 tries=3, ok_rc=(0, 1))]))
    if "ashby" in fams:
        refresh.append(("ashby", [
            Step("ashby_fetch_jobs", ["ashby_fetch_jobs.py"],
                 tries=3, ok_rc=(0, 1))]))
    if "portals" in fams:
        # BOARDS_KEEP_ALL=1: without it every remote board is filtered to its
        # contingent slice, which is why 8 remote boards contributed 256 rows
        # to the master while their APIs carry tens of thousands of postings.
        # Output goes to boards_all.jsonl, so the contract-only boards.jsonl
        # the legacy Contract_Gigs workbooks read stays untouched.
        refresh.append(("boards", [
            Step("boards_fetch", ["boards_fetch.py"],
                 env={"BOARDS_KEEP_ALL": "1"},
                 pre=truncate(os.path.join(NORM, "boards_all.jsonl")),
                 tries=3),
        ]))
        refresh.append(("phenom", [
            Step("phenom_fetch", ["phenom_fetch.py"],
                 # 25 min is ~4x headroom: the full 127-board run measures
                 # 384s at RPS=6/WORKERS=8.  Boards not reached inside the
                 # budget stay PENDING rather than being recorded empty.
                 env={"PHENOM_KEEP_ALL": "1", "PHENOM_BUDGET_MIN": "25"},
                 pre=truncate(os.path.join(NORM, "phenom_all.jsonl")),
                 tries=3),
        ]))
        refresh.append(("bullhorn", [
            Step("bh_ensure_tokens", func=ensure_bh_tokens,
                 tries=LOCAL_TRIES),
            Step("bh_fetch", ["bh_fetch.py"], env={"BH_KEEP_ALL": "1"},
                 needs=(p("bh_tokens_merged.txt"),)),
        ]))
        # ats_fetch needs the slug list contract_mine_getro emits, so it is
        # wired in main() to start once the getro chain finishes (or at once
        # when getro isn't being refreshed this run).
        #
        # workable is deliberately EXCLUDED here and given its own chain below.
        # apply.workable.com applies a durable PER-IP penalty (see the long
        # note in ats_fetch.RPS): once tripped, every workable board — live or
        # dead — costs a full 43s of TRIES exhaustion before raising, which
        # measured out at 26% of the queue dragging the whole run from ~15
        # boards/s to ~1.  Splitting it means one blocked host can no longer
        # starve the other seven.
        refresh.append(("ats", [
            Step("ats_fetch", ["ats_fetch.py"],
                 env={"ATS_KEEP_ALL": "1",
                      "ATS_PLATFORMS": "ashby,smartrecruiters,recruitee,"
                                       "breezy,bamboohr,jazzhr,teamtailor",
                      # Identifies the RUN so .prev is taken once, not once per
                      # supervised attempt — otherwise attempt 2 overwrites the
                      # pre-run generation with attempt 1's output.
                      "ATS_RUN_ID": run_id,
                      # Pinned OFF rather than left to the board-count switch in
                      # ats_fetch.  That switch flips per-job detail back ON
                      # below JZ_DETAIL_MAX_BOARDS=3000 pending jazzhr boards,
                      # and tiering deliberately puts the queue there (~1,146
                      # hot + a warm slice).  Detail turns each live board into
                      # 1 + up to 80 requests at 6 rps: ~75,000 requests, ~3.5h,
                      # against a 25-minute budget.
                      "JZ_NO_DETAIL": "1",
                      # Delisting, in the NON-DESTRUCTIVE mode.  Append-only
                      # merging ran at +78-81% per platform per run with
                      # "dropped 0", and retired:0 across all 71,659 ledger
                      # boards -- so nothing has ever left this corpus and a
                      # growing share of it is postings you cannot apply to.
                      # mode=flag keeps every row and stamps delisted_on, so
                      # the row count does not fall, MAX_SHRINK cannot trip,
                      # and the lanes get to sort on truth.  Enabling all
                      # platforms at once is safe HERE only because flag mode
                      # deletes nothing; ATS_DELIST_MODE=drop must still be
                      # rolled out one platform at a time.
                      "ATS_DELIST": "all",
                      "ATS_DELIST_MODE": "flag",
                      # ats_fetch defaults BUDGET_MIN to 25 and run_all never
                      # overrode it, so 2026-08-19 stopped at
                      # "13769/20560 boards, 6791 still pending" -- ~20k rows
                      # a run at the ledger's 7.1 rows/board warm yield.  It
                      # was not competing for wall-clock with anything: this
                      # chain ended 01:48 and the build tail could not start
                      # until boards_fetch finished at 02:37.  74 idle minutes.
                      "ATS_BUDGET_MIN": "90"},
                 needs=(p("getro_ats_slugs.json"),),
                 # 40 attempts is what turned 32 permanently-broken boards into
                 # 2h16m of blocked build tail on 2026-08-16.  ats_fetch now
                 # retires a board after 3 consecutive failed runs, so the cause
                 # is gone; tries=3 with ok_rc=(0,1) is the seatbelt that bounds
                 # any future storm to a single attempt.  ok_rc alone would be
                 # wrong — the exit-1 is deliberate (it is what makes 429'd
                 # boards get revisited), so it is tolerated, not removed.
                 tries=3, ok_rc=(0, 1)),
        ]))
        refresh.append(("ats-workable", [
            Step("ats_fetch_workable", ["ats_fetch.py"],
                 env={"ATS_KEEP_ALL": "1", "ATS_PLATFORMS": "workable",
                      "ATS_RPS": "workable:2", "ATS_CONC": "workable:4",
                      # Its OWN state file.  Both chains run in parallel and
                      # each saves the whole dict at exit, so sharing one file
                      # meant the later finisher erased the other's completion
                      # record entirely.  See the ATS_STATE note in ats_fetch.
                      "ATS_STATE": "ats_all_workable_state.json",
                      # ...and its own ledger, for the identical reason.
                      "ATS_LEDGER": "ats_workable_yield_ledger.json",
                      "ATS_RUN_ID": run_id,
                      # 320 workers against a Semaphore(4) gate is what made
                      # this step cost 10,757s.  The budget stops SUBMITTING,
                      # but the executor still drains every started future at
                      # 43s each: 320/4 x 43s = ~57 min per attempt, twice.
                      # 8 workers caps that drain at ~86s.
                      "ATS_WORKERS": "8",
                      # Bounded: if the IP is still penalised this exits on the
                      # budget instead of spending hours proving it.
                      #
                      # 30 -> 90.  At 30 the last run managed
                      # "604/16837 boards fetched, 16233 still pending" and
                      # only 4,550 of ~21,371 known slugs are settled -- ~128k
                      # rows unfetched, and 112 minutes of the run's wall-clock
                      # were idle.  Do NOT read this as licence to go higher
                      # without measuring: apply.workable.com is ONE shared
                      # Cloudflare host with a durable per-IP penalty (99
                      # rate-limit backoffs last run), so throughput does not
                      # scale linearly with the budget.  Check boards/min at
                      # 60 before considering 120.
                      "ATS_BUDGET_MIN": "90",
                      "ATS_DELIST": "all",
                      "ATS_DELIST_MODE": "flag"},
                 needs=(p("getro_ats_slugs.json"),), tries=2),
        ]))

    # ---- the corpus-mined tail ------------------------------------------
    # ats_mine_tenants.py is the SOLE producer of all 17 tenants/*.csv, and it
    # reads all_jobs_master.csv (~3 min).  It used to be buried inside the
    # zohorecruit chain; every tail chain needs its output, so it is hoisted
    # into a serial prelude and the tail chains wait on it.
    if ({"tail", "zohorecruit"} & set(fams)):
        refresh.append(("prelude", [
            Step("ats_mine_tenants", ["ats_mine_tenants.py"],
                 needs=(p("all_jobs_master.csv"),), tries=LOCAL_TRIES),
        ]))

    def T(fam):
        return os.path.join(BASE, "tenants", fam + ".csv")

    if "tail" in fams:
        # soft_needs, not needs: "this family has no tenants yet" is a normal
        # state.  With needs= it returns False, which run_chain reads as a
        # failure and the whole run exits 1.
        for tag, script, env, tenants in (
                # WD_DEDUPE=1 was here and made this step a NO-OP: it is the
                # first branch of workday_fetch.main() and returns before
                # load_boards() is ever called, so every run re-deduped the
                # same 648,692 rows in 16s, exited 0, and looked healthy while
                # fetching nothing.  WD_BUDGET_MIN next to it was dead config
                # — unreachable — which is the tell that a fetch was intended.
                # Dedupe is a separate maintenance mode; run it on its own.
                ("workday", "workday_fetch.py",
                 {"WD_BUDGET_MIN": "45"}, "workday"),
                ("oracle", "oracle_fetch.py", {}, "oracle"),
                ("icims", "icims_fetch.py", {}, "icims"),
                ("adp", "adp_fetch.py", {}, "adp"),
                ("paycom", "paycom_fetch.py", {}, "paycom_path"),
                ("eightfold", "eightfold_fetch.py", {}, "eightfold"),
                ("jobvite", "jobvite_fetch.py", {}, "jobvite"),
                ("gem", "gem_fetch.py", {}, "gem"),
                ("comeet", "comeet_fetch.py", {}, "comeet"),
                # --- 2026-08-20: the families host_census.py surfaced -------
                # Same shape as the rest of the tail: tenants/<fam>.csv driven,
                # board-keyed state, append-only.
                #
                # BUDGET_MIN=60 each.  Discovery found far more boards than
                # any of these has ever fetched -- manatal 3,670, dayforce
                # 4,556, neogov 3,010 -- so an unbounded first run would stall
                # the build tail for most of a day.  They are board-keyed and
                # resumable, so a budget means "drain over several nights",
                # which is exactly how workday (45) and workable (90) are
                # already handled.  They run in parallel, so wall-clock is the
                # slowest one, not the sum.
                #
                # KEEP_ALL IS NOT OPTIONAL.  radancy's defaults to OFF
                # (radancy_fetch.py tests == "1"), so WITHOUT this env the
                # fetcher silently writes only its contingent slice -- the
                # exact failure that left 8 remote boards contributing 256 rows
                # to the master until BOARDS_KEEP_ALL was found.  The other
                # five default ON; they are passed explicitly anyway so the
                # plan line shows what the run is actually asking for.
                ("ukg", "ukg_fetch.py",
                 {"UKG_KEEP_ALL": "1", "UKG_BUDGET_MIN": "60"}, "ukg"),
                ("manatal", "manatal_fetch.py",
                 {"MANATAL_KEEP_ALL": "1",
                  "MANATAL_BUDGET_MIN": "60"}, "manatal"),
                ("dayforce", "dayforce_fetch.py",
                 {"DAYFORCE_KEEP_ALL": "1",
                  "DAYFORCE_BUDGET_MIN": "60"}, "dayforce"),
                ("neogov", "neogov_fetch.py",
                 {"NEOGOV_KEEP_ALL": "1",
                  "NEOGOV_BUDGET_MIN": "60"}, "neogov"),
                ("radancy", "radancy_fetch.py",
                 {"RADANCY_KEEP_ALL": "1",
                  "RADANCY_BUDGET_MIN": "60"}, "radancy"),
                ("cornerstone", "cornerstone_fetch.py",
                 {"CORNERSTONE_KEEP_ALL": "1",
                  "CORNERSTONE_BUDGET_MIN": "60"}, "cornerstone")):
            refresh.append((tag, [
                # Same seatbelt the ats chain carries.  FETCH_TRIES=40 against a
                # permanently-broken tenant is not a retry, it is a stall: on
                # 2026-08-19 icims banked 74,715 rows on attempt 1, exited 1,
                # and then spent 45 minutes printing "0 rows" 39 more times
                # against the same 4 broken tenants (2x HTTP 500, one 404, one
                # SSLError).  It is also why the run reported FAILED.  ok_rc
                # tolerates the deliberate exit-1 (it is what makes throttled
                # boards get revisited) without removing it.
                Step(tag + "_fetch", [script], env=env,
                     soft_needs=(T(tenants),), tries=3, ok_rc=(0, 1))]))
        # The five atstail families run_all does not already drive as
        # zohorecruit.  No ATSTAIL_REFRESH: that rewrites the shared JSONL to
        # drop a family's rows before re-fetching, so a family that has started
        # refusing loses its rows outright.
        refresh.append(("atstail-rest", [
            Step("atstail_fetch_rest", ["atstail_fetch.py"],
                 env={"ATSTAIL_ONLY": "pinpoint,keka,join,taleo,wttj",
                      "ATSTAIL_BUDGET_MIN": "0"},
                 soft_needs=(T("pinpoint"),), tries=3, ok_rc=(0, 1))]))

    if "remote" in fams:
        # Budgets are MANDATORY and the two conventions collide:
        #   float(env or D) -- "0" is truthy, so 0 is a ZERO-SECOND deadline
        #                      (THEMUSE_BUDGET, CJ_DEADLINE, W3_*, RL_*)
        #   int(env or 0)   -- 0 means UNLIMITED (JG_MAX_MIN, *_BUDGET_MIN)
        # Leaving JG_MAX_MIN unset is what let jobgether run 8,977s.
        refresh.append(("themuse", [
            Step("themuse_fetch", ["themuse_fetch.py"],
                 env={"THEMUSE_BUDGET": "5400"}, tries=3)]))
        refresh.append(("careerjet", [
            Step("careerjet_fetch", ["careerjet_fetch.py"],
                 env={"CJ_DEADLINE": "3600"}, tries=3)]))
        refresh.append(("jobgether", [
            Step("jobgether_fetch", ["jobgether_fetch.py"],
                 env={"JG_MAX_MIN": "90"}, tries=3)]))
        refresh.append(("remoteleaf", [
            Step("remoteleaf_fetch", ["remoteleaf_fetch.py"],
                 env={"RL_MAX_SECONDS": "1800"}, tries=3)]))
        # ok_rc + low tries: it exits `1 if failed` whenever ANY of its nine
        # sources fails, which is the steady state (cryptojobslist 403,
        # vuejobs 404).  At tries=40 that was 40 full re-crawls of the healthy
        # seven.  It no longer deletes its output, so a tolerated exit 1 is
        # simply "seven of nine worked".
        refresh.append(("nicheboards", [
            Step("nicheboards_fetch", ["nicheboards_fetch.py"],
                 tries=2, ok_rc=(0, 1))]))
        # Listing-only by default.  The phase-2 apply-url resolver managed
        # 1,200 of 17,798 urls in 1,695s against constant 429s -- it cannot
        # finish, and it spends the single-IP budget every night.
        refresh.append(("web3career", [
            Step("web3career_fetch", ["web3career_fetch.py"],
                 env={"W3_RESOLVE": "0", "W3_MAX_SECONDS": "900"}, tries=2)]))
        # usajobs lives HERE, not in "keyed", because it has a keyless path
        # (usajobs_fetch.py) and US federal postings are squarely in
        # scope.  It sat in a DEFAULT_OFF family with no handler, which is
        # why contract_norm/usajobs.jsonl froze on 2026-08-14.  It resumes
        # and appends by id, so "remote"'s resume-only rotation is correct
        # and it must NOT be truncated.
        refresh.append(("usajobs", [
            Step("usajobs_fetch", ["usajobs_fetch.py"],
                 env={"USAJOBS_DAYS": "90"}, tries=3, ok_rc=(0, 1))]))
        # smallremote was a pure orphan -- in no family at all, referenced by
        # no .py and no .sh.  It writes through cc.Writer, which opens "a",
        # so without the truncate below every run appends another complete
        # copy of the board.
        refresh.append(("smallremote", [
            Step("smallremote_fetch", ["smallremote_fetch.py"],
                 pre=truncate(os.path.join(NORM, "smallremote.jsonl")),
                 tries=3, ok_rc=(0, 1))]))

    # ---- keyed ----------------------------------------------------------
    # Declared in FAMILIES and DEFAULT_OFF since the beginning and NEVER given
    # a handler, so `--only keyed` ran the build tail and nothing else.  Now
    # that usajobs has moved to "remote" (it has a keyless path), adzuna is
    # the only genuinely key-gated source left, which is what this family was
    # always meant to describe.
    if "keyed" in fams:
        if os.environ.get("ADZUNA_APP_ID") and os.environ.get("ADZUNA_APP_KEY"):
            refresh.append(("adzuna", [
                Step("adzuna_fetch", ["adzuna_fetch.py"],
                     env={"ADZUNA_COUNTRIES": "us", "ADZUNA_DAYS": "30"},
                     tries=3, ok_rc=(0, 1))]))
        else:
            say("[keyed] adzuna SKIPPED — set ADZUNA_APP_ID and "
                "ADZUNA_APP_KEY to enable it")

    # ---- intl -----------------------------------------------------------
    # Also declared-but-unhandled.  Out of the US scope, so it stays in
    # DEFAULT_OFF and is reachable only via --only intl.
    if "intl" in fams:
        # arbeitsagentur and intlmisc keep NO cursor and write through
        # cc.Writer ("a"), so each run appends a COMPLETE second copy.  That
        # is the documented reason this family was left unwired; truncating
        # first is the fix, and it is the same pre= the boards/phenom
        # appenders already use.
        refresh.append(("arbeitsagentur", [
            Step("arbeitsagentur_fetch", ["arbeitsagentur_fetch.py"],
                 pre=truncate(os.path.join(NORM, "arbeitsagentur.jsonl")),
                 tries=3, ok_rc=(0, 1))]))
        refresh.append(("intlmisc", [
            Step("intlmisc_fetch", ["intlmisc_fetch.py"],
                 pre=truncate(os.path.join(NORM, "intlmisc.jsonl")),
                 tries=3, ok_rc=(0, 1))]))
        # intlboards is the exception: it DOES keep per-source cursors in
        # contract_norm/.intlboards_state.json and appends only new rows, so
        # truncating it would throw away everything the cursor has already
        # walked past and never re-fetch it.
        refresh.append(("intlboards", [
            Step("intlboards_fetch", ["intlboards_fetch.py"],
                 tries=3, ok_rc=(0, 1))]))

    tail = []
    if fams and not args.build_only:
        tail.append(Step(
            "normalize_mined", ["normalize_mined.py"],
            needs=(p("contract_from_getro.jsonl"),
                   p("contract_from_lever.jsonl")),
            pre=truncate(os.path.join(NORM, "getro.jsonl"),
                         os.path.join(NORM, "lever.jsonl")),
            tries=LOCAL_TRIES))
    tail += [
        # BEFORE bh_build_combined, because it is the thing that reads these
        # files.  Several tail fetchers append without any cross-run skip state
        # (arbeitsagentur keeps no state file at all; intlmisc has no skip
        # state; four of intlboards' six sources re-append in full), so the
        # same posting accumulates a copy per run.  bh collapses them anyway,
        # but all_jobs_build still streams every byte at 3-5 GB RSS.  First run
        # on the 2026-08-14 corpus collapsed 2,906,211 rows and 1.7 GB.
        Step("norm_dedupe", ["norm_dedupe.py"], tries=LOCAL_TRIES),
        Step("build_sources", ["build_sources.py"], tries=LOCAL_TRIES),
        Step("getro_build_sheet", ["getro_build_sheet.py"],
             needs=(p("getro_all_jobs.jsonl"), p("getro_job_boards.csv")),
             tries=LOCAL_TRIES),
        Step("lever_build_sheet", ["lever_build_sheet.py"],
             needs=(p("lever_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        # greenhouse/ashby are driven by their own bash scripts, but
        # all_jobs_build.py reads their masters — so without these two steps
        # run_all merged whatever stale CSV happened to be on disk.
        Step("greenhouse_build_sheet", ["greenhouse_build_sheet.py"],
             needs=(p("greenhouse_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("ashby_build_sheet", ["ashby_build_sheet.py"],
             needs=(p("ashby_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("rippling_build_sheet", ["rippling_build_sheet.py"],
             needs=(p("rippling_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("personio_build_sheet", ["personio_build_sheet.py"],
             needs=(p("personio_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("yc_build_sheet", ["yc_build_sheet.py"],
             needs=(p("yc_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("dover_build_sheet", ["dover_build_sheet.py"],
             needs=(p("dover_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("notion_build_sheet", ["notion_build_sheet.py"],
             needs=(p("notion_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        Step("bh_build_combined", ["bh_build_combined.py"],
             tries=LOCAL_TRIES),
        # Always last: it merges every master above.  The build chain runs
        # fail-soft, so a family with no JSONL yet is skipped rather than
        # taking this step down with it.
        Step("all_jobs_build", ["all_jobs_build.py"],
             tries=LOCAL_TRIES),
    ]
    return discover, refresh, tail


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(
        description="Refresh every job pipeline and rebuild the monolithic "
                    "All_Jobs workbook.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Families:")[0])
    ap.add_argument("--discover", action="store_true",
                    help="re-run board/tenant discovery before refreshing")
    ap.add_argument("--build-only", action="store_true",
                    help="skip all fetching; rebuild sheets from data on disk")
    ap.add_argument("--resume", action="store_true",
                    help="continue an interrupted refresh (no rotation)")
    ap.add_argument("--only", default="",
                    help="comma list of families to refresh "
                         f"({','.join(FAMILIES)})")
    ap.add_argument("--skip", default="",
                    help="comma list of families NOT to refresh")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan, execute nothing")
    args = ap.parse_args()

    # DEFAULT_OFF families are reachable only by naming them in --only.
    pool = FAMILIES if args.only else [f for f in FAMILIES
                                       if f not in DEFAULT_OFF]
    fams = [f for f in pool
            if (not args.only or f in args.only.split(","))
            and f not in args.skip.split(",")]
    for tok in filter(None, args.only.split(",") + args.skip.split(",")):
        if tok not in FAMILIES:
            ap.error(f"unknown family {tok!r} (choose from "
                     f"{', '.join(FAMILIES)})")
    if args.build_only:
        fams = []

    os.makedirs(LOGS, exist_ok=True)
    dry = args.dry_run
    t0 = time.time()
    say(f"run_all: families={fams or '(build only)'} discover={args.discover} "
        f"resume={args.resume} dry_run={dry}")

    discover, refresh, tail = build_plan(args, fams)

    # ---- discovery (uses the PREVIOUS crawl's data, so runs pre-rotation)
    if discover:
        # The slug sweep produces rippling_candidates.csv / personio_
        # candidates.csv, so the chains that consume them must wait for it.
        # Same Event contract as the ats-waits-for-getro wiring below: the
        # event is pre-set when the producer is not part of this run, or the
        # waiters would block forever.
        slugs_done = threading.Event()
        if not any(tag == "slugs" for tag, _ in discover):
            slugs_done.set()
        waiters = {"rippling-disc", "workday-disc"}
        threads = []
        for tag, steps in discover:
            if tag == "slugs":
                threads.append(threading.Thread(
                    target=run_chain, args=(tag, steps, dry, slugs_done)))
            elif tag in waiters:
                def slug_waiter(tag=tag, steps=steps):
                    slugs_done.wait()
                    if _failed.get("slugs"):
                        say(f"[{tag}] slug sweep failed — running against the "
                            f"existing candidate list")
                    run_chain(tag, steps, dry)
                threads.append(threading.Thread(target=slug_waiter))
            else:
                threads.append(threading.Thread(
                    target=run_chain, args=(tag, steps, dry)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if _failed:
            say(f"discovery failures: {_failed} — continuing with the "
                f"existing board lists")
            # Discovery is best-effort by design (Wayback CDX is rate-limited
            # and flaky), so its failures must not survive into the final exit
            # status — otherwise every --discover run exits 1 even though the
            # refresh and build phases both succeeded.
            _discovery_failed.update(_failed)
            _failed.clear()

    # ---- rotation
    if fams and not args.resume:
        for fam in fams:
            rotate_family(fam, dry)
    elif args.resume:
        say("[rotate] --resume: keeping JSONLs + state files")

    # ---- refresh chains (parallel; ats waits for getro)
    getro_done = threading.Event()
    if not any(tag == "getro" for tag, _ in refresh):
        getro_done.set()
    # Same pre-set guard, third instance: if the prelude is not part of this
    # run (e.g. --only remote), every tail chain would wait on it forever.
    tenants_ready = threading.Event()
    if not any(tag == "prelude" for tag, _ in refresh):
        tenants_ready.set()
    tenant_waiters = {"workday", "oracle", "icims", "adp", "paycom",
                      "eightfold", "jobvite", "gem", "comeet", "atstail-rest",
                      "zohorecruit"}
    threads = []
    for tag, steps in refresh:
        if tag == "prelude":
            threads.append(threading.Thread(
                target=gated(tag, steps, dry, done_event=tenants_ready)))
        elif tag in tenant_waiters:
            threads.append(threading.Thread(
                target=gated(tag, steps, dry, wait_event=tenants_ready)))
        elif tag == "getro":
            threads.append(threading.Thread(
                target=gated(tag, steps, dry, done_event=getro_done)))
        elif tag == "ats":
            def ats_runner(steps=steps):
                getro_done.wait()
                if _failed.get("getro"):
                    say("[ats] getro chain failed - running against the "
                        "existing slug list")
                # Note the wait above happens BEFORE gated() acquires — see the
                # rule in gated().
                gated("ats", steps, dry)()
            threads.append(threading.Thread(target=ats_runner))
        else:
            threads.append(threading.Thread(target=gated(tag, steps, dry)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # ---- normalize + build (sequential: these are the memory-heavy steps).
    # fail_soft: each builder is an independent local transform, so a family
    # with no JSONL yet must not stop all_jobs_build from running.
    run_chain("build", tail, dry, fail_soft=True)

    # ---- report
    say(f"run_all finished in {(time.time() - t0) / 60:.1f} min")
    for name in ("getro_jobs_master.csv", "lever_jobs_master.csv",
                 "greenhouse_jobs_master.csv", "ashby_jobs_master.csv",
                 "rippling_jobs_master.csv", "personio_jobs_master.csv",
                 "yc_jobs_master.csv", "dover_jobs_master.csv",
                 "notion_jobs_master.csv",
                 "portals_combined_master.csv", "all_jobs_master.csv"):
        q = p(name)
        if os.path.exists(q):
            mt = datetime.fromtimestamp(os.path.getmtime(q), tz=timezone.utc)
            say(f"  {name}: {os.path.getsize(q) / 1e6:.1f} MB, "
                f"updated {mt.strftime('%Y-%m-%d %H:%M')}Z")
        else:
            say(f"  {name}: MISSING")
    snap = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    xlsx = p(f"All_Jobs_{snap}.xlsx")
    if os.path.exists(xlsx):
        say(f"  All_Jobs_{snap}.xlsx: {os.path.getsize(xlsx) / 1e6:.1f} MB")
    # The standing backlog.  Both existing censuses answer only "how is family
    # X doing?" against a closed list -- ats_mine_tenants has 19 regexes and
    # coverage_report has a FAMS table -- so a platform nobody has written a
    # rule for is invisible to the build queue rather than merely unbuilt.
    # This is the one report whose output is the UNKNOWN tail, so it has to run
    # unattended or it stops being read.  Non-fatal: it is a report.
    if not dry and os.path.exists(p_master := p("all_jobs_master.csv")):
        try:
            # Board-level coverage first: it reports how much of what we
            # already KNOW about we are actually reaching, and its default mode
            # is one second.  This is the report that surfaced "workable
            # 21,370 known / 4,436 reached".  --with-rows --with-master is the
            # useful-but-slow mode (streams ~5.5 GB of JSONL + master); opt in
            # with RUN_ALL_COVERAGE_FULL=1.
            say("[report] coverage_report — boards known vs reached")
            cov = [sys.executable, "coverage_report.py"]
            if os.environ.get("RUN_ALL_COVERAGE_FULL") == "1":
                cov += ["--with-rows", "--with-master"]
            subprocess.run(cov, cwd=BASE, timeout=3600, check=False)
            say("[report] host_census — un-piped apply-url apexes")
            subprocess.run([sys.executable, "host_census.py", p_master,
                            "--top", "40"], cwd=BASE, timeout=1800, check=False)
        except (OSError, subprocess.SubprocessError) as e:
            say(f"[report] host_census skipped: {e}")
    if _discovery_failed:
        say(f"discovery steps that failed (non-fatal): {_discovery_failed}")
    if _failed:
        say(f"FAILED steps: {_failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
