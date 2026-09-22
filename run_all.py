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
  python3 run_all.py --rollback          undo an INTERRUPTED run: move every
                                         .prev.jsonl it rotated back into place
                                         (then run bare as usual)
  python3 run_all.py --allow-battery     start even when not on AC power

Families: getro · lever · portals.  The build stage always runs every builder
(each is an idempotent local transform), so masters stay consistent even when
only one family was refreshed.

Refresh semantics: fetchers append to their JSONL and skip boards marked done
in their *_fetch_state.json, so a true refresh must rotate first.  Unless
--resume is given, each selected family's JSONLs move to *.prev.jsonl (one
generation kept as a safety net) and its state files are deleted.  Stateless
appenders (boards_fetch, phenom_fetch) instead truncate their own output
before every attempt so retries can't duplicate rows.

Every step is supervised like run_supervised.sh: up to 40 relaunches (each
fetcher resumes from its own per-item state), stdout+stderr appended to
logs/<step>.log, and the step's final summary line echoed to the console.
A relaunch that makes NO progress is not repeated 40 times, though: three
identical attempts end the retries (see run_step's stall rule), and a network
outage holds the retries instead of spending them.

Safety rails added 2026-09-15 (see the plan of that date):
  * logs/run_all.state.json records the phase and every file rotated, so a
    run that dies after rotation cannot be clobbered by a bare re-run -- the
    next invocation refuses (exit 3) and names the right re-entry:
    --resume, --build-only or --rollback.  logs/run_all.lock (flock) stops two
    runs overlapping.
  * preflight: AC power, network, free disk, swap -- the checks the runbook
    used to ask the operator to remember.
  * a row check after each rotated family's fetch falls back to .prev when the
    fresh generation collapsed; a disk guard pauses the build tail and prunes
    .prev before the disk fills.
  * a heartbeat every 15 min and logs/run_all_summary.json at the end.
Exit codes: 0 clean, 1 some step FAILED (the build still ran), 3 refused.
"""

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

# RUN_ALL_BASE is a TEST SEAM for run_all_selftest.py, which runs the whole
# supervisor against fake step scripts in a temp dir.  Everything that touches
# the filesystem derives from BASE, so pointing it elsewhere is what guarantees
# a test can never move a real .prev.jsonl.  Never set it for a real run.
BASE = (os.environ.get("RUN_ALL_BASE")
        or os.path.dirname(os.path.abspath(__file__)))
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
            # wttj is FR-heavy (58,237 of 88,431 offices are FR) and would by
            # that alone belong in `intl`.  It is default-ON anyway, for two
            # reasons the intl families cannot claim.  It costs ~1 minute: the
            # whole 88k corpus is 179 Algolia slices fetched 12-wide, measured
            # at 15s for the walk.  And published_at_timestamp is on 100% of
            # its records, against a master where only ~19% of rows carry the
            # employer's own date -- it is the cheapest real-date supply the
            # pipeline has.  It also truncates its own JSONL, so it has none
            # of the append-a-second-copy problem that keeps arbeitsagentur
            # and intlmisc out of a bare run.
            "wttj",
            "keyed",   # needs an API key present in the env to do anything.
            "intl")    # out of the US scope; reachable via --only, never
                       # default: arbeitsagentur keeps NO state file at all, so
                       # every run appends another complete copy.

# Families that exist for --only but are NOT in a bare `python3 run_all.py`.
#
# notion joined them 2026-08-26.  It costs far more than it returns: 269 rows
# in a 4,341,414-row master (0.006%), against a 44-minute probe stall, a
# candidate list that silently deleted 802 live boards, and a board gate that
# has to tell a job board from a reading list.  Nothing is deleted -- the
# scripts, the JSONL and notion_jobs_master.csv all stay, so all_jobs_build
# keeps merging the rows it already has, and `--only notion` still runs the
# whole family.  It just no longer costs a bare run anything.
DEFAULT_OFF = frozenset({"keyed", "intl", "notion"})
ENUM_SLACK = 3000  # --discover: sweep past the highest known collection id

# Per-state-file key scoping for state_refresh.  A file listed here has only
# the keys under these prefixes considered for re-fetch; everything else is
# kept verbatim.  See state_refresh.main_paths.
def _refresh_scopes():
    return {os.path.join(BASE, "atstail_fetch_state.json"):
            ("pinpoint:", "keka:", "join:", "taleo:", "wttj:")}


def _envf(name, default):
    try:
        return float(os.environ.get(name) or default)
    except ValueError:
        return float(default)


FETCH_TRIES = 40   # steps that resume from a state file
LOCAL_TRIES = 2    # local transforms / miners (a failure is deterministic)
RETRY_SLEEP = _envf("RUN_ALL_RETRY_SLEEP", 3)
# Seconds a step gets to honour SIGTERM before the hard timeout escalates to
# SIGKILL.  Generous on purpose: these fetchers flush a state file and a ledger
# on the way out, and losing that re-dates every undated posting on the next
# run -- far more expensive than waiting a minute for a clean stop.
TERM_GRACE = float(os.environ.get("RUN_ALL_TERM_GRACE") or 60)

# ---- the stall rule (run_step) ---------------------------------------------
# FETCH_TRIES=40 exists so a fetcher that crashed can resume from its state --
# forty relaunches that each make PROGRESS.  Forty relaunches that make none
# is a stall: on 2026-09-15 rippling_fetch_jobs banked 2,568 of 2,569 boards on
# attempt 1, then spent attempts 2-15 (~11 min, still going when the run was
# cancelled) re-trying one board that answers HTTP 400, printing the same
# "Done. 0 job rows ...; 1 boards failed" every time -- and holding
# rippling_tenant_fetch behind it.  icims (45 min), ats_fetch (2h16m) and
# notion_probe_boards (44 min) are the same shape, written up in memory.
#
# A step whose final line matches its partial_ok regex has banked its data and
# is only re-trying stragglers, so three identical attempts end it as PARTIAL
# (success).  Anything else -- a traceback, a SystemExit guard -- might be a
# remote outage that clears, so it is only declared STALLED (failure) once the
# identical streak is both >= STALL_REPEATS attempts and >= STALL_MIN minutes
# old, with the waits between attempts growing along STALL_SLEEPS.  A
# 10-minute api.getro.com blip therefore cannot fail getro_fetch_pdl (41% of
# Getro's rows).
STALL_REPEATS = 3
STALL_MIN = _envf("RUN_ALL_STALL_MIN", 15)
PARTIAL_SLEEP = _envf("RUN_ALL_PARTIAL_SLEEP", 30)
STALL_SLEEPS = tuple(float(x) for x in (
    os.environ.get("RUN_ALL_STALL_SLEEPS") or "60,180,300,600").split(","))
# A failed attempt while the NETWORK is down is not evidence about the step:
# hold (without spending the attempt) until it is back, up to this long.
NET_WAIT_MIN = _envf("RUN_ALL_NET_WAIT_MIN", 90)
NET_POLL_S = _envf("RUN_ALL_NET_POLL_S", 30)
NET_PROBES = (("www.google.com", 443), ("one.one.one.one", 443),
              ("www.apple.com", 443))

# The "some boards failed, re-run to retry" summary every board fetcher prints
# on a partial success.  None of them uses a distinct exit code -- partial,
# crash and SystemExit guard are all rc 1 -- so ok_rc=(0, 1) also accepted
# tracebacks as success.  This matches the fetcher's OWN words instead:
#   rippling/lever/personio/yc/dover/greenhouse  "Done. N job rows in X; K boards failed"
#   ashby_fetch_jobs.py:215                      "K boards failed — re-run to retry"
#   atstail_fetch.py:1315                        "K tenants failed — re-run to retry"
# re.match, not fullmatch: several append " (re-run to retry)".
PARTIAL_BOARDS = (r"Done\. \d+ job rows in .*; [1-9]\d* boards failed"
                  r"|\d+ (?:boards|tenants) failed — re-run to retry")

# ---- run marker, lock, safety gates ----------------------------------------
STATE_FILE = os.path.join(LOGS, "run_all.state.json")
LOCK_FILE = os.path.join(LOGS, "run_all.lock")
SUMMARY_FILE = os.path.join(LOGS, "run_all_summary.json")
HISTORY_FILE = os.path.join(LOGS, "run_all_history.jsonl")
LAST_SUCCESS_FILE = os.path.join(LOGS, "run_all.last_success.json")
SHRUNK_DIR = os.path.join(LOGS, "shrunk")
EXIT_REFUSED = 3   # not 2: argparse exits 2 on a usage error
# Phases in which a death leaves the live JSONLs half-fetched (rotating,
# refresh) or leaves a finished fetch unbuilt (build -> --build-only).  NOT
# "report": by then the master is written and a bare run is an ordinary one.
PAST_ROTATION = ("rotating", "refresh", "build")
# Row check: a fresh generation below FLOOR x its .prev is a collapse, and the
# build falls back to the .prev.  0.9 for getro, because getro WITHOUT its PDL
# half is 0.587 of a full generation -- on the 0.6 knife edge -- and for any
# family whose chain did not finish cleanly.
SHRINK_FLOOR = _envf("RUN_ALL_SHRINK_FLOOR", 0.6)
SHRINK_FLOOR_STRICT = 0.9
SHRINK_FLOORS = {"getro": 0.9}
SHRINK_GUARD = os.environ.get("RUN_ALL_SHRINK_GUARD", "1") != "0"
# Disk: preflight refuses below MIN_FREE, warns below WARN_FREE; the build-tail
# guard pauses at PAUSE, resumes at RESUME, gives up on the paused step after
# KILL_MIN; .prev is pruned before bh_build_combined when below PRUNE_BELOW.
MIN_FREE_GB = _envf("RUN_ALL_MIN_FREE_GB", 12)
WARN_FREE_GB = _envf("RUN_ALL_WARN_FREE_GB", 25)
SWAP_WARN_GB = _envf("RUN_ALL_SWAP_WARN_GB", 10)
DISK_PAUSE_GB = _envf("RUN_ALL_DISK_PAUSE_GB", 5)
DISK_RESUME_GB = _envf("RUN_ALL_DISK_RESUME_GB", 8)
DISK_KILL_MIN = _envf("RUN_ALL_DISK_KILL_MIN", 30)
DISK_POLL_S = _envf("RUN_ALL_DISK_POLL_S", 10)
PRUNE_BELOW_GB = _envf("RUN_ALL_PRUNE_BELOW_GB", 25)
HEARTBEAT_MIN = _envf("RUN_ALL_HEARTBEAT_MIN", 15)

_print_lock = threading.Lock()
_failed_lock = threading.Lock()   # _failed writes vs the discovery hand-off
_failed = {}       # family -> step name that failed
_discovery_failed = {}  # same, but for the best-effort discovery phase only:
                        # recorded and reported, never fatal (see main())
_bg_threads = []   # discovery chains nothing waits for (see `background` in
                   # main()): started with the rest, joined after the build
                   # tail so they bank their state without gating any fetcher.
_bg_tags = set()   # their tags, so the post-build join can move THEIR failures
                   # to _discovery_failed without touching refresh/build ones.

# Child processes, the marker, and the cross-thread bits the signal handler,
# disk guard and heartbeat read.  The locks the handler takes are RLocks: it
# runs in the MAIN thread, which may already hold them (the build tail runs
# there), and a plain Lock would deadlock the shutdown.
_spawn_lock = threading.RLock()   # held around every Popen
_stopping = threading.Event()     # set once shutdown begins: no new children
_active = {}       # pid -> (tag, step name, Popen, argv, t0)
_critical = threading.RLock()     # row-check / prune file moves
_state_lock = threading.RLock()
_state = {}        # in-memory mirror of logs/run_all.state.json
_records = []      # one dict per step outcome, for the summary
_records_lock = threading.Lock()
_built = set()     # build sheets that already ran inside their refresh chain
_rowcheck = {}     # live path -> verdict dict
_chain_ok = {}     # refresh chain tag -> True/False once it has finished
_run = {"dry": False, "resume": False, "run_id": None, "phase": None}


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
                 func=None, tries=FETCH_TRIES, soft_needs=(), ok_rc=(0,),
                 hard_timeout=None, out=(), progress=(), partial_ok=None,
                 first_env=None, skip_if=None, marks_built=False,
                 cleanup=()):
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
        # Wall-clock ceiling in SECONDS, enforced by the parent.  A step's own
        # --budget is a REQUEST; this is the guarantee.  Learned 2026-08-26:
        # careers_resolve honours its budget in the submit loop and still ran
        # 4.1h past it draining in-flight work, and an in-process threading
        # .Timer added to bound that did not fire under 32 threads of SSL load.
        # A child cannot be trusted to bound itself, so the supervisor does it.
        # Kills produce a negative rc, which the retry guard below refuses to
        # retry -- a step killed for overrunning must not start over.
        self.hard_timeout = hard_timeout
        # Progress evidence for the stall rule.  `out`: files the step APPENDS
        # to (growth = progress).  `progress`: small files whose CONTENT change
        # is progress (hashed).  Never list a file another chain also writes
        # -- zoho shares atstail.jsonl, so its growth would say nothing.
        self.out = tuple(out)
        self.progress = tuple(progress)
        # Regex over the attempt's final line meaning "finished, some items
        # failed, re-run to retry".  A stall or tries exhaustion on such a
        # line is PARTIAL success, not a failure.  See PARTIAL_BOARDS.
        self.partial_ok = partial_ok
        # Env applied on ATTEMPT 1 ONLY, and never under --resume.  Exists for
        # zoho's ATSTAIL_REFRESH, which drops every zohorecruit row and state
        # key before fetching: in env= it re-ran on every retry, so each
        # attempt threw away the previous one's work and one flaky tenant
        # could cost 40 full re-fetches.
        self.first_env = dict(first_env or {})
        # Callable -> True means "already done this run, skip".  The tail's
        # build sheets use it for the ones their refresh chain already built.
        self.skip_if = skip_if
        self.marks_built = marks_built
        # Files to delete if the step is KILLED (rc < 0): a builder's
        # half-written .tmp.
        self.cleanup = tuple(cleanup)

    def describe(self):
        if self.func is not None:
            return f"<internal: {self.name}>"
        e = " ".join(f"{k}={v}" for k, v in self.env.items())
        if self.first_env:
            e += (" " if e else "") + "[attempt1: " + " ".join(
                f"{k}={v}" for k, v in self.first_env.items()) + "]"
        return (e + " " if e else "") + " ".join(self.argv)


def _ceiling(env, grace_min=None):
    """Parent-enforced wall clock for a step that declares its own *_BUDGET_MIN.

    A fetcher's budget stops it SUBMITTING new work; it does not bound the
    drain of what is already in flight, and every measured overrun in this repo
    has been in that drain -- manatal 93.6 min against 60, careers_resolve 4.1h
    against 8h.  So the supervisor grants budget + grace and then takes the
    process back.  Returns None when the step declares no budget (0 = unlimited
    is a deliberate setting in several places and must stay unlimited).
    """
    if grace_min is None:
        grace_min = float(os.environ.get("RUN_ALL_BUDGET_GRACE_MIN") or 30)
    for k, v in env.items():
        if k.endswith("_BUDGET_MIN"):
            try:
                b = float(v)
            except (TypeError, ValueError):
                continue
            if b > 0:
                return (b + grace_min) * 60
    return None


_SIG_SUBS = (
    (re.compile(r"0x[0-9a-fA-F]+"), "0x_"),
    (re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?"
                r"(?:Z|[+-]\d{2}:?\d{2})?"), "<ts>"),
    (re.compile(r"\b\d{1,2}:\d{2}:\d{2}\b"), "<hms>"),
    (re.compile(r"\b\d+(?:\.\d+)?\s?(?:ms|s|sec|secs|seconds|min|mins)\b"),
     "<dur>"),
)


def _signature(line):
    """An attempt's final line with what differs between two IDENTICAL
    failures normalised away: object addresses (a requests error carries
    `object at 0x...`), timestamps, durations.  Counts are KEPT on purpose --
    "5 boards failed" -> "3 boards failed" is progress."""
    for rx, rep in _SIG_SUBS:
        line = rx.sub(rep, line)
    return line.strip()


def _attempt_last(log_path, offset, limit=65536):
    """Last meaningful line of the attempt that began at byte `offset`.  Reads
    only that attempt's tail -- the old code rescanned the whole appended log
    (1.7 MB for getro) every time a step finished."""
    try:
        with open(log_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(offset, f.tell() - limit))
            body = f.read().decode("utf-8", "replace")
    except OSError:
        return ""
    last = ""
    for line in body.splitlines():
        line = line.strip()
        if line and not line.startswith("==="):
            last = line
    return last


def _stats(paths):
    """(size, mtime) per path; (-1, -1) when absent."""
    out = []
    for q in paths:
        try:
            st = os.stat(q)
            out.append((st.st_size, st.st_mtime))
        except OSError:
            out.append((-1, -1))
    return tuple(out)


def _delta(stats0, step):
    """Total bytes the step's declared outputs gained (or lost) overall."""
    if not step.out:
        return None
    now = _stats(step.out)
    return sum(b[0] for b in now if b[0] > 0) - \
        sum(a[0] for a in stats0 if a[0] > 0)


def _wrote(stats0, step):
    """Did the step WRITE any of its declared outputs this attempt?

    None when the step declares no out= files -- "unknown", not "no".  Only a
    step that TELLS us where its rows land can be checked, which is why out=
    was added to the tail fetchers alongside this.

    Deliberately "changed", not "grew".  Growth is the wrong test now that
    manatal/dayforce/neogov/ukg/cornerstone carry a board-scoped merge: a run
    that refreshes boards and finds postings deleted upstream legitimately
    SHRINKS its file (dayforce did exactly that on 2026-09-16, 113,196 ->
    113,075).  Calling that a silent no-op would accuse the five families this
    check was built to protect.  mtime is included because os.replace always
    stamps it, so a merge that happens to land on an identical byte count is
    still seen as work.
    """
    if not step.out:
        return None
    return _stats(step.out) != stats0


def _sizes(paths):
    out = []
    for q in paths:
        try:
            out.append(os.path.getsize(q))
        except OSError:
            out.append(-1)
    return tuple(out)


def _fingerprints(paths):
    """Content fingerprints for Step.progress files: sha1 up to 64 MB, else
    (size, mtime).  Content, not mtime, because most fetchers rewrite their
    state at the end of EVERY attempt whether or not anything changed."""
    fp = []
    for q in paths:
        try:
            st = os.stat(q)
            if st.st_size > 64 * 2**20:
                fp.append((st.st_size, st.st_mtime_ns))
                continue
            h = hashlib.sha1()
            with open(q, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            fp.append((st.st_size, h.hexdigest()))
        except OSError:
            fp.append(None)
    return tuple(fp)


def _net_up(timeout=5):
    """True if any well-known host takes a TCP connection.  Resolves the name
    too, so a dead resolver counts as down -- which is what it is to a
    fetcher.  RUN_ALL_ASSUME_NET=1 is a test seam (run_all_selftest.py)."""
    if os.environ.get("RUN_ALL_ASSUME_NET") == "1":
        return True
    for host, port in NET_PROBES:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            continue
    return False


def _hold_for_network(tag, name):
    """Block while the network is down, up to NET_WAIT_MIN.  True when it came
    back: the caller re-runs the attempt WITHOUT charging it.  A wifi drop
    used to burn all 40 attempts in a few minutes and report GIVING UP on a
    step that had nothing wrong with it."""
    say(f"[{tag}] network DOWN after {name} failed — holding retries "
        f"(up to {NET_WAIT_MIN:.0f} min) instead of spending them")
    deadline = time.time() + NET_WAIT_MIN * 60
    while time.time() < deadline:
        if _stopping.wait(NET_POLL_S):
            return False
        if _net_up():
            say(f"[{tag}] network back — re-running {name}")
            return True
    say(f"[{tag}] network still down after {NET_WAIT_MIN:.0f} min — "
        f"counting the attempt")
    return False


def _record(tag, step, outcome, attempts, t0, rc=None, last="", wrote=None,
            bytes_delta=None):
    """wrote: True/False when the step declares out= files, else None.

    False on a SUCCESSFUL step is the signal this whole file was missing.
    run_step judges a step by its exit code and a last-line regex and never
    looks at rows, so a fetcher that banks nothing exits 0 and is recorded ok.
    That is how ukg, manatal, dayforce, neogov and cornerstone each printed
    "0 to fetch (N already recorded)", finished in 0.1 min and reported ok for
    weeks while ~392k rows -- 95-98% of them in no other pipe -- went stale.
    """
    with _records_lock:
        _records.append({"tag": tag, "step": step.name, "outcome": outcome,
                         "attempts": attempts,
                         "secs": round(time.time() - t0, 1),
                         "rc": rc, "last": (last or "")[:300],
                         "wrote": wrote, "bytes_delta": bytes_delta})


def _wait_child(proc, step, tag):
    """(rc, timed_out).  hard_timeout enforcement lives here."""
    if not step.hard_timeout:
        return proc.wait(), False
    try:
        return proc.wait(timeout=step.hard_timeout), False
    except subprocess.TimeoutExpired:
        # SIGTERM first: these fetchers checkpoint their state and several
        # install a handler to flush on the way out, so a straight SIGKILL
        # would throw away work that is already paid for.  SIGKILL only if it
        # will not go.
        _ht = (f"{step.hard_timeout / 60:.0f}-min"
               if step.hard_timeout >= 60 else f"{step.hard_timeout:.0f}s")
        say(f"[{tag}] {step.name} exceeded its {_ht} hard timeout — "
            f"terminating")
        proc.terminate()
        try:
            return proc.wait(timeout=TERM_GRACE), True
        except subprocess.TimeoutExpired:
            say(f"[{tag}] {step.name} ignored SIGTERM — killing")
            proc.kill()
            return proc.wait(), True


def _mark_built(step):
    if step.marks_built:
        _built.add(step.name)


def run_step(step, tag, dry):
    if dry:
        say(f"[{tag}] DRY  {step.name}: {step.describe()}")
        return True
    t0 = time.time()
    if step.skip_if is not None and step.skip_if():
        say(f"[{tag}] SKIP {step.name}: already built this run inside its "
            f"refresh chain")
        _record(tag, step, "skipped", 0, t0)
        return True
    for f in step.soft_needs:
        if _rows(f) < 1:
            say(f"[{tag}] NOOP {step.name}: no {os.path.basename(f)} yet "
                f"— nothing to fetch (not a failure)")
            _record(tag, step, "noop", 0, t0)
            return True
    for f in step.needs:
        if not os.path.exists(f):
            say(f"[{tag}] SKIP {step.name}: missing input "
                f"{os.path.basename(f)}")
            _record(tag, step, "missing-input", 0, t0)
            return False
    log_path = os.path.join(LOGS, step.name + ".log")
    prev_sig, streak, streak_t0 = None, 0, t0
    last, rc, holds, launches = "", None, 0, 0
    # True once ANY attempt ended on the step's partial_ok line: its data is
    # banked, so a later attempt that only crashed while re-trying the
    # stragglers must not turn the step into a failure.  (greenhouse/ashby
    # used to take attempt 1's exit 1 as final; a crash on their attempt 2
    # would otherwise make them FAILED where they were clean before.)
    banked = False
    # True once ANY attempt WROTE the step's out= files.  Cumulative
    # for the same reason `banked` is: attempt 1 can bank rows and then die
    # re-trying stragglers, and attempt 2 legitimately writes nothing.  Judging
    # the LAST attempt alone would call that a no-op.
    wrote_any = False
    # Sizes at the START OF THE STEP, for the per-source byte delta in the
    # summary.  stats0 below is per-ATTEMPT and answers "did this attempt
    # write"; this one answers "what did this step add overall", which is the
    # closest thing the run has to a per-source row ledger.  Bytes rather than
    # rows on purpose: workday.jsonl is 1.1GB and counting its lines every run
    # would cost more than the signal is worth.
    stats_run0 = _stats(step.out)
    attempt = 0
    while attempt < step.tries:
        attempt += 1
        if _stopping.is_set():
            _record(tag, step, "killed", attempt - 1, t0, rc, last)
            return False
        if step.pre:
            # Contained.  An OSError here (disk full — this box has hit 4 GiB)
            # used to propagate out of run_chain PAST done_event.set(), so the
            # run exited 0 with the output missing and every waiter on that
            # event blocked forever.
            try:
                step.pre()
            except OSError as e:
                say(f"[{tag}] FAIL {step.name} pre-hook: {e}")
                _record(tag, step, "failed", attempt, t0, None, str(e))
                return False
        if step.func is not None:
            try:
                step.func()
                say(f"[{tag}] done {step.name} "
                    f"({time.time() - t0:.0f}s)")
                _mark_built(step)
                _record(tag, step, "ok", attempt, t0)
                return True
            except Exception as e:  # noqa: BLE001 — supervised retry
                last = f"{type(e).__name__}: {e}"
                say(f"[{tag}] FAIL {step.name} attempt {attempt}: {e}")
                time.sleep(RETRY_SLEEP)
                continue
        env = dict(os.environ, PYTHONUNBUFFERED="1", **step.env)
        # Keyed on LAUNCHES, not `attempt`: a network hold rewinds the attempt
        # counter, and re-applying zoho's ATSTAIL_REFRESH after it would wipe
        # the rows the first launch had just fetched.
        if launches == 0 and step.first_env and not _run["resume"]:
            env.update(step.first_env)
        launches += 1
        sizes0, fp0 = _sizes(step.out), _fingerprints(step.progress)
        stats0 = _stats(step.out)
        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"=== [{step.name}] attempt {attempt} "
                      f"{datetime.now(timezone.utc).isoformat()} ===\n")
            log.flush()
            offset = log.tell()
            say(f"[{tag}] run  {step.name} (attempt {attempt}) "
                f"-> logs/{step.name}.log")
            with _spawn_lock:
                if _stopping.is_set():
                    _record(tag, step, "killed", attempt - 1, t0, rc, last)
                    return False
                # Own session: a Ctrl-C or a TERM aimed at run_all reaches
                # run_all ALONE, and it stops each child with TERM + grace so
                # the child can flush its state (see _on_signal).
                proc = subprocess.Popen(
                    [PY] + step.argv, cwd=BASE, env=env, stdout=log,
                    stderr=subprocess.STDOUT, start_new_session=True)
                _active[proc.pid] = (tag, step.name, proc, step.argv,
                                     time.time())
            _state_write()
            try:
                rc, timed_out = _wait_child(proc, step, tag)
            finally:
                _active.pop(proc.pid, None)
            log.write(f"=== [{step.name}] attempt {attempt} exit {rc} ===\n")
        last = _attempt_last(log_path, offset)
        wrote_any = wrote_any or bool(_wrote(stats0, step))
        if rc in step.ok_rc:
            if rc:
                say(f"[{tag}] note {step.name} exited {rc} — tolerated "
                    f"(partial-success exit code)")
            say(f"[{tag}] done {step.name} ({time.time() - t0:.0f}s)"
                + (f"  | {last[:110]}" if last else ""))
            _mark_built(step)
            _record(tag, step, "tolerated" if rc else "ok", attempt, t0, rc,
                    last, wrote=(wrote_any if step.out else None),
                    bytes_delta=_delta(stats_run0, step))
            return True
        # A NEGATIVE rc is death by signal, not a transient error, and it must
        # never be retried.  subprocess returns -N for signal N, which ok_rc
        # cannot express (it is a whitelist of ordinary exit codes), so on
        # 2026-08-26 careers_resolve -- which already carried ok_rc=(0, 1) --
        # was SIGTERM'd at 2h42m and the harness restarted it FROM SCRATCH.
        # Attempt 2 ran 9h24m and gated every fetcher in the run behind it.
        # A signal means an operator, the OOM killer, or a supervising timeout
        # decided this process should stop; running it again defies that.
        if rc < 0:
            say(f"[{tag}] {step.name} killed by signal {-rc} on attempt "
                f"{attempt} — NOT retrying (see logs/{step.name}.log)")
            for q in step.cleanup:
                # A killed builder leaves its half-written .tmp behind -- up
                # to 2.6 GB for bh_build_combined, on a disk the guard only
                # killed it for because it was nearly full.
                try:
                    os.remove(q)
                    say(f"[{tag}] removed {os.path.basename(q)} left by the "
                        f"killed {step.name}")
                except OSError:
                    pass
            _record(tag, step, "timeout" if timed_out else "killed",
                    attempt, t0, rc, last)
            return False
        if _stopping.is_set():
            _record(tag, step, "killed", attempt, t0, rc, last)
            return False
        # Network first: an attempt that failed because the wifi dropped says
        # nothing about the step, so it is neither charged nor compared.
        if holds < 5 and not _net_up():
            holds += 1
            if _hold_for_network(tag, step.name):
                attempt -= 1
                continue
        sig = _signature(last)
        progressed = (sig != prev_sig
                      or any(b > a for a, b in zip(sizes0, _sizes(step.out)))
                      or _fingerprints(step.progress) != fp0)
        prev_sig = sig
        if progressed:
            streak, streak_t0 = 1, time.time()
        else:
            streak += 1
        partial = bool(step.partial_ok and re.match(step.partial_ok, last))
        banked = banked or partial
        if streak >= STALL_REPEATS:
            if partial:
                say(f"[{tag}] STALLED {step.name}: {streak} identical "
                    f"attempts, no progress — its data is banked, accepted "
                    f"as PARTIAL  | {last[:100]}")
                _mark_built(step)
                _record(tag, step, "partial", attempt, t0, rc, last)
                return True
            if time.time() - streak_t0 >= STALL_MIN * 60:
                if banked:
                    say(f"[{tag}] STALLED {step.name}: {streak} identical "
                        f"failures re-trying stragglers after an earlier "
                        f"attempt banked its data — accepted as PARTIAL  | "
                        f"{last[:100]}")
                    _mark_built(step)
                    _record(tag, step, "partial", attempt, t0, rc, last)
                    return True
                say(f"[{tag}] STALLED {step.name}: {streak} identical "
                    f"failures over {(time.time() - streak_t0) / 60:.0f} min "
                    f"— giving up (see logs/{step.name}.log)  | {last[:100]}")
                _record(tag, step, "stalled", attempt, t0, rc, last)
                return False
        if attempt >= step.tries:
            break   # no nap after the LAST attempt: it only delays the tail
        if streak <= 1:
            nap = RETRY_SLEEP
        elif partial:
            nap = PARTIAL_SLEEP
        else:
            nap = STALL_SLEEPS[min(streak - 2, len(STALL_SLEEPS) - 1)]
        say(f"[{tag}] exit {rc} from {step.name} attempt {attempt}; "
            f"retrying in {nap:.0f}s"
            + (f" (identical x{streak})" if streak > 1 else ""))
        if _stopping.wait(nap):
            _record(tag, step, "killed", attempt, t0, rc, last)
            return False
    # Out of attempts.  A step whose LAST attempt still ended on its own
    # "some boards failed" line banked everything else: greenhouse/ashby at
    # tries=3 print a different failed-count each attempt, so they never form
    # an identical streak, and without this they would regress from
    # "tolerated" to FAILED.
    if banked:
        say(f"[{tag}] {step.name}: all {step.tries} attempts used; an attempt "
            f"banked its data and the rest re-tried stragglers — accepted as "
            f"PARTIAL  | {last[:90]}")
        _mark_built(step)
        _record(tag, step, "partial", attempt, t0, rc, last)
        return True
    say(f"[{tag}] GIVING UP on {step.name} after {step.tries} attempts "
        f"(see logs/{step.name}.log)")
    _record(tag, step, "failed", attempt, t0, rc, last)
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
# 112, not 96.  At 96 the two heaviest chains could NEVER overlap -- workday
# (64) + atstail-rest (48) = 112 -- so atstail-rest was admitted the exact
# second workday released, every run, as a pure serial tail.  Measured
# 2026-09-05: workday ended 05:11:20 and atstail-rest started 05:11:20,
# finishing 05:45:35 -- 34 min added to the critical path, while 32 permits sat
# free from 03:01 onward.  The two hit disjoint hosts (myworkdayjobs.com vs
# pinpoint/keka/join/taleo/wttj), so there is no shared-host penalty to
# provoke, and the cap was never a real egress ceiling anyway: it meters 15
# chains while 29 others run unmetered at weight 0.
GATE = Gate(int(os.environ.get("RUN_ALL_CONC") or 112))


def gated(tag, steps, dry, wait_event=None, done_event=None, post_steps=()):
    """Thread body: optional dependency wait, then a weighted permit.

    THE RULE: wait on the Event BEFORE acquiring, never while holding a permit.
    A chain that waits for its producer while holding capacity the producer
    needs is a deadlock, and the release must be in a finally — a leaked permit
    starves every remaining chain for the life of the run.

    post_steps run AFTER done_event is set and the permit released: the
    family's build sheet, moved here from the serial build tail so it runs
    while workday is still fetching.  After the event, so getro's sheet (300s)
    cannot delay the ats chain that waits on getro_done; recorded under
    "<tag>-sheet", so a failed sheet does not read as "getro chain failed".
    They run only when the chain itself succeeded -- otherwise the row-check
    sweep may still swap the family's data, and the tail builds it after.
    """
    def _run():
        if wait_event is not None:
            wait_event.wait()
        w = WEIGHT.get(tag, 0)
        GATE.acquire(w)
        try:
            ok = run_chain(tag, steps, dry, done_event=done_event)
        finally:
            GATE.release(w)
        _chain_ok[tag] = ok
        if post_steps and ok and not _stopping.is_set():
            run_chain(tag + "-sheet", post_steps, dry)
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
                with _failed_lock:
                    prev = _failed.get(tag)
                    _failed[tag] = f"{prev},{st.name}" if prev else st.name
                ok = False
                if not fail_soft:
                    break
    except Exception as e:  # noqa: BLE001 — a chain must never take the run down
        with _failed_lock:
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


def rotate_jsonls(fam):
    """[(live JSONL, refresh chain tag)] that rotation moves to .prev for a
    family.  A FUNCTION, not a module-level dict, so the paths resolve against
    BASE at call time -- run_all_selftest.py points BASE at a temp dir, and a
    dict built at import would still name the real files.  The chain tag is
    what the row check and the prune key on (a family's files can belong to
    different chains: portals is boards + phenom + bullhorn)."""
    # .get(fam, []) rather than [fam]: an unguarded lookup turns "somebody
    # added a family and forgot one of these dicts" into a KeyError crash
    # mid-run instead of a harmless no-op rotation.
    return {
        "getro": [(p("getro_all_jobs.jsonl"), "getro")],
        "lever": [(p("lever_all_jobs.jsonl"), "lever")],
        # The eight ats_all_*.jsonl are deliberately NOT rotated here any more.
        # ats_fetch owns its own generation as of 2026-08-16: it stages this
        # run's rows, rewrites each live file board-scoped (keeping every row
        # whose board it did not authoritatively re-fetch) and writes .prev
        # itself immediately before the atomic install.  Rotating here would
        # hand it an empty live file, so the board-scoped merge would have
        # nothing to preserve — which is exactly the hole backfill_from_prev()
        # existed to paper over.
        "portals": [(os.path.join(NORM, "boards_all.jsonl"), "boards"),
                    (os.path.join(NORM, "phenom_all.jsonl"), "phenom"),
                    (os.path.join(NORM, "bullhorn_all.jsonl"), "bullhorn")],
        "rippling": [(p("rippling_all_jobs.jsonl"), "rippling"),
                     (p("rippling_tenant_all_jobs.jsonl"), "rippling")],
        "personio": [(p("personio_all_jobs.jsonl"), "personio")],
        "yc": [(p("yc_all_jobs.jsonl"), "yc")],
        "dover": [(p("dover_all_jobs.jsonl"), "dover")],
        "notion": [(p("notion_all_jobs.jsonl"), "notion")],
        "greenhouse": [(p("greenhouse_all_jobs.jsonl"), "greenhouse")],
        "ashby": [(p("ashby_all_jobs.jsonl"), "ashby")],
        # slugs emits candidate lists, not job rows.
        "slugs": [],
        # Deliberately EMPTY.  zohorecruit is one of six families sharing
        # contract_norm/atstail.jsonl, so rotating that file here would
        # discard the other five (taleo, pinpoint, keka, wttj, join).  The
        # fetcher does its own scoped refresh instead — ATSTAIL_REFRESH
        # rewrites the JSONL keeping every row that is not this family's.
        "zohorecruit": [],
    }.get(fam, [])


def rotate_family(fam, dry):
    jsonls = rotate_jsonls(fam)
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
            # radancy joined 2026-09-04.  It is the ONE of the six 2026-08-20
            # families that satisfies BOTH halves of the trigger written
            # below: it has drained (coverage 80/83 boards, flat since 08-28,
            # so a bare run had 1 board to fetch and wrote 276 rows against
            # 34,099 on its first pass), and radancy_fetch.py:1152 _merge()
            # is a real board-scoped merge, so re-fetching rewrites its rows
            # instead of appending a second copy.
            "radancy",
            # manatal joined 2026-09-16, the second of the six to graduate,
            # and it satisfies the same two-part trigger radancy did.  It had
            # drained (3,816 of 4,291 boards reached, flat since 08-28, so a
            # bare run printed "0 to fetch" and finished in 0.1 min), and
            # manatal_fetch._merge() is now a real board-scoped merge ported
            # from radancy_fetch:1152 -- so re-fetching REPLACES a board's rows
            # instead of appending a second copy, which is the whole reason the
            # five were held back.
            #
            # What it was costing: 257,118 manatal rows in the master, 95.3% of
            # them in NO other pipe, and 0.9% dated this month.  The family had
            # stopped producing new postings entirely while still reporting ok.
            #
            # Wall clock is ~free.  MANATAL_BUDGET_MIN is 120 and this chain
            # runs parallel to workday_fetch, which took 176 min on 2026-09-15,
            # so manatal drains inside workday's shadow rather than on the
            # critical path.  Re-check that if workday ever gets faster.
            "manatal",
            # ...and the last four on 2026-09-16, which completes the set.
            # Each now calls cc.merge_board_scoped (lifted out of manatal's
            # local copy so a fifth near-identical clone never happened --
            # slug_common.py exists because three CDX discoverers drifted).
            # What they were costing, all dated August or earlier and 95-98%
            # exclusive to their own pipe: dayforce 88,493 rows / 0.0% dated
            # this month, neogov 26,626 / 0.0%, ukg 13,261 / 0.6%,
            # cornerstone 6,423 / 0.0% -- its state had not moved since
            # 2026-08-20, 27 days, while reporting ok every run.
            #
            # All four carry a 60-min budget against workday_fetch's measured
            # 176 min in the same parallel wave, so they drain in its shadow
            # and cost nothing on the critical path.
            #
            # cornerstone has a standing caveat that is NOT about the merge:
            # it has no *_discover_boards.py at all, so tenants/cornerstone.csv
            # has no writer but cornerstone_fetch.py itself.  Refreshing
            # un-stales the 63 boards it knows; it cannot find a 64th.
            "dayforce", "neogov", "ukg", "cornerstone",
            )]
        # ALL SIX 2026-08-20 FAMILIES HAVE NOW GRADUATED -- radancy 09-04,
        # manatal and then dayforce/neogov/ukg/cornerstone 09-16.  The bar was
        # always two-part and it has not moved: a family may be refreshed once
        # it has DRAINED its first pass and carries a BOARD-SCOPED MERGE.
        #
        # Keep the second half in mind before adding anything else here.
        # norm_dedupe is NOT a substitute, though it looks like one: it
        # collapses on (source, board, job_id), but its own docstring says
        # "a DISK and BUILD-TIME problem, not a correctness problem".  It
        # bounds growth; it cannot retire a posting that has vanished from the
        # board.  Only a merge can.  An append-only family added here re-fetches
        # every board and writes a SECOND copy of every row, for ever.
        #
        # The cost of the other error -- leaving a drained family out -- is what
        # 2026-09-16 measured: ~392k rows across the five, 95-98% of them in no
        # other pipe, every one of them dated August or earlier, from ~14,000
        # boards that had quietly stopped producing new postings while every
        # run reported ok.  Both mistakes are expensive; only one is visible.
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
                _state_append("state_backups", [q, q + ".prev.json"])
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
    #
    # rippling_detail_cache.json is never rotated for the same reason and one
    # more: it is keyed by posting uuid and createdOn is immutable per uuid,
    # so a cached posting never needs re-fetching.  Deleting it re-pays ~18k
    # detail requests -- about 20 minutes -- on every single refresh.
    for q, chain in jsonls:
        if os.path.exists(q):
            prev = q[:-len(".jsonl")] + ".prev.jsonl"
            say(f"[rotate] {os.path.basename(q)} -> {os.path.basename(prev)}"
                + (" (dry)" if dry else ""))
            if not dry:
                # Recorded BEFORE the move: a run killed between the two still
                # names every pair it may have moved, so --rollback and the
                # row check can never miss one.  The inode is what lets them
                # tell the file this run moved from an older .prev left under
                # the same name when the kill landed between the two.
                _state_append("rotated", [q, prev, chain, fam, _ino(q)])
                os.replace(q, prev)
    for q in states:
        if os.path.exists(q):
            say(f"[rotate] delete {os.path.basename(q)}"
                + (" (dry)" if dry else ""))
            if not dry:
                os.remove(q)


def max_collection_id():
    """The id ENUM_SLACK is measured from: the highest id ever SCANNED, not the
    highest one FOUND.

    Measuring from the highest FOUND id is a self-limiting watermark, and it
    froze this sweep silently for 13 days.  getro_enumerate_boards records
    every id it probes in <out>.scanned and skips those forever, 404s included.
    So one run scans to found_max + SLACK; the tail of that range 404s because
    those ids are not allocated yet; and from then on END lands exactly on the
    scanned frontier.  Zero ids remain, the step exits 0 in one second with an
    all-zeros counter, and every later --discover reports it healthy.

    Measured 2026-08-26: found_max 52727 + 3000 == scanned_max 55727, to the
    id.  Re-probing 52728..58727 turned up 6 real job boards the freeze had
    hidden (Matter Venture Partners, Creandum, Cowboy Ventures, ...) and moved
    the allocator frontier to 54344.

    Taking the scanned frontier instead makes the slack advance every run.
    """
    mx = 0
    path = p("getro_all_collections.jsonl")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    mx = max(mx, int(json.loads(line).get("id", 0)))
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
    scanned = path + ".scanned"
    if os.path.exists(scanned):
        with open(scanned, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.isdigit():
                    mx = max(mx, int(line))
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


# ---------------------------------------------------------------- run marker
# logs/run_all.state.json is the run's memory across a crash.  It exists
# because the runbook's hardest rule -- "never re-run bare after an
# interruption" -- was a thing the operator had to know: rotation moves every
# live JSONL to .prev at t=0, so a second bare run after a killed one rotates
# the HALF-FETCHED files over the last good generation.  It happened to the
# 2026-09-15 run (cancelled 18 min after rotating 12 JSONLs; rolled back by
# hand).  Now the next invocation reads the phase and refuses.
def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, default=str)
    os.replace(tmp, path)


def _state_write(**updates):
    """Update + persist the marker (atomic).  A no-op in a dry run: a
    --dry-run started beside a live run must never overwrite its marker."""
    if _run["dry"]:
        return
    with _state_lock:
        _state.update(updates)
        _state["updated_utc"] = _now()
        _state["children"] = [{"pid": pid, "step": v[1], "argv": v[3]}
                              for pid, v in list(_active.items())]
        try:
            os.makedirs(LOGS, exist_ok=True)
            _atomic_json(STATE_FILE, _state)
        except OSError as e:
            os.write(2, f"[run_all] WARN could not write marker: {e}\n"
                     .encode())


def _state_append(key, item):
    if _run["dry"]:
        return
    with _state_lock:
        _state.setdefault(key, []).append(item)
    _state_write()


def _load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError):
        return {}


def _set_phase(phase):
    _run["phase"] = phase
    _state_write(phase=phase)


_lock_fh = [None]


def _take_lock():
    """flock on logs/run_all.lock for the life of the process.  The kernel
    drops it when the process dies, however it dies, so "lock free" means "no
    run_all is alive" -- a pid in the marker cannot say that (pids recycle)."""
    os.makedirs(LOGS, exist_ok=True)
    fh = open(LOCK_FILE, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        return False
    _lock_fh[0] = fh
    return True


def _pid_runs(pid, argv):
    """True if `pid` is alive AND still runs the script the marker recorded,
    so a recycled pid is never mistaken for an orphan."""
    try:
        os.kill(int(pid), 0)
    except (OSError, ValueError, TypeError):
        return False
    try:
        cmd = subprocess.run(["ps", "-o", "command=", "-p", str(pid)],
                             capture_output=True, text=True,
                             timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return True
    script = os.path.basename((argv or [""])[0])
    return bool(script) and script in cmd


def _refuse(msg):
    say("REFUSED: " + msg)
    sys.exit(EXIT_REFUSED)


def _eff_phase(st):
    ph = st.get("phase")
    return st.get("interrupted_phase") if ph == "interrupted" else ph


def _past_rotation(st):
    """Did the run this marker describes leave rotated generations behind?

    Keyed on the ROTATED PAIRS, not only the phase name.  A --resume run
    carries the interrupted run's pairs but starts again at phase
    "discovery" (the chain always adds --discover); killed there, a
    phase-only check read "died before rotating anything" and let a bare run
    rotate the half-fetched files over the good .prev -- the exact loss the
    marker exists to stop (reproduced by the 2026-09-15 code review).  A run
    that reached "report" had built its master, so it is finished either way.
    """
    if not st or st.get("phase") in ("done", "rolled_back"):
        return False
    eff = _eff_phase(st)
    if eff == "report":
        return False
    return eff in PAST_ROTATION or bool(st.get("rotated"))


def _guard(args):
    """May this invocation run?  Returns the previous marker (to carry the
    rotated pairs and run_id into --resume), or exits EXIT_REFUSED."""
    prev = _load_state()
    if not _take_lock():
        _refuse(f"another run_all holds logs/run_all.lock (marker: pid "
                f"{prev.get('pid')}, phase {prev.get('phase')}, run "
                f"{prev.get('run_id')}).  Let it finish, or stop it with "
                f"`kill -TERM <pid>` (it stops its children cleanly).")
    orphans = [c for c in (prev.get("children") or [])
               if _pid_runs(c.get("pid"), c.get("argv"))]
    if orphans:
        _refuse("the previous run_all is gone but "
                f"{len(orphans)} of its step(s) are still running and writing"
                ": " + ", ".join(f"{c.get('step')} (pid {c.get('pid')})"
                                 for c in orphans)
                + ".  Wait for them, or stop them, then retry.")
    phase = prev.get("phase")
    if not prev or phase in ("done", "rolled_back"):
        return prev
    eff = _eff_phase(prev)
    if _past_rotation(prev):
        if args.rollback or args.resume or args.build_only:
            return prev
        n = len(prev.get("rotated") or [])
        _refuse(
            f"the previous run ({prev.get('run_id')}, started "
            f"{prev.get('started_utc')}) died during `{eff}` after rotating "
            f"{n} JSONL(s).  A bare run now would rotate those half-fetched "
            f"files over the last good generation.  Choose one:\n"
            f"    python3 run_all.py --resume [--discover]  continue that "
            f"fetch (keeps its state, reuses its run id)\n"
            f"    python3 run_all.py --build-only           rebuild from what "
            f"is on disk (right if it died in the build tail)\n"
            f"    python3 run_all.py --rollback             put its .prev "
            f"files back, then run bare\n"
            f"(from the chain: RUN_ALL_ARGS=--resume ./discover_full_chain.sh)")
    say(f"[guard] the previous run ({prev.get('run_id')}) stopped during "
        f"`{eff}`, before rotating anything — a fresh run is safe")
    return prev


def _rollback(prev, force):
    """Undo an interrupted run's rotation from its marker."""
    eff = _eff_phase(prev)
    if not _past_rotation(prev):
        _refuse(f"nothing to roll back — the last run's phase is "
                f"{prev.get('phase')!r} and it left no rotated pairs")
    if eff == "build" and not force:
        _refuse("the last run died in the BUILD tail, after its fetch had "
                "finished: its live JSONLs are complete.  --build-only "
                "finishes it; --rollback --force throws the whole fetch away.")
    pruned = {tuple(x[:2]) for x in (prev.get("pruned") or [])}
    n = 0
    for entry in prev.get("rotated") or []:
        live, prv = entry[0], entry[1]
        ino = entry[4] if len(entry) > 4 else None
        if (live, prv) in pruned:
            say(f"[rollback] {os.path.basename(prv)} was pruned by that run "
                f"— {os.path.basename(live)} keeps its new generation")
        elif not os.path.exists(prv):
            say(f"[rollback] no {os.path.basename(prv)} (row-check fallback "
                f"or never rotated) — {os.path.basename(live)} left as is")
        elif ino is not None and _ino(prv) != ino:
            # The pair is recorded BEFORE the move, so a run killed between
            # the two -- or a file rotated again since by something else --
            # leaves an OLDER .prev under that name.  Moving it would put an
            # older generation over the current live file.
            say(f"[rollback] {os.path.basename(prv)} is not the file that run "
                f"rotated (inode changed) — {os.path.basename(live)} left "
                f"as is")
        else:
            os.replace(prv, live)
            n += 1
            say(f"[rollback] {os.path.basename(prv)} -> "
                f"{os.path.basename(live)}")
    for st, bak in prev.get("state_backups") or []:
        if os.path.exists(bak):
            shutil.copy2(bak, st)
            say(f"[rollback] {os.path.basename(st)} restored from its "
                f".prev.json")
    _state.clear()
    _state.update(prev)
    _state_write(phase="rolled_back", rolled_back_utc=_now())
    say(f"[rollback] {n} JSONL(s) restored.  The fetch state files rotation "
        f"deleted are not restorable, and do not need to be: the next bare "
        f"run deletes them again.  run_all can run bare now.")


# ---------------------------------------------------------------- preflight
def _free_gb(path=None):
    try:
        return shutil.disk_usage(path or BASE).free / 2**30
    except OSError:
        return float("nan")


def _swap_gb():
    try:
        out = subprocess.run(["sysctl", "-n", "vm.swapusage"],
                             capture_output=True, text=True,
                             timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    m = re.search(r"used\s*=\s*([\d.]+)([MG])", out)
    if not m:
        return None
    v = float(m.group(1))
    return v / 1024 if m.group(2) == "M" else v


def _top_rss(n=5):
    """The n biggest memory users, summed per program: Chrome is ~30 helper
    processes, and only their sum explains a swap problem."""
    try:
        out = subprocess.run(["ps", "-axo", "rss=,comm="], capture_output=True,
                             text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return "?"
    agg = {}
    for line in out.splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2 or not parts[0].isdigit():
            continue
        name = os.path.basename(parts[1])
        if name.startswith("Google Chrome"):
            name = "Google Chrome"
        agg[name] = agg.get(name, 0) + int(parts[0])
    top = sorted(agg.items(), key=lambda kv: -kv[1])[:n]
    return ", ".join(f"{k} {v / 2**20:.1f}G" for k, v in top)


def _on_ac_power():
    try:
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True,
                             text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    if "AC Power" in out:
        return True
    if "Battery Power" in out:
        return False
    return None


def _preflight(args):
    """The checks the runbook asked the operator to remember before a 4-7h
    run.  Returns fatal messages; [] means go."""
    if os.environ.get("RUN_ALL_PREFLIGHT") == "0":
        say("[preflight] skipped (RUN_ALL_PREFLIGHT=0)")
        return []
    fatal = []
    ac, net, free, swap = _on_ac_power(), _net_up(), _free_gb(), _swap_gb()
    power = {True: "AC", False: "BATTERY"}.get(ac, "?")
    say(f"[preflight] power={power}"
        f"  network={'up' if net else 'DOWN'}  free={free:.1f}G  "
        f"swap={'?' if swap is None else f'{swap:.1f}'}G")
    if ac is False and not args.allow_battery:
        fatal.append("on BATTERY.  caffeinate cannot stop macOS Low Power "
                     "Sleep: the 2026-09-12 chain hibernated at 1% for 7h12m. "
                     " Plug in, or pass --allow-battery.")
    if not net:
        fatal.append("the network is unreachable")
    if free < MIN_FREE_GB and not args.force:
        fatal.append(f"only {free:.1f} GB free (< {MIN_FREE_GB:.0f}).  Free "
                     f"space first, or pass --force.")
    elif free < WARN_FREE_GB:
        say(f"[preflight] WARN {free:.1f} GB free < {WARN_FREE_GB:.0f}: the "
            f"build tail can draw ~5 GB plus swap growth.  The guard will "
            f"pause it and prune .prev if it has to.")
    if swap is not None and swap > SWAP_WARN_GB:
        say(f"[preflight] WARN swap is ALREADY {swap:.1f} GB — the leading "
            f"indicator of a disk crunch in the build tail (swapfiles share "
            f"this volume).  Biggest users: {_top_rss()}.  Close them before "
            f"the build tail starts (~4h in).")
    return fatal


def _caffeinate():
    """Hold a no-sleep assertion for exactly this process's life.  Every long
    step was already launched under `caffeinate -i` by hand; now nobody has to
    remember.  (It cannot stop Low Power Sleep on battery -- hence the AC
    check above.)"""
    try:
        subprocess.Popen(["caffeinate", "-i", "-s", "-w", str(os.getpid())],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         start_new_session=True)
    except OSError:
        pass


# ---------------------------------------------------------------- row check
def _count_lines(path):
    n = 0
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(16 << 20), b""):
                n += chunk.count(b"\n")
    except FileNotFoundError:
        return None
    return n


def _ino(path):
    try:
        return os.stat(path).st_ino
    except OSError:
        return None


def _rotated_pairs(chain=None):
    """(live, prev, chain, family, inode-or-None) for every rotated pair."""
    with _state_lock:
        pairs = [tuple(x[:4]) + ((x[4] if len(x) > 4 else None),)
                 for x in (_state.get("rotated") or [])]
    return [x for x in pairs if chain is None or x[2] == chain]


def _check_pair(live, prev, chain, fam, strict, ino=None):
    """Judge one rotated JSONL against its .prev and, below the floor, fall
    back to the .prev.  Exit status says nothing about data: the 2026-08-31
    run exited 0 while one himalayas 503 cut boards_all 97,997 -> 23,283 and
    flipped 42,875 triage cards to dead.  This compares rows."""
    with _critical:
        if live in _rowcheck:
            return _rowcheck[live]
        prev_n, live_n = _count_lines(prev), _count_lines(live)
        floor = max(SHRINK_FLOORS.get(fam, SHRINK_FLOOR),
                    SHRINK_FLOOR_STRICT if strict else 0.0)
        v = {"family": fam, "chain": chain, "prev": prev, "floor": floor,
             "prev_rows": prev_n, "live_rows": live_n}
        name = os.path.basename(live)
        ratio = (live_n or 0) / prev_n if prev_n else None
        if not prev_n:
            v["verdict"] = "new"
        elif live_n is not None and live_n >= floor * prev_n:
            v["verdict"] = "ok"
            say(f"[rowcheck] {name}: {live_n:,} rows vs {prev_n:,} in .prev "
                f"({ratio:.0%}) ok")
        elif not SHRINK_GUARD:
            v["verdict"] = "shrunk"
            say(f"ROWCHECK SHRUNK {name}: {live_n} vs {prev_n:,} "
                f"(RUN_ALL_SHRINK_GUARD=0 — building from it anyway)")
        elif ino is not None and _ino(prev) != ino:
            v["verdict"] = "prev-mismatch"
            say(f"ROWCHECK SHRUNK {name}: {live_n} vs {prev_n:,} in "
                f"{os.path.basename(prev)} — but that .prev is not the file "
                f"this run rotated (inode changed), so NOT falling back to it")
        else:
            v["verdict"] = "fallback"
            if live_n is not None:
                # link, THEN replace: the live name is never missing, so a
                # kill between the two leaves live=partial and prev intact.
                # logs/shrunk/ is outside every *.jsonl glob -- a sibling
                # <name>.shrunk.jsonl would be read as a new source by
                # contract_common.norm_files and the contract builders.
                os.makedirs(SHRUNK_DIR, exist_ok=True)
                shr = os.path.join(SHRUNK_DIR, f"{name}.{_run['run_id']}")
                if os.path.exists(shr):
                    os.remove(shr)
                try:
                    os.link(live, shr)
                except OSError:
                    shutil.copy2(live, shr)
                v["partial_kept"] = shr
            os.replace(prev, live)
            say(f"ROWCHECK FALLBACK {name}: "
                + (f"{live_n:,} rows vs {prev_n:,} in .prev ({ratio:.0%}) is "
                   f"below the {floor:.0%} floor"
                   if live_n is not None else f"missing after its fetch "
                   f"(.prev had {prev_n:,} rows)")
                + " — building from the PREVIOUS generation"
                + (f"; the partial is at logs/shrunk/{os.path.basename(shr)}"
                   if live_n is not None else ""))
        _rowcheck[live] = v
        _state_write(rowcheck=dict(_rowcheck))
        return v


def _rowcheck_fn(chain):
    """In-chain row check: runs right after the chain's last fetch step, so a
    fallback lands BEFORE the miner and the build sheet read the file."""
    def _fn():
        for live, prev, ch, fam, ino in _rotated_pairs(chain):
            _check_pair(live, prev, ch, fam, strict=False, ino=ino)
    return _fn


def _rowcheck_sweep():
    """After the refresh join: the pairs whose chain aborted before its own
    row check (or, under --build-only, every carried pair).  Strict floor --
    a chain that did not finish cleanly is the likeliest to have left a
    partial generation.  Never raises: a failure here must not cost the
    build tail."""
    for live, prev, ch, fam, ino in _rotated_pairs():
        if live in _rowcheck:
            continue
        try:
            _check_pair(live, prev, ch, fam,
                        strict=_chain_ok.get(ch) is not True, ino=ino)
        except Exception as e:  # noqa: BLE001
            say(f"[rowcheck] WARN could not check {os.path.basename(live)}: "
                f"{type(e).__name__}: {e} — building from it as is")


def _prune_prev(why):
    """Delete the .prev of every pair that is safe to lose: verdict ok AND
    its chain finished (ok or partial).  Never ats_fetch's own .prev files --
    it manages that generation itself.  The pairs go into the marker so
    --rollback --force knows they are gone."""
    freed = n = 0
    with _critical:
        for live, v in list(_rowcheck.items()):
            prev = v.get("prev")
            if v.get("verdict") != "ok" or _chain_ok.get(v.get("chain")) \
                    is not True or not prev or not os.path.exists(prev):
                continue
            try:
                sz = os.path.getsize(prev)
                os.remove(prev)
            except OSError as e:
                say(f"[prune] WARN {os.path.basename(prev)}: {e}")
                continue
            freed += sz
            n += 1
            _state_append("pruned", [live, prev])
    say(f"[prune] {why}: deleted {n} .prev.jsonl ({freed / 2**30:.1f} GB) "
        f"— only families whose fetch finished and passed the row check; "
        f"free now {_free_gb():.1f} GB")


def _prune_if_low():
    free = _free_gb()
    if free >= PRUNE_BELOW_GB:
        say(f"[prune] {free:.1f} GB free >= {PRUNE_BELOW_GB:.0f} — keeping "
            f"every .prev.jsonl")
        return
    _prune_prev(f"{free:.1f} GB free < {PRUNE_BELOW_GB:.0f} before "
                f"bh_build_combined")


# ---------------------------------------------------------------- watchers
def _disk_guard(stop):
    """Pause, then prune (the operator's standing choice, 2026-09-15).

    The 2026-09-14 build tail was saved by a hand-run Monitor doing exactly
    this: free fell to 4.85 GB with bh_build_combined at a 33 GB footprint,
    SIGSTOP held it for 92 min, and it finished normally.  No build step has a
    hard_timeout, so a pause cannot trip a watchdog.  Outside the build phase
    it only warns."""
    paused = {}          # pid -> (t_paused, step)
    pruned = False
    last_note = 0.0
    while not stop.wait(DISK_POLL_S):
        free, now = _free_gb(), time.time()
        if _run["phase"] != "build":
            if free < DISK_RESUME_GB and now - last_note > 300:
                say(f"[disk] WARN {free:.1f} GB free during "
                    f"{_run['phase']}")
                last_note = now
            continue
        builds = {pid: v for pid, v in list(_active.items())
                  if v[0] == "build"}
        for pid in list(paused):
            if pid not in builds:
                paused.pop(pid)
        if free < DISK_PAUSE_GB:
            for pid, v in builds.items():
                if pid in paused:
                    continue
                try:
                    os.kill(pid, signal.SIGSTOP)
                except OSError:
                    continue
                paused[pid] = (now, v[1])
                say(f"[disk] GUARD {free:.1f} GB free < {DISK_PAUSE_GB:.0f}"
                    f" — PAUSED {v[1]} (SIGSTOP)")
            if paused and not pruned:
                pruned = True
                _prune_prev(f"disk guard at {free:.1f} GB")
        elif paused and free > DISK_RESUME_GB:
            for pid, (t, name) in list(paused.items()):
                try:
                    os.kill(pid, signal.SIGCONT)
                except OSError:
                    pass
                say(f"[disk] GUARD {free:.1f} GB free — resumed {name} after "
                    f"{(now - t) / 60:.0f} min")
                paused.pop(pid)
        for pid, (t, name) in list(paused.items()):
            if now - t > DISK_KILL_MIN * 60:
                say(f"[disk] GUARD still {free:.1f} GB free after "
                    f"{DISK_KILL_MIN:.0f} min paused — TERMINATING {name}.  "
                    f"Its write is atomic, so its previous output stays in "
                    f"place; the build tail continues without it.")
                for sig in (signal.SIGTERM, signal.SIGCONT):
                    try:
                        os.kill(pid, sig)
                    except OSError:
                        pass
                paused.pop(pid)
            elif now - last_note > 300:
                sw = _swap_gb()
                say(f"[disk] {name} paused {(now - t) / 60:.0f} min | free "
                    f"{free:.1f}G swap {'?' if sw is None else f'{sw:.1f}'}G"
                    f" | biggest: {_top_rss()} — closing Chrome / idle "
                    f"sessions frees swap, which frees disk")
                last_note = now


def _heartbeat(stop, medians):
    """One line every HEARTBEAT_MIN.  run_all used to be silent between step
    boundaries, and a 124-min gap with only workday open looked exactly like
    the 08-10 hang (a child stuck in state U after a wifi drop)."""
    while not stop.wait(HEARTBEAT_MIN * 60):
        now = time.time()
        items = sorted(list(_active.values()), key=lambda v: v[4])
        parts = []
        for _tag, name, _proc, _argv, t0 in items:
            el, med = now - t0, medians.get(name)
            parts.append(f"{name} {el / 60:.0f}m"
                         + (" SLOW" if med and med >= 60 and el > 2 * med
                            else ""))
        sw, free = _swap_gb(), _free_gb()
        say(f"[hb] phase={_run['phase']} running {len(parts)}: "
            f"{', '.join(parts[:12]) or '-'}{' ...' if len(parts) > 12 else ''}"
            f" | free {free:.0f}G swap {'?' if sw is None else f'{sw:.1f}'}G")
        _state_write(free_gb=round(free, 1), swap_gb=sw)


_handling = [False]


def _on_signal(signum, _frame):
    """SIGTERM/SIGINT: stop every child cleanly, record where we were, exit.

    Runs in the MAIN thread, possibly while it holds _spawn_lock/_state_lock/
    _critical (all RLocks for that reason).  Never say(): _print_lock is a
    plain Lock and the main thread may be inside a print."""
    if _handling[0]:
        return
    _handling[0] = True
    code = 130 if signum == signal.SIGINT else 143
    try:
        _stopping.set()
        _spawn_lock.acquire(timeout=10)     # no new children from here on
        procs = [v[2] for v in list(_active.values())]
        os.write(2, (f"\n[run_all] signal {signum}: stopping {len(procs)} "
                     f"child process(es), up to {TERM_GRACE:.0f}s to flush "
                     f"state\n").encode())
        for pr in procs:
            # CONT too: a child the disk guard SIGSTOP'd cannot act on TERM.
            for sig in (signal.SIGTERM, signal.SIGCONT):
                try:
                    pr.send_signal(sig)
                except OSError:
                    pass
        # Poll waitpid(WNOHANG) directly, NOT Popen.wait(timeout).  During the
        # build tail the MAIN thread -- the one running this handler -- is
        # itself parked inside that child's Popen.wait(), holding its
        # non-reentrant _waitpid_lock, so Popen.wait here could never see the
        # exit and every Ctrl-C cost the full TERM_GRACE.  A chain thread that
        # reaps first just makes this see ChildProcessError: already gone.
        pending = {pr.pid: pr for pr in procs}
        deadline = time.time() + TERM_GRACE
        while pending and time.time() < deadline:
            for pid in list(pending):
                if pending[pid].returncode is not None:
                    pending.pop(pid)
                    continue
                try:
                    if os.waitpid(pid, os.WNOHANG)[0]:
                        pending.pop(pid)
                except ChildProcessError:
                    pending.pop(pid)
            if pending:
                time.sleep(0.1)
        for pr in pending.values():
            try:
                pr.kill()
            except OSError:
                pass
        _critical.acquire(timeout=60)       # let a fallback finish its pair
        # Only children that may STILL be alive (ignored TERM, got KILL) stay
        # listed: those are the orphans the next run's guard must wait for.
        for pid in list(_active):
            if pid not in pending:
                _active.pop(pid, None)
        if _state.get("run_id"):
            # No run_id = the marker for THIS run was never initialised (a
            # signal during the guard or preflight): the previous run's marker
            # is still on disk and must not be overwritten.
            _state_write(phase="interrupted", interrupted_phase=_run["phase"],
                         interrupted_utc=_now())
        os.write(2, (f"[run_all] stopped during {_run['phase']}; the next "
                     f"run_all will name the re-entry\n").encode())
    finally:
        os._exit(code)


# ---------------------------------------------------------------- summary
def _history_medians():
    """Median seconds per step over its last 5 successful runs (>= 3 needed)."""
    by = {}
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    h = json.loads(line)
                except ValueError:
                    continue
                for r in h.get("steps") or []:
                    if r.get("outcome") in ("ok", "tolerated", "partial") \
                            and r.get("secs"):
                        by.setdefault(r["step"], []).append(float(r["secs"]))
    except OSError:
        return {}
    return {k: statistics.median(v[-5:]) for k, v in by.items()
            if len(v) >= 3}


def _norm_sizes():
    out = {}
    try:
        for name in os.listdir(NORM):
            if name.endswith(".jsonl") and ".prev." not in name:
                try:
                    out[name] = os.path.getsize(os.path.join(NORM, name))
                except OSError:
                    pass
    except OSError:
        pass
    return out


def _log_grep(step_name, pattern):
    """Last line matching `pattern` in the tail of logs/<step>.log."""
    try:
        with open(os.path.join(LOGS, step_name + ".log"), "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 262144))
            body = f.read().decode("utf-8", "replace")
    except OSError:
        return ""
    hits = [ln.strip() for ln in body.splitlines() if re.search(pattern, ln)]
    return hits[-1] if hits else ""


def _verify_keys():
    """The runbook's "verify 0 blank / 0 dup job_keys DIRECTLY" step.  --out
    to logs/, NEVER the default: that is today's baseline_job_keys file, which
    stage 1 of the chain wrote from the OLD master."""
    if not os.path.exists(p("extract_baseline_keys.py")):
        return None
    out = os.path.join(LOGS, ".verify_keys.jsonl")
    try:
        r = subprocess.run([PY, "extract_baseline_keys.py", "--out", out],
                           cwd=BASE, capture_output=True, text=True,
                           timeout=1800)
        txt = r.stdout
    except (OSError, subprocess.SubprocessError) as e:
        say(f"KEYS: verification did not run ({e})")
        return None
    finally:
        try:
            os.remove(out)
        except OSError:
            pass

    def num(label):
        m = re.search(rf"{label}\s+([\d,]+)", txt)
        return int(m.group(1).replace(",", "")) if m else None
    rows, blank = num("master rows"), num("blank job_key")
    written, distinct = num("keys written"), num("distinct keys")
    dup = (written - distinct) if None not in (written, distinct) else None
    ok = "self-check OK" in txt
    say(f"KEYS: {rows if rows is None else f'{rows:,}'} master rows, "
        f"{blank} blank, {dup} dup"
        + ("" if ok else "  — the self-check did NOT pass"))
    return {"rows": rows, "blank": blank, "dup": dup, "self_check": ok}


def _summary(t0, t_disc_end, t_refresh_end, growth0, growth1, medians):
    end = time.time()
    with _records_lock:
        recs = list(_records)
    counts = {}
    for r in recs:
        counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
    fast = [f"{r['step']} {r['secs']:.0f}s (median {medians[r['step']]:.0f}s)"
            for r in recs
            if r["outcome"] in ("ok", "tolerated", "partial")
            and medians.get(r["step"], 0) >= 60
            and r["secs"] < 0.1 * medians[r["step"]]]
    flagged = [f"{r['step']}: {r['last'][:120]}" for r in recs
               if re.search(r"INCOMPLETE|BUDGET REACHED|still pending",
                            r.get("last") or "", re.I)]
    frozen = sorted(n for n in growth1 if growth1[n] == growth0.get(n, -1))
    # Steps that SUCCEEDED and wrote nothing.  This is the per-step version of
    # `frozen` and it is the one worth acting on: `frozen` measures bytes across
    # contract_norm and is mostly noise (legacy files superseded by *_all.jsonl
    # sit there for ever), while this names the step, so "ukg_fetch reported ok
    # and banked nothing" is unmissable.  Steps that declare no out= files are
    # excluded -- they are unknown, not idle.
    silent = [f"{r['step']} ({r['secs']:.0f}s, {r['outcome']}): "
              f"{(r['last'] or '')[:90]}"
              for r in recs
              if r.get("wrote") is False
              and r["outcome"] in ("ok", "tolerated", "partial")]
    ran = {r["step"] for r in recs}
    say("==== run summary ====")
    say("  outcomes: " + ", ".join(f"{k} {v}" for k, v in sorted(
        counts.items(), key=lambda kv: -kv[1])))
    for r in recs:
        if r["outcome"] not in ("ok", "skipped", "noop"):
            say(f"  {r['outcome'].upper():<10} {r['step']:<26} "
                f"{r['attempts']} att {r['secs'] / 60:6.1f} min | "
                f"{(r['last'] or '')[:90]}")
    for live, v in _rowcheck.items():
        if v["verdict"] != "new":
            say(f"  rowcheck {v['verdict']:<8} {os.path.basename(live):<32} "
                f"{v['live_rows']} vs {v['prev_rows']} (floor "
                f"{v['floor']:.0%})")
    if fast:
        say("  FAST (under 10% of the step's usual time — a no-op in "
            "disguise?): " + "; ".join(fast))
    for f in flagged:
        say(f"  capped: {f}")
    if silent:
        say("  SILENT (reported success, wrote no new rows — the shape that "
            "hid five frozen ATS families for a month):")
        for f in silent:
            say(f"    {f}")
    if frozen:
        say(f"  append-only files with NO new bytes this run: "
            f"{', '.join(frozen)}")
    cutoff = _log_grep("all_jobs_build", r"posted-date cutoff") \
        if "all_jobs_build" in ran else ""
    if cutoff:
        say(f"  {cutoff}")
    keys = None
    master = p("all_jobs_master.csv")
    rebuilt = os.path.exists(master) and os.path.getmtime(master) >= t0
    if rebuilt:
        keys = _verify_keys()
        # started_utc = when this master's data was FETCHED (the original
        # run's start for a --resume or --build-only re-entry): it is what
        # discover_full_chain.sh passes as the next new_jobs --since.
        _atomic_json(LAST_SUCCESS_FILE, {
            "run_id": _run["run_id"],
            "started_utc": _state.get("fetch_started_utc")
            or datetime.fromtimestamp(t0, tz=timezone.utc)
            .isoformat(timespec="seconds"),
            "finished_utc": _now()})
    summary = {
        "run_id": _run["run_id"], "argv": sys.argv[1:],
        "started_utc": datetime.fromtimestamp(t0, tz=timezone.utc)
        .isoformat(timespec="seconds"),
        "finished_utc": _now(),
        "minutes": {"total": round((end - t0) / 60, 1),
                    "discovery": round((t_disc_end - t0) / 60, 1),
                    "refresh": round((t_refresh_end - t_disc_end) / 60, 1),
                    "build": round((end - t_refresh_end) / 60, 1)},
        "outcomes": counts, "steps": recs, "rowcheck": _rowcheck,
        "fast": fast, "capped": flagged, "frozen_norm_files": frozen,
        "silent_steps": silent,
        "norm_growth_bytes": {n: growth1[n] - growth0.get(n, 0)
                              for n in growth1},
        "cutoff": cutoff, "keys": keys, "master_rebuilt": rebuilt,
        "failed": dict(_failed), "discovery_failed": dict(_discovery_failed),
    }
    try:
        _atomic_json(SUMMARY_FILE, summary)
        with open(HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps({k: summary[k] for k in (
                "run_id", "started_utc", "minutes", "outcomes", "steps")},
                default=str) + "\n")
    except OSError as e:
        say(f"[summary] WARN could not write: {e}")
    # Partials kept by earlier runs' fallbacks: inspected by now or never.
    try:
        for name in os.listdir(SHRUNK_DIR):
            if not name.endswith("." + str(_run["run_id"])):
                os.remove(os.path.join(SHRUNK_DIR, name))
    except OSError:
        pass


# ---------------------------------------------------------------- plan
def build_plan(args, fams):
    """Returns (discover_chains, refresh_chains, tail_steps, post_steps).
    Chains are (tag, [Step...]) run in parallel; the ats chain is keyed to
    start after the getro chain via events wired in main(); post_steps maps a
    refresh chain tag to the build sheet that runs after it."""
    discover, refresh = [], []
    # One id per RUN, shared by every supervised attempt of a step.  ats_fetch
    # uses it to take .prev once per run rather than once per attempt -- which
    # is why --resume reuses the interrupted run's id (main() sets it from the
    # marker): a fresh id would let the resumed run retake .prev from the
    # half-finished generation.
    run_id = _run["run_id"] or datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ")

    if args.discover:
        if "getro" in fams:
            end = max_collection_id() + ENUM_SLACK
            discover.append(("getro-disc", [Step(
                "getro_enumerate_boards",
                ["getro_enumerate_boards.py", "1", str(end),
                 "getro_all_collections.jsonl"],
                # Its progress is in the .scanned frontier, not its last line.
                out=(p("getro_all_collections.jsonl"),),
                progress=(p("getro_all_collections.jsonl.scanned"),))]))
        # PROBE STEPS ARE tries=2, ok_rc=(0,1).  Every one of them ends with
        # `raise SystemExit(1 if errors else 0)` -- exit 1 means "the board
        # list IS written, N candidates errored, re-run to retry them", a
        # partial success.  Without ok_rc they inherited FETCH_TRIES=40, and
        # on 2026-08-26 notion_probe_boards spent 44 minutes (07:05->07:49)
        # re-probing 5,649 candidates 40 times over 2 persistent errors, then
        # reported GIVING UP on a step that had succeeded on attempt 1.  Same
        # shape as the icims stall documented in the tail chain below.
        # CDX DISCOVER STEPS ARE tries=2 WITH A CEILING, as of 2026-09-02.
        # They used to inherit FETCH_TRIES=40, which was harmless only because
        # they were doing nothing: their state marked every CDX page done in
        # July, so each one issued ZERO requests and exited 0 in seconds.
        # cdx_common now windows the crawl on capture date, so these three talk
        # to web.archive.org again -- and web.archive.org 429s.  At tries=40,
        # with 6 internal retries backing off to 240s per page, one throttled
        # night would gate EVERY fetcher in the run (all discovery chains are
        # joined before any refresh starts).  Bounding it is safe because a
        # discovery failure is already non-fatal AND because a partial window
        # does not stamp last_crawl -- tomorrow's run re-asks for exactly the
        # same window, so nothing is lost by giving up early tonight.
        CDX_DISC_CEILING = float(os.environ.get("CDX_DISC_CEILING") or 2700)
        # The slug sweep walks 16 apexes (up to ~3.5k pages) rather than one
        # family's four, so it gets its own, larger ceiling -- but a ceiling.
        SLUG_DISC_CEILING = float(
            os.environ.get("SLUG_DISC_CEILING") or 5400)
        if "lever" in fams:
            discover.append(("lever-disc", [
                Step("lever_discover_boards", ["lever_discover_boards.py"],
                     needs=(p("getro_all_jobs.jsonl"),),
                     tries=2, hard_timeout=CDX_DISC_CEILING),
                Step("lever_probe_boards", ["lever_probe_boards.py"],
                     needs=(p("lever_candidates.csv"),),
                     tries=2, ok_rc=(0, 1)),
            ]))
        if "portals" in fams:
            discover.append(("bh-disc", [
                Step("bh_github_discover", ["bh_github_discover.py"],
                     tries=LOCAL_TRIES),
                # tries=2 (it was the default 40): discovery sits on the
                # barrier, so a deterministic crash must cost two attempts,
                # not the stall rule's ~20 min.  Same for the three below.
                Step("bh_extract_tokens", ["bh_extract_tokens.py"],
                     needs=(p("bh_urlscan_domains.txt"),), tries=2),
                Step("bh_merge_tokens", func=merge_bh_tokens,
                     tries=LOCAL_TRIES),
            ]))
        # The Wayback-free channel, wired in 2026-09-02.  Common Crawl +
        # urlscan.io over every family, sharing each family's OWN slug parser
        # so it can never disagree with the primary path about what a slug is.
        # It is not a fallback and is not conditional on CDX being down: these
        # are genuinely different corpora, and on the day it was written it
        # found 2,567 candidates the Wayback pipes had never produced.
        #
        # NOT gated on the slug sweep -- that coupling is the whole problem it
        # exists to solve.  It publishes to alt_ats_slugs.json, which
        # ats_fetch.load_slugs() already picks up via its *_ats_slugs.json
        # glob, so a slug found here is actually fetched.
        #
        # channels=cc,urlscan drops `norm` DELIBERATELY.  Measured on the
        # 2026-09-02 standalone run: norm scanned 50 files in 2,950s -- 90% of
        # the script's runtime -- to return 2 new greenhouse slugs and 0 lever.
        # The primary discoverers already grep those same corpora.  Run
        # `alt_discover_boards.py --channels norm` by hand if you want it.
        if "tail" in fams:
            discover.append(("alt-disc", [
                Step("alt_discover_boards",
                     ["alt_discover_boards.py",
                      "--family", "all", "--channels", "cc,urlscan"],
                     tries=2, ok_rc=(0, 1),
                     # 5400, not 2700: this step took 2,670s on 2026-09-02,
                     # thirty seconds inside the old ceiling, and it now also
                     # writes <family>_cc_slugs.txt per family.  It is the
                     # PRIMARY discovery channel since the Wayback crawl became
                     # opt-in, so starving it is no longer a cheap loss.  Its
                     # writes are per-family and grow-only, so a kill costs
                     # only the family in flight.
                     hard_timeout=float(os.environ.get("ALT_DISC_CEILING")
                                        or 5400)),
            ]))
        # SmartRecruiters' own job directory, added 2026-09-16.  SR slugs are
        # unguessable (ApexSystems3) and the archives only hold the ones
        # somebody linked; the directory enumerates the platform itself.
        # Measured before wiring: 338 keyword queries in 286s, 0 errors,
        # 1,621 companies of which 418 were not among our 7,389 -- each a
        # live board by construction.  Keyword state is a 14-day TTL, so a
        # 45-minute budget walks the plan over successive nights.  Writes go
        # through candidates_common.locked_merge + an atomic sidecar, which
        # is what makes it safe in `background` below.
        if "portals" in fams:
            discover.append(("vendor-dir", [
                Step("vendor_dir_discover",
                     ["vendor_dir_discover.py", "--budget-min",
                      os.environ.get("VENDOR_DIR_BUDGET_MIN") or "45"],
                     tries=2, ok_rc=(0, 1),
                     hard_timeout=float(os.environ.get("VENDOR_DIR_CEILING")
                                        or 3600)),
            ]))
        # Name -> board guessing for employers the master knows but no ATS
        # pipe reaches (slug_universe.py; US-leaning only, the user's call).
        # MEASURED, and small: ~3 right boards per 90 employers, so a full
        # pass over ~6,000 busy employers is ~200 boards.  Kept on a modest
        # budget; slug_channel_report.py's `guess` row decides whether it
        # stays -- turn it off if it holds under ~100 live boards after two
        # weeks.  First run also builds the universe cache (~7 min, one pass
        # over the master), which the budget covers.
        if "slugs" in fams and os.environ.get("SLUG_GUESS") != "0":
            discover.append(("slug-guess", [
                Step("slug_guess",
                     ["slug_guess.py", "--budget-min",
                      os.environ.get("SLUG_GUESS_BUDGET_MIN") or "45"],
                     tries=2, ok_rc=(0, 1),
                     hard_timeout=float(os.environ.get("SLUG_GUESS_CEILING")
                                        or 4200)),
            ]))
        # The shared Wayback slug sweep.  It emits <fam>_candidates.csv for
        # rippling/personio AND wayback_ats_slugs.json for ats_fetch.py, so
        # main() gates those chains on it finishing.
        if "slugs" in fams:
            discover.append(("slugs", [
                # BOUNDED since 2026-09-02.  This step had neither tries nor a
                # hard_timeout, so when Wayback started answering 429 it sat in
                # exponential backoff at ~8 minutes per page and gated the whole
                # refresh phase -- 10 downstream families wait on it.  It had to
                # be killed by hand mid-run.  A discovery step going quiet must
                # cost one ceiling, never the run.
                Step("slug_discover_boards", ["slug_discover_boards.py"],
                     tries=2, hard_timeout=SLUG_DISC_CEILING),
            ]))
        # Parses the workday-jobs / workday-site rows the slug sweep just
        # cached, probes the new (tenant, pod, site) triples through the CXS
        # API, and merges the live ones into tenants/workday.csv.  It MUST
        # wait on the sweep -- it reads slug_wayback_urls.txt -- hence the
        # entry in `waiters` below.
        if "slugs" in fams:
            discover.append(("workday-disc", [
                # Common Crawl, added 2026-09-02.  Workday was the last family
                # with a single discovery surface: alt_discover_boards skips it
                # on purpose (a board is a (tenant, pod, site) TRIPLE, not a
                # slug), so when Wayback 429'd for a whole run this pipe had no
                # channel at all.  It writes workday_cc_urls.txt, the second
                # raw dump workday_discover_boards now parses alongside the
                # Wayback one -- same "tag\turl" format, same parser, so all
                # three of its documented traps still apply to these rows.
                #
                # MEASURED before wiring: CC yields 3,881 boards / 1,714
                # tenants the Wayback sweep never had, against 2,611 new from
                # Wayback itself.  It is the STRONGER channel, not a fallback.
                # (Unprobed counts -- workday_discover_boards probes each one
                # through workday_fetch.post_jobs before admitting it.)
                #
                # ok_rc=(0,1) + a ceiling for the same reason rippling's CC
                # step carries them: the index throws transient 502/503 and a
                # bad night there must never fail the run.
                Step("workday_cc_discover", ["workday_cc_discover.py"],
                     tries=2, ok_rc=(0, 1),
                     hard_timeout=float(os.environ.get("WD_CC_CEILING")
                                        or 1800)),
                # BOUNDED because the CC channel above tripled its queue:
                # 2,611 new boards from Wayback alone, 6,492 from both.  This
                # step probes every new board live, and it runs in the
                # DISCOVERY phase -- which is joined before any fetcher starts
                # -- so an unbounded probe here gates the whole refresh.
                # Measured 2026-09-02: 60 boards probed in ~30s at 24 workers
                # (26 live, 43.3%), so 6,492 would be ~55 min of gate.
                #
                # --limit takes the most-seen first and merges what it probes,
                # so those boards are `known` next run and the queue DRAINS
                # over ~3 runs rather than being re-probed.  Same contract as
                # ats_fetch's budget: a cap is coverage paced across runs, not
                # coverage lost.  The ceiling is only a backstop.
                Step("workday_discover_boards",
                     ["workday_discover_boards.py", "--limit",
                      os.environ.get("WD_DISC_LIMIT") or "2500"],
                     tries=2, ok_rc=(0, 1),
                     hard_timeout=float(os.environ.get("WD_DISC_CEILING")
                                        or 2700)),
            ]))
        if "rippling" in fams:
            discover.append(("rippling-disc", [
                # Common Crawl is a second corpus, independent of the Wayback
                # sweep, and the only rippling surface still paying: +208
                # board slugs on 2026-08-24 against 3,029 already known.
                # Must run BEFORE the probe so the new slugs get classified
                # in the same pass.  ok_rc=(0,1) -- the CC index throws
                # transient 502/503 and a bad day there must not fail the run.
                Step("rippling_cc_discover", ["rippling_cc_discover.py"],
                     tries=2, ok_rc=(0, 1)),
                Step("rippling_probe_boards", ["rippling_probe_boards.py"],
                     needs=(p("rippling_candidates.csv"),),
                     tries=2, ok_rc=(0, 1)),
            ]))
        if "notion" in fams:
            discover.append(("notion-disc", [
                Step("notion_discover_boards", ["notion_discover_boards.py"],
                     tries=2),
                Step("notion_probe_boards", ["notion_probe_boards.py"],
                     needs=(p("notion_candidates.csv"),),
                     tries=2, ok_rc=(0, 1)),
            ]))
        if "tail" in fams:
            # paylocity, new 2026-09-16.  It rides the `tail` family because
            # its output is tenants/paylocity.csv, which atstail_fetch reads --
            # same shape as ukg/dayforce/neogov and discovered the same way, by
            # a dedicated *_discover_boards.py rather than ats_mine_tenants
            # (there is no corpus URL carrying a Paylocity tenant: 15,081 of
            # the 16,189 paylocity rows in the master are
            # /Recruiting/Jobs/Details/<numeric id>, which names no board).
            #
            # WIRED DELIBERATELY AND IMMEDIATELY.  A discovery script with no
            # caller is a standing failure mode in this repo, not a
            # hypothetical: tenants/cornerstone.csv has no writer but
            # cornerstone_fetch.py itself, discover_getro_companies.py has no
            # caller at all, and discover_full_chain.sh:52-56 names
            # fastapply_live.json as "the one still open".  This would have
            # been the next one.
            #
            # Budgeted, because the first sweep is ~9,000 sequential requests
            # at 3 rps (~50 min) against ONE shared Cloudflare-fronted host.
            # State is per-id so the sweep resumes, and once drained a run has
            # nothing to do -- the steady-state cost is a few requests for
            # whatever ids were added since.  Re-probing known ids needs
            # --refresh and is not on the nightly path.
            discover.append(("paylocity-disc", [
                Step("paylocity_discover_boards",
                     ["paylocity_discover_boards.py"],
                     env={"PAYLOCITY_BUDGET_MIN":
                          os.environ.get("PAYLOCITY_BUDGET_MIN", "30")},
                     tries=2, ok_rc=(0, 1),
                     # NOT T() -- that helper is defined further down
                     # build_plan, inside the refresh section.
                     out=(p("tenants/paylocity.csv"),),
                     hard_timeout=_ceiling({"PAYLOCITY_BUDGET_MIN": "30"})),
            ]))
        if "dover" in fams:
            discover.append(("dover-disc", [
                # Crashed on the same deterministic "bad-body" CDX error in
                # attempts 1 and 2 (dover_discover_boards.log:9-19).
                Step("dover_discover_boards", ["dover_discover_boards.py"],
                     tries=2),
                Step("dover_probe_boards", ["dover_probe_boards.py"],
                     needs=(p("dover_candidates.csv"),),
                     tries=2, ok_rc=(0, 1)),
            ]))
        # Board discovery for the two families whose fetch just moved into
        # run_all.  Reachable only via --discover on purpose: these are
        # multi-hour Wayback CDX crawls that find new BOARDS, and new POSTINGS
        # come from re-fetching the boards already known -- which is exactly
        # why refresh_gh_ashby.sh skipped them too.
        if "greenhouse" in fams:
            discover.append(("gh-disc", [
                Step("greenhouse_discover_boards",
                     ["greenhouse_discover_boards.py"],
                     tries=2, hard_timeout=CDX_DISC_CEILING),
                Step("greenhouse_probe_boards",
                     ["greenhouse_probe_boards.py"],
                     needs=(p("greenhouse_candidates.csv"),),
                     tries=2, ok_rc=(0, 1)),
            ]))
        if "ashby" in fams:
            discover.append(("ashby-disc", [
                Step("ashby_discover_boards", ["ashby_discover_boards.py"],
                     tries=2, hard_timeout=CDX_DISC_CEILING),
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
            # neogov defaults its Wayback crawl ON (neogov_discover_boards
            # .py:115, NEOGOV_CDX), and it was missed by 9ed55fd, the 2026-09-02
            # commit that made the crawl opt-in for slugs/lever/greenhouse/
            # ashby.  Measured 2026-09-05: 714s and 345,309 CDX lines across 14
            # batches -- absorbing four 504s, a 503 and two 429s -- to go from
            # "3014 candidates already on file" to "3014 candidates total", then
            # "probing 0 candidates".  Zero new candidates.  Same opt-in
            # treatment as the other four; NEOGOV_CDX=1 brings it back.
            _disc_env = {"neogov": {"NEOGOV_CDX": os.environ.get(
                "NEOGOV_CDX", "0")}}
            for _tag, _script in (("ukg", "ukg_discover_boards.py"),
                                  ("manatal", "manatal_discover_boards.py"),
                                  ("dayforce", "dayforce_discover_boards.py"),
                                  ("neogov", "neogov_discover_boards.py"),
                                  ("radancy", "radancy_discover_boards.py"),
                                  ("phenom", "phenom_discover_boards.py")):
                discover.append((_tag + "-disc", [
                    Step(_script[:-3], [_script], tries=2, ok_rc=(0, 1),
                         env=_disc_env.get(_tag)),
                ]))
        # Company-owned careers subdomains (jobs.<company>.com).  This is the
        # only discovery channel here that is not inside the closed loop:
        # every enterprise ATS family gets its tenants from ats_mine_tenants
        # mining the finished master, so it can never contain an employer
        # another source did not already hand us.  careers_hosts.py reads the
        # Common Crawl host graph instead -- 246M hostnames, 157,700 of them
        # careers subdomains, against 3,142 the corpus had ever seen.
        #
        # It MUST come before the `prelude` chain that runs ats_mine_tenants:
        # resolved boards are written with src="careers", and that file
        # preserves exactly the rows whose src is not "corpus".  Discover
        # chains run ahead of refresh chains, so the ordering holds.
        #
        # The resolve pass is bounded and resumable -- 157k hosts is many
        # hours -- so successive --discover runs work through the backlog.
        # OPT-IN, and off by default since 2026-08-26.  Every *-disc chain is
        # joined before ANY refresh starts, so this step's budget is the whole
        # run's critical path.  At CAREERS_BUDGET_RUNALL=28800 it cost 12.1h of
        # a 15.5h run -- 8h of budget plus a 4.1h drain -- and returned 660
        # resolved hosts, 5.4% of its input, and 0 hosts to radancy.  The sweep
        # is resumable and its yield is slow-moving, so it belongs in a WEEKLY
        # long run, not in front of every night's postings:
        #
        #   CAREERS_DISC=1 CAREERS_BUDGET_RUNALL=28800 ./discover_full_chain.sh
        if "tail" in fams and os.environ.get("CAREERS_DISC") == "1":
            discover.append(("careers-disc", [
                Step("careers_hosts", ["careers_hosts.py"], tries=2,
                     ok_rc=(0, 1)),
                # tries=1, and a hard ceiling of budget + 20 min.  Both are
                # load-bearing: the 2026-08-26 block was a SIGTERM'd attempt 1
                # being retried from scratch, then attempt 2 overrunning its
                # budget by 4.1h in a drain the budget does not cover.
                Step("careers_resolve",
                     ["careers_resolve.py", "--budget",
                      os.environ.get("CAREERS_BUDGET_RUNALL", "5400")],
                     needs=(p("careers_candidates.csv"),), tries=1,
                     ok_rc=(0, 1),
                     hard_timeout=float(os.environ.get(
                         "CAREERS_BUDGET_RUNALL", "5400")) + 1200),
            ]))
        if "yc" in fams:
            discover.append(("yc-disc", [
                # Its stub guard exits after ~5 min of its own retries, so the
                # old 40 tries could hold the barrier for hours.
                Step("yc_discover_boards", ["yc_discover_boards.py"],
                     tries=2),
            ]))

    if "getro" in fams:
        refresh.append(("getro", [
            Step("getro_check_live",
                 ["getro_check_live.py", "getro_all_collections.jsonl"],
                 needs=(p("getro_all_collections.jsonl"),),
                 tries=LOCAL_TRIES),
            Step("getro_fetch_jobs", ["getro_fetch_jobs.py"],
                 needs=(p("getro_job_boards.csv"),),
                 out=(p("getro_all_jobs.jsonl"),)),
            # Crashes and resumes per batch, so its 40 tries do real work --
            # the growth of the shared JSONL is what tells the stall rule so.
            Step("getro_fetch_pdl", ["getro_fetch_pdl.py"],
                 out=(p("getro_all_jobs.jsonl"),)),
            # Before the miner: a fallback to .prev must land before
            # contract_mine_getro reads the file.
            Step("rowcheck_getro", func=_rowcheck_fn("getro"), tries=1),
            Step("contract_mine_getro", ["contract_mine_getro.py"],
                 needs=(p("getro_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        ]))
    if "lever" in fams:
        refresh.append(("lever", [
            Step("lever_fetch_jobs", ["lever_fetch_jobs.py"],
                 needs=(p("lever_job_boards.csv"),),
                 out=(p("lever_all_jobs.jsonl"),), partial_ok=PARTIAL_BOARDS),
            Step("rowcheck_lever", func=_rowcheck_fn("lever"), tries=1),
            Step("contract_mine_lever", ["contract_mine_lever.py"],
                 needs=(p("lever_all_jobs.jsonl"),), tries=LOCAL_TRIES),
        ]))
    if "rippling" in fams:
        refresh.append(("rippling", [
            # The step that stormed on 2026-09-15: one board answering HTTP
            # 400 on every attempt.  partial_ok ends that in ~3 min instead of
            # 40 x 50s, and lets rippling_tenant_fetch below still run.
            Step("rippling_fetch_jobs", ["rippling_fetch_jobs.py"],
                 needs=(p("rippling_job_boards.csv"),),
                 out=(p("rippling_all_jobs.jsonl"),),
                 partial_ok=PARTIAL_BOARDS),
            # *.rippling-ats.com: a different key space and a different
            # product (HTML only, no JSON API).  ok_rc=(0,1) because a few
            # tenant hosts are always mid-retirement.
            Step("rippling_tenant_fetch", ["rippling_tenant_fetch.py"],
                 needs=(p("rippling-tenant_candidates.csv"),),
                 tries=2, ok_rc=(0, 1)),
            Step("rowcheck_rippling", func=_rowcheck_fn("rippling"), tries=1),
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
            # ATSTAIL_REFRESH on the FIRST attempt only (see Step.first_env):
            # in env= it dropped every zohorecruit row and state key on every
            # retry.  No `out`: atstail.jsonl is shared with atstail-rest, so
            # its growth says nothing about this step.
            Step("zoho_fetch", ["atstail_fetch.py"],
                 env={"ATSTAIL_ONLY": "zohorecruit"},
                 first_env={"ATSTAIL_REFRESH": "zohorecruit"},
                 partial_ok=PARTIAL_BOARDS,
                 needs=(os.path.join(BASE, "tenants", "zohorecruit.csv"),)),
        ]))
    if "personio" in fams:
        # personio_fetch_jobs doubles as the probe (see its docstring): the
        # host rate-limits hard enough that a separate probe pass would double
        # the load for no new information.
        refresh.append(("personio", [
            Step("personio_fetch_jobs", ["personio_fetch_jobs.py"],
                 needs=(p("personio_candidates.csv"),),
                 out=(p("personio_all_jobs.jsonl"),),
                 partial_ok=PARTIAL_BOARDS),
            Step("rowcheck_personio", func=_rowcheck_fn("personio"), tries=1),
        ]))
    if "yc" in fams:
        refresh.append(("yc", [
            Step("yc_fetch_jobs", ["yc_fetch_jobs.py"],
                 needs=(p("yc_candidates.csv"),),
                 out=(p("yc_all_jobs.jsonl"),), partial_ok=PARTIAL_BOARDS),
            Step("rowcheck_yc", func=_rowcheck_fn("yc"), tries=1),
        ]))
    if "notion" in fams:
        refresh.append(("notion", [
            Step("notion_fetch_jobs", ["notion_fetch_jobs.py"],
                 needs=(p("notion_job_boards.csv"),),
                 out=(p("notion_all_jobs.jsonl"),)),
            Step("rowcheck_notion", func=_rowcheck_fn("notion"), tries=1),
        ]))
    if "dover" in fams:
        refresh.append(("dover", [
            Step("dover_fetch_jobs", ["dover_fetch_jobs.py"],
                 needs=(p("dover_job_boards.csv"),),
                 out=(p("dover_all_jobs.jsonl"),), partial_ok=PARTIAL_BOARDS),
            Step("rowcheck_dover", func=_rowcheck_fn("dover"), tries=1),
        ]))
    # Fetch only.  ~8,400 greenhouse boards and ~3,650 ashby boards, both
    # resumable, both independent of everything else in the run -- so they get
    # their own chains and drain in parallel with the rest.  tries mirrors
    # refresh_gh_ashby.sh's retry loop.
    # partial_ok, NOT ok_rc=(0, 1) any more: that also accepted a traceback
    # as success.  Besides a crash, their exit-1 paths are the boards-failed
    # summary, which partial_ok matches (also at tries exhaustion, since the
    # failed count changes between attempts, and also when a later attempt
    # crashes after one banked its data), and ashby's missing-candidates
    # guard (ashby_fetch_jobs.py:168) -- which now correctly FAILS instead of
    # passing as "tolerated" with no data fetched.
    if "greenhouse" in fams:
        refresh.append(("greenhouse", [
            Step("greenhouse_fetch_jobs", ["greenhouse_fetch_jobs.py"],
                 tries=3, out=(p("greenhouse_all_jobs.jsonl"),),
                 partial_ok=PARTIAL_BOARDS),
            Step("rowcheck_greenhouse", func=_rowcheck_fn("greenhouse"),
                 tries=1)]))
    if "ashby" in fams:
        refresh.append(("ashby", [
            Step("ashby_fetch_jobs", ["ashby_fetch_jobs.py"],
                 tries=3, out=(p("ashby_all_jobs.jsonl"),),
                 partial_ok=PARTIAL_BOARDS),
            Step("rowcheck_ashby", func=_rowcheck_fn("ashby"), tries=1)]))
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
            # The 2026-08-31 guard: one himalayas 503 cut this file 97,997 ->
            # 23,283 with exit 0.  Now that generation is caught here and the
            # build uses .prev instead.
            Step("rowcheck_boards", func=_rowcheck_fn("boards"), tries=1),
        ]))
        refresh.append(("phenom", [
            Step("phenom_fetch", ["phenom_fetch.py"],
                 # 25 min is ~4x headroom: the full 127-board run measures
                 # 384s at RPS=6/WORKERS=8.  Boards not reached inside the
                 # budget stay PENDING rather than being recorded empty.
                 env={"PHENOM_KEEP_ALL": "1", "PHENOM_BUDGET_MIN": "25"},
                 pre=truncate(os.path.join(NORM, "phenom_all.jsonl")),
                 tries=3),
            Step("rowcheck_phenom", func=_rowcheck_fn("phenom"), tries=1),
        ]))
        refresh.append(("bullhorn", [
            Step("bh_ensure_tokens", func=ensure_bh_tokens,
                 tries=LOCAL_TRIES),
            Step("bh_fetch", ["bh_fetch.py"], env={"BH_KEEP_ALL": "1"},
                 needs=(p("bh_tokens_merged.txt"),),
                 out=(os.path.join(NORM, "bullhorn_all.jsonl"),)),
            Step("rowcheck_bullhorn", func=_rowcheck_fn("bullhorn"), tries=1),
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
                      # Per-job detail is ON again, 2026-09-16, because it is
                      # now BOUNDED PER RUN instead of per board.  The old
                      # reasoning was sound and is preserved here: detail turned
                      # each live board into 1 + up to 80 requests at 6 rps --
                      # ~75,000 requests, ~3.5h, against a 25-minute budget --
                      # so it was pinned off rather than left to ats_fetch's
                      # board-count switch.
                      #
                      # What that bought, measured 2026-09-15: jazzhr 138,829
                      # rows at 1.8% dated, bamboohr 90,651 at 0.7%.  Those are
                      # the two LARGEST board inventories in the pipeline
                      # (24,254 and 14,830 boards) and triage/lanes.py:441 sorts
                      # undated rows last in every lane, so ~229k rows exist and
                      # are unreachable in practice.  datePosted lives only in
                      # the per-job payload, so no date is recoverable any other
                      # way.
                      #
                      # ats_fetch now fetches detail ONLY for postings with no
                      # date yet and stops at ATS_DETAIL_BUDGET per run, so the
                      # steady state is ~5k requests (~14 min) and the one-time
                      # backfill drains over ~17 runs instead of spending 3.5h
                      # in one step.  The step says "INCOMPLETE" while postings
                      # remain undated, which run_all's summary surfaces under
                      # `capped:`.  Raise the budget if that figure stops
                      # falling; set JZ_NO_DETAIL=1 to put it back the old way.
                      "ATS_DETAIL_BUDGET": os.environ.get(
                          "ATS_DETAIL_BUDGET", "8000"),
                      "BH_DETAIL": "1",
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
                 tries=3, ok_rc=(0, 1), hard_timeout=_ceiling(
                     {"ATS_BUDGET_MIN": "90"})),
        ]))
        # RE-ENABLED 2026-09-02 at 30 min, exactly as the note below
        # prescribed.  The precondition it named -- "leave it off until the
        # penalty ages out" -- was measured, not assumed: 25 boards probed
        # serially at 1 rps returned 25x HTTP 200, zero 429, zero 403, at 51.7
        # boards/min.  On 2026-08-26 the same host gave 7.9 boards/min with 476
        # rate-limit backoffs, so the per-IP penalty is gone, not merely
        # quieter.  Budget goes back to 30, NOT the 90 that made it worse; at
        # the measured rate 30 min covers ~1,550 boards against the 707 that 90
        # minutes bought under the penalty.  Set ATS_WORKABLE=0 to disable.
        # Re-measure boards/min from the run log before raising this again.
        # The 30 -> 90 budget rise below did the OPPOSITE of what it intended:
        # throughput fell from ~26 boards/min at 30 min (823/16,837) to 7.9 at
        # 90 min (707/15,950), with 476 rate-limit backoffs.  apply.workable.com
        # is one shared Cloudflare host whose per-IP penalty DEEPENS under
        # sustained load, so spending longer on it buys less and taxes every
        # other pipe's share of the same IP.  The note below already predicted
        # this ("throughput does not scale linearly with the budget").  Leave it
        # off until the penalty ages out, then re-enable at 30 and measure
        # boards/min before touching the number again.
        # Workable's own US job directory as a FEED (workable_dir_fetch.py),
        # added 2026-09-16.  It keeps only employers ats-workable does not
        # already fetch by board -- their jobs.workable.com/view keys can never
        # dedupe against apply.workable.com keys -- so it is purely additive.
        # jobs.workable.com, not apply.workable.com: a different host from the
        # per-IP penalty described below, walked single-threaded at ~1 rps.
        # The walk is a cursor (~3,500 pages, ~70 min); a budgeted run resumes
        # the next night and the finished walk replaces workable_dir.jsonl in
        # one os.replace.
        if os.environ.get("WORKABLE_DIR") != "0":
            refresh.append(("workable-dir", [
                Step("workable_dir_fetch", ["workable_dir_fetch.py"],
                     env={"WORKABLE_DIR_BUDGET":
                          os.environ.get("WORKABLE_DIR_BUDGET") or "5400"},
                     tries=2, ok_rc=(0, 1),
                     hard_timeout=float(os.environ.get(
                         "WORKABLE_DIR_BUDGET") or 5400) + 1800),
            ]))
        if os.environ.get("ATS_WORKABLE") != "0":
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
                      "ATS_BUDGET_MIN": "30",
                      "ATS_DELIST": "all",
                      "ATS_DELIST_MODE": "flag"},
                 # Parent-enforced ceiling.  ATS_BUDGET_MIN stops this step
                 # SUBMITTING boards; it does not bound the drain of the ones
                 # already in flight, and workable's drain is precisely what
                 # cost 5,867s against a 90-minute budget on 2026-08-26.
                 needs=(p("getro_ats_slugs.json"),), tries=2,
                 hard_timeout=_ceiling({"ATS_BUDGET_MIN": "30"})),
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
                # 45 -> 180.  The board universe went 949 -> 1,769 -> 5,210
                # while this budget stood still, so the run covered a steadily
                # smaller FRACTION of it: 2,303/5,210 on 2026-08-26.  Measured
                # 200, raised from 180 on 2026-09-07.  The "51 boards/min
                # -> ~102 min for a full sweep" note below it went stale as the
                # board list grew: measured 2026-09-05 the rate was 40.5
                # boards/min against 7,819 boards, so a full sweep needs ~193
                # min and 180 stopped it at 7,295 with 524 still pending.  200
                # finishes the sweep with margin; _ceiling() adds the 30-min
                # grace on top automatically, so the parent clock follows.  Unlike workable there is no shared-host penalty to
                # provoke -- these are per-tenant pods and the whole run logged
                # 6 edge backoffs (wd1: 4, wd5: 2) -- and at ~328 rows/board
                # this is the highest intake-per-minute in the repo.
                ("workday", "workday_fetch.py",
                 {"WD_BUDGET_MIN": os.environ.get("WD_BUDGET_MIN", "200")},
                 "workday"),
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
                # (radancy_fetch.py:227 tests == "1"), so WITHOUT this env the
                # fetcher silently writes only its contingent slice -- the
                # exact failure that left 8 remote boards contributing 256 rows
                # to the master until BOARDS_KEEP_ALL was found.  The other
                # five default ON; they are passed explicitly anyway so the
                # plan line shows what the run is actually asking for.
                ("ukg", "ukg_fetch.py",
                 {"UKG_KEEP_ALL": "1", "UKG_BUDGET_MIN": "60"}, "ukg"),
                # 60 -> 120.  It hit the budget with tenants still pending
                # every run; the watchdog added 2026-08-26 is what makes a
                # bigger number safe, since the deadline can no longer be
                # deferred by a board that will not finish.
                # 120 -> 140, measured 2026-09-16.  Now that its state
                # refreshes, manatal re-fetches all 3,819 live boards every run
                # and needs ~126 min: the run got to 3,815/3,819 -- FOUR boards
                # short -- before the watchdog fired at 125 min, hard-exited
                # past the merge, and left 255,188 rows stranded in a 159MB
                # .part.  The work was done and then thrown a run into the
                # future for the sake of four boards.  It stays inside
                # workday_fetch's 134-min shadow at 140.
                ("manatal", "manatal_fetch.py",
                 {"MANATAL_KEEP_ALL": "1",
                  "MANATAL_BUDGET_MIN": "140"}, "manatal"),
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
                # out= is what makes the zero-growth check in _record work.
                # Every one of these fifteen writes contract_norm/<tag>.jsonl,
                # so the tag IS the filename -- verified for all fifteen, not
                # assumed.  Without this the run cannot tell "fetched nothing"
                # from "fetched plenty", which is precisely how five of them
                # reported ok for a month while collecting nothing.
                Step(tag + "_fetch", [script], env=env,
                     soft_needs=(T(tenants),), tries=3, ok_rc=(0, 1),
                     out=(os.path.join(NORM, tag + ".jsonl"),),
                     hard_timeout=_ceiling(env))]))
        # The five atstail families run_all does not already drive as
        # zohorecruit.  No ATSTAIL_REFRESH: that rewrites the shared JSONL to
        # drop a family's rows before re-fetching, so a family that has started
        # refusing loses its rows outright.
        refresh.append(("atstail-rest", [
            # paylocity joined 2026-09-16 -- a brand-new family, not a
            # rescued one.  It is the largest un-piped ATS in the corpus
            # (host_census ranks it first: 1,440 boards, 16,189 rows) and every
            # row we had came through getro, i.e. only boards that happened to
            # sit on a VC job board.  It rides atstail rather than getting its
            # own *_fetch.py because atstail already reads tenants/<fam>.csv,
            # which is exactly the shape paylocity_discover_boards.py produces.
            #
            # Boards are keyed by a v4 UUID no crawl can enumerate; discovery
            # walks the vendor's legacy integer route instead.  Of the first
            # 115 boards found, 110 (95.7%) are employers that appear ZERO
            # times in the master, and every row carries a real employer
            # PublishedDate -- against a corpus average near 19%.
            Step("atstail_fetch_rest", ["atstail_fetch.py"],
                 env={"ATSTAIL_ONLY":
                      "pinpoint,keka,join,taleo,wttj,paylocity",
                      "ATSTAIL_BUDGET_MIN": "0"},
                 soft_needs=(T("pinpoint"),), tries=3, ok_rc=(0, 1),
                 out=(os.path.join(NORM, "atstail.jsonl"),))]))

    if "remote" in fams:
        # Budgets are MANDATORY and the two conventions collide:
        #   float(env or D) -- "0" is truthy, so 0 is a ZERO-SECOND deadline
        #                      (THEMUSE_BUDGET, CJ_DEADLINE, W3_*, RL_*)
        #   int(env or 0)   -- 0 means UNLIMITED (JG_MAX_MIN, *_BUDGET_MIN)
        # Leaving JG_MAX_MIN unset is what let jobgether run 8,977s.
        # TTL_DAYS is a RE-WALK interval, and both of these shipped with one
        # longer than the interval between runs -- so the expiry that was added
        # to un-freeze them still left them no-oping most nights.  Measured
        # 2026-08-26: themuse's 12,266 cells were ~5-6.5d old against a 7d TTL
        # and careerjet's 1,272 partitions 2.78d old against a 3d TTL, so BOTH
        # printed "0 queries"/"queued 0 partitions" and exited in ten seconds.
        # Set here rather than left to the module default so the plan line
        # shows the cadence the run is actually asking for.
        refresh.append(("themuse", [
            Step("themuse_fetch", ["themuse_fetch.py"],
                 env={"THEMUSE_BUDGET": "5400",
                      "THEMUSE_TTL_DAYS": "2"}, tries=3)]))
        refresh.append(("careerjet", [
            Step("careerjet_fetch", ["careerjet_fetch.py"],
                 env={"CJ_DEADLINE": "3600",
                      "CJ_TTL_DAYS": "1"}, tries=3)]))
        refresh.append(("jobgether", [
            # hard_timeout added 2026-09-04.  This step declared a 90-minute
            # budget and had NO parent ceiling, so JG_MAX_MIN was honour-system
            # only -- the same shape that let manatal sit 2h47m past a 60-min
            # budget.  _ceiling() gives the child its own budget plus the
            # standard grace before the parent takes it out.
            Step("jobgether_fetch", ["jobgether_fetch.py"],
                 env={"JG_MAX_MIN": "90"}, tries=3,
                 hard_timeout=_ceiling({"JG_MAX_MIN": "90"}))]))
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
        # (usajobs_fetch.py:658) and US federal postings are squarely in
        # scope.  It sat in a DEFAULT_OFF family with no handler, which is
        # why contract_norm/usajobs.jsonl froze on 2026-08-14.  It resumes
        # and appends by id, so "remote"'s resume-only rotation is correct
        # and it must NOT be truncated.
        refresh.append(("usajobs", [
            Step("usajobs_fetch", ["usajobs_fetch.py"],
                 env={"USAJOBS_DAYS": "90"}, tries=3, ok_rc=(0, 1))]))
        # smallremote was a pure orphan -- in no family at all, referenced by
        # no .py and no .sh.  It writes through cc.Writer, which opens "a", so
        # something has to stop a re-run appending a second copy of the board.
        # That used to be a truncate() pre hook here, which was correct only if
        # the fetcher then rewrote every source -- and it does not: it keeps a
        # per-source "done" watermark and skips.  truncate-then-skip-everything
        # zeroed contract_norm/smallremote.jsonl on EVERY run from 2026-08-14 to
        # 08-25 (0 rows, 2s, exit 0, ~8,126 rows lost).  smallremote_fetch.py now
        # does a source-scoped rewrite itself, keeping the sources it is not
        # re-fetching, so truncating here would just restore the bug.
        refresh.append(("smallremote", [
            Step("smallremote_fetch", ["smallremote_fetch.py"],
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

    # ---- wttj -----------------------------------------------------------
    # The whole Welcome to the Jungle corpus, both public Algolia indices, in
    # one step.  --truncate because cc.Writer appends: without it every run
    # lays down a second complete copy of 88k rows.
    #
    # This SUPERSEDES intlmisc_fetch.do_wttj (same index, 15 of 42 fields,
    # serial, and parked in DEFAULT_OFF `intl` where it refreshed only when
    # somebody remembered `--only intl`).  That source is unregistered there.
    #
    # It does NOT supersede atstail's wttj leg, which is still in ATSTAIL_ONLY
    # below.  atstail walks tenants/wttj.csv (4,648 boards) one company at a
    # time against the same index this step reads whole, so the two overlap
    # and atstail is the slower, smaller half.  Retiring it is a separate
    # decision, because its rows are already in the shared atstail.jsonl and
    # dropping the leg strands them: there is no ATSTAIL_REFRESH here (see the
    # note on atstail-rest), so they would never be re-fetched or delisted.
    if "wttj" in fams:
        refresh.append(("wttj", [
            Step("wttj_fetch", ["wttj_fetch.py", "--truncate"],
                 tries=3, ok_rc=(0, 1),
                 hard_timeout=_ceiling({"WTTJ_BUDGET_MIN": "30"}))]))

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
    ]
    # The per-family build sheets.  Each is also appended to its refresh
    # chain (post_steps in gated()), so it runs as soon as that family's
    # fetch and row check finish -- inside the 82-124 min window where only
    # workday is still fetching -- instead of here, in the strictly serial
    # tail (~390s a run, getro alone 300s).  The tail copies skip themselves
    # when the chain already built them, and still run for --build-only,
    # --only, and any chain that aborted.  None of them reads another
    # family's output, norm_dedupe's, build_sources', or any .prev
    # (checked 2026-09-15).
    # greenhouse/ashby are driven by their own bash scripts, but
    # all_jobs_build.py reads their masters — so without these two steps
    # run_all merged whatever stale CSV happened to be on disk.
    sheets = {  # refresh chain tag -> (sheet step, needs)
        "getro": ("getro_build_sheet",
                  (p("getro_all_jobs.jsonl"), p("getro_job_boards.csv"))),
        "lever": ("lever_build_sheet", (p("lever_all_jobs.jsonl"),)),
        "greenhouse": ("greenhouse_build_sheet",
                       (p("greenhouse_all_jobs.jsonl"),)),
        "ashby": ("ashby_build_sheet", (p("ashby_all_jobs.jsonl"),)),
        "rippling": ("rippling_build_sheet", (p("rippling_all_jobs.jsonl"),)),
        "personio": ("personio_build_sheet", (p("personio_all_jobs.jsonl"),)),
        "yc": ("yc_build_sheet", (p("yc_all_jobs.jsonl"),)),
        "dover": ("dover_build_sheet", (p("dover_all_jobs.jsonl"),)),
        "notion": ("notion_build_sheet", (p("notion_all_jobs.jsonl"),)),
    }
    refreshed = {tag for tag, _ in refresh}
    post = {tag: [Step(name, [name + ".py"], needs=needs, tries=LOCAL_TRIES,
                       marks_built=True)]
            for tag, (name, needs) in sheets.items() if tag in refreshed}
    tail += [Step(name, [name + ".py"], needs=needs, tries=LOCAL_TRIES,
                  skip_if=(lambda n=name: n in _built))
             for name, needs in sheets.values()]
    tail += [
        # Before the heaviest step: if the disk is already under
        # RUN_ALL_PRUNE_BELOW_GB, drop the .prev of every family whose fetch
        # finished and passed its row check (~4.2 GB; getro alone is 3.5).
        # The 2026-09-10 and 09-14 build tails both needed this done by hand,
        # and the mid-run approval did not come.
        Step("prune_prev", func=_prune_if_low, tries=1),
        Step("bh_build_combined", ["bh_build_combined.py"],
             tries=LOCAL_TRIES,
             cleanup=(p("portals_combined_master.csv.tmp"),)),
        # Always last: it merges every master above.  The build chain runs
        # fail-soft, so a family with no JSONL yet is skipped rather than
        # taking this step down with it.
        # THE WORKBOOK IS OFF BY DEFAULT as of 2026-09-04.  all_jobs_build
        # writes All_Jobs_<date>.xlsx with Workbook(write_only=True), which
        # spills ~9.8 GB of uncompressed sheet XML into TMPDIR -- the same
        # volume the masters live on -- before zipping it down to 1.1 GB.  On
        # 2026-09-04 that took the box from 11 GiB free to 1.4 GiB, and cost
        # 41.9 min of a strictly serial build tail.
        #
        # Nothing in this repo opens the result.  load_workbook has exactly
        # two call sites: triage_export.py:380 (its own Worklist export) and
        # vet_build.py:195 (Vet_Queue_*).  Confirmed with the operator.
        #
        # ALL_JOBS_XLSX=1 brings it back for one run.  The flag it passes has
        # existed since the file was written (all_jobs_build.py:84, early
        # return at :1071 immediately after os.replace(tmp_csv, OUT_CSV)) --
        # run_all was simply the only caller never using it.
        Step("all_jobs_build",
             ["all_jobs_build.py"]
             + ([] if os.environ.get("ALL_JOBS_XLSX") == "1"
                else ["--csv-only"]),
             tries=LOCAL_TRIES,
             cleanup=(p("all_jobs_master.csv.tmp"),)),
    ]
    return discover, refresh, tail, post


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
    ap.add_argument("--rollback", action="store_true",
                    help="undo an interrupted run's rotation (restore every "
                         ".prev.jsonl it moved), then exit")
    ap.add_argument("--allow-battery", action="store_true",
                    help="start even when the Mac is not on AC power")
    ap.add_argument("--force", action="store_true",
                    help="start below the free-disk floor / roll back a run "
                         "that died in its build tail")
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
    _run["dry"], _run["resume"] = dry, args.resume
    t0 = time.time()
    say(f"run_all: families={fams or '(build only)'} discover={args.discover} "
        f"resume={args.resume} dry_run={dry}")

    # ---- guard, preflight, marker
    # Nothing is written before the guard and preflight pass: a refused run
    # must leave the previous (possibly interrupted) marker exactly as it was.
    prev = _load_state()
    if dry:
        if prev.get("phase") not in (None, "done", "rolled_back"):
            say(f"[guard] DRY: the last marker reads phase="
                f"{prev.get('phase')} ({_eff_phase(prev)}), run "
                f"{prev.get('run_id')} — a real run would check it first")
        if args.rollback:
            for live, prv, *_ in prev.get("rotated") or []:
                say(f"[rollback] DRY {os.path.basename(prv)} -> "
                    f"{os.path.basename(live)}")
            return
    else:
        prev = _guard(args)
        if args.rollback:
            _rollback(prev, args.force)
            return
        fatal = _preflight(args)
        if fatal:
            _refuse("preflight: " + "  |  ".join(fatal))
        _caffeinate()
    # Carry the interrupted run's rotated pairs into BOTH re-entries.
    # --resume continues its fetch; --build-only builds from what it left --
    # and without the pairs the partial files would get no row check and the
    # next bare run would rotate them over the good .prev (both reproduced by
    # the 2026-09-15 code review).
    carry = _past_rotation(prev) and (args.resume or args.build_only)
    # --resume keeps the interrupted run's id (ats_fetch's ATS_RUN_ID must
    # not change mid-generation); --build-only fetches nothing, so gets its own.
    _run["run_id"] = (prev.get("run_id") if carry and args.resume else None) \
        or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # The date the data was FETCHED -- what the next new-jobs diff counts
    # "posted since" from.  A resume or a build-only re-entry did not fetch it.
    if args.build_only or carry:
        try:
            _ls = json.load(open(LAST_SUCCESS_FILE, encoding="utf-8"))
        except (OSError, ValueError):
            _ls = {}
        fetch_started = (prev.get("fetch_started_utc") or prev.get("started_utc")
                         or (_ls.get("started_utc") if args.build_only else None)
                         or _now())
    else:
        fetch_started = _now()
    if not dry:
        _state.clear()
        if carry:
            # What the interrupted run rotated is still the truth about which
            # .prev files belong to this generation -- the row check and any
            # later --rollback need it.
            for k in ("rotated", "state_backups", "pruned"):
                if prev.get(k):
                    _state[k] = prev[k]
            if args.build_only and prev.get("rowcheck"):
                # Nothing is fetched, so the verdicts that run already
                # reached (and any fallback it already made) still stand.
                # Under --resume they do NOT: stateless fetchers re-fetch.
                _state["rowcheck"] = dict(prev["rowcheck"])
                _rowcheck.update(prev["rowcheck"])
        _state.update(pid=os.getpid(), run_id=_run["run_id"],
                      argv=sys.argv[1:], started_utc=_now(),
                      fetch_started_utc=fetch_started,
                      resumed_from=prev.get("run_id") if carry else None)
        _set_phase("discovery")
        # Handlers only NOW: before this point the marker on disk is the
        # PREVIOUS run's, and a Ctrl-C during the guard or preflight would
        # have overwritten it with an empty "interrupted" one.  Default
        # signal handling is correct there -- nothing has been launched.
        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)
        say(f"[marker] run {_run['run_id']}"
            + (f" ({'resuming' if args.resume else 'building after'} "
               f"{prev.get('run_id')}: {len(_state.get('rotated', []))}"
               f" rotated file(s) carried over)" if carry else "")
            + f" -> {os.path.relpath(STATE_FILE, BASE)}")
    medians = _history_medians() if not dry else {}
    watch_stop = threading.Event()
    if not dry:
        threading.Thread(target=_heartbeat, args=(watch_stop, medians),
                         daemon=True).start()
        threading.Thread(target=_disk_guard, args=(watch_stop,),
                         daemon=True).start()

    discover, refresh, tail, post = build_plan(args, fams)

    # phenom-disc is DEFERRED rather than joined at the barrier: it has been
    # the barrier's last chain in every run since 2026-09-07 (919-1,913s), yet
    # the only thing that reads what it writes is phenom_fetch.  It reads the
    # master + phenom_candidates.csv, writes tenants/phenom.csv atomically
    # (phenom_discover_boards.py:496-519), and the prelude writes
    # tenants/phenom_cx.csv, not phenom.csv -- so only the phenom refresh
    # chain waits for it, and the barrier lifts at the next-slowest chain
    # (dover-disc; 5.6-6.6 min earlier, measured on the last four runs).
    # gh-disc and dover-disc stay on the barrier: both read
    # getro_all_jobs.jsonl, which rotation moves.
    # The event is pre-set when phenom-disc is not planned: it lives under
    # `tail` while the phenom fetch lives under `portals`, so --only portals
    # --discover would otherwise wait forever.
    deferred = {"phenom-disc": threading.Event()}
    for _tag, _ev in deferred.items():
        if not any(tag == _tag for tag, _ in discover):
            _ev.set()

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
        # workday-disc parses slug_wayback_urls.txt, and ONLY a --cdx run
        # rewrites that dump.  The crawl is opt-in and off, so the file has
        # not moved since 2026-08-26 -- waiting on the slug sweep blocks 562s
        # of real workday discovery behind 3,890s of a producer that cannot
        # affect its input.  Keep the wait only when the crawl that would
        # rewrite the dump is actually going to run.
        # rippling-disc joins workday-disc on the same reasoning, measured
        # 2026-09-05: the sweep moved rippling_candidates.csv 2,703 -> 2,703
        # and rippling_probe_boards returned a byte-identical "2560 boards
        # (1887 live, 673 empty)" in 0s.  Only a real CDX crawl can change its
        # input, and the crawl is opt-in and off, so waiting on the sweep
        # blocked a 1-second probe behind 2,990s of a producer that cannot
        # affect it.  Both waits come back the moment the crawl is enabled.
        waiters = set()
        if os.environ.get("CDX_ENABLE") == "1":
            waiters |= {"workday-disc", "rippling-disc"}

        # BACKGROUND: started here, joined AFTER the build tail.
        #
        # The discovery join is a barrier -- the slowest chain gates every
        # fetcher, whether or not anything depends on it.  Measured 2026-09-05:
        # the last tenant-writing chain (phenom-disc) finished 01:31:37, but
        # the barrier did not lift until 02:04:12, because these two were still
        # running.  What that 32m35s bought, against 14 held-back families:
        #     slugs     2,990s -> +6 candidate slugs, 0 net-new live boards
        #     alt-disc  2,649s -> 0 new candidates (0 on 09-04 too)
        # Neither writes tenants/*.csv, so neither can affect the prelude's
        # merge.  Both write grow-only candidate CSVs that the refresh reads at
        # file level, so a sweep landing mid-refresh hands it the previous
        # list -- 105,058 of 105,064 rows, 99.994% -- and the remainder is
        # picked up next run because candidates never shrink.
        #
        # THE WRITE THEY NOW RACE IS ATOMIC.  slug_discover_boards.py:553 was a
        # plain open(...,"w") over personio_candidates.csv and rippling-tenant_
        # candidates.csv, which personio_fetch_jobs and rippling_tenant_fetch
        # read during the refresh; that is a truncation race and it had to be
        # fixed (tmp + os.replace) before this was safe.  alt_discover_boards
        # already wrote atomically at :622.  Do not background a third chain
        # without checking the same thing.
        #
        # THE WRITES THEY RACE EACH OTHER ON ARE NOW LOCKED (2026-09-16).
        # Atomic stopped torn reads, but slugs and alt-disc both REWRITE the
        # same candidate CSVs, and the sweep's 50-minute-old snapshot was
        # overwriting alt-disc's merges (137 bamboohr, 95 breezy, 138
        # recruitee missing).  Every candidate writer now holds
        # candidates_common.csv_lock around read-modify-replace, which is what
        # admits vendor-dir and slug-guess here: both write only through
        # locked_merge and tmp+os.replace sidecars.
        background = {"slugs", "alt-disc", "vendor-dir", "slug-guess"}
        if os.environ.get("CDX_ENABLE") == "1":
            # A real crawl DOES move rippling-disc's input, so the barrier is
            # correct again -- and its waiter sits in the foreground, so
            # backgrounding the producer would only move the block, not remove
            # it.  Keep the sweep in the foreground whenever it can crawl.
            background.discard("slugs")
        if os.environ.get("DISC_JOIN_ALL") == "1":
            background = set()  # escape hatch: restore the old barrier
            deferred_on = False
        else:
            deferred_on = True
        _bg_tags.update(background)
        if deferred_on:
            _bg_tags.update(deferred)
        # A DISCOVER STEP MUST NOT STRAND ITS PROBE.  These chains are
        # [discover, probe], and run_chain is fail-fast, so when the discover
        # step exited 1 the probe was skipped — even though it needs nothing
        # from that step beyond a candidate CSV that is already on disk, and
        # even though alt_discover_boards had just added rows to it. Measured
        # 2026-09-02 during the Wayback outage: 74 lever and 247 greenhouse
        # alt-discovered candidates went unprobed for the whole run, while
        # ashby's 240 sailed through purely because ashby_fetch_jobs lives in
        # the refresh phase instead. The wiring was the difference, not the
        # scripts.
        #
        # The discover steps no longer fail for a CDX outage (the crawl is
        # opt-in and wrapped), so this is a belt to that braces: any OTHER
        # discover failure still leaves the probe to do its job.
        soft = {"lever-disc", "gh-disc"}
        threads = []
        for tag, steps in discover:
            if tag == "slugs":
                th = threading.Thread(
                    target=run_chain, args=(tag, steps, dry, slugs_done))
            elif tag in waiters:
                def slug_waiter(tag=tag, steps=steps):
                    slugs_done.wait()
                    if _failed.get("slugs"):
                        say(f"[{tag}] slug sweep failed — running against the "
                            f"existing candidate list")
                    run_chain(tag, steps, dry)
                th = threading.Thread(target=slug_waiter)
            elif tag in deferred:
                th = threading.Thread(
                    target=run_chain,
                    args=(tag, steps, dry, deferred[tag]))
            else:
                th = threading.Thread(
                    target=run_chain,
                    args=(tag, steps, dry, None, tag in soft))
            later = tag in background or (deferred_on and tag in deferred)
            (_bg_threads if later else threads).append(th)
        for t in threads + _bg_threads:
            t.start()
        for t in threads:
            t.join()
        if _bg_threads:
            names = sorted(background | (set(deferred) if deferred_on
                                         else set()))
            say(f"[disc] {len(_bg_threads)} chain(s) still running past the "
                f"barrier ({', '.join(names)}); only their consumers wait "
                f"for them — joined after the build tail")
        # Under _failed_lock: a deferred chain can fail concurrently, and a
        # failure landing between the update and the clear would be lost.
        with _failed_lock:
            if _failed:
                say(f"discovery failures: {_failed} — continuing with the "
                    f"existing board lists")
                # Discovery is best-effort by design (Wayback CDX is
                # rate-limited and flaky), so its failures must not survive
                # into the final exit status — otherwise every --discover run
                # exits 1 even though the refresh and build phases both
                # succeeded.
                _discovery_failed.update(_failed)
                _failed.clear()
        if not deferred_on:
            for _ev in deferred.values():
                _ev.wait()

    # PHASE BOUNDARY.  The three phases are strictly serial and each has a
    # different bottleneck, so a single total is unattributable.  The
    # 2026-09-04 run reported "433.6 min" against 08-20's "169.2 min" and the
    # comparison was not even like-for-like -- 08-20 ran discover=False with a
    # 45-minute workday budget -- so nobody could tell whether a change helped
    # without hand-reconstructing timestamps out of chain_run_all.out.
    t_disc_end = time.time()

    # ---- rotation
    if fams and not args.resume:
        _set_phase("rotating")
        for fam in fams:
            rotate_family(fam, dry)
    elif args.resume:
        say("[rotate] --resume: keeping JSONLs + state files")
    _set_phase("refresh")
    growth0 = _norm_sizes()

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
    # refresh chain -> the deferred discovery chain it consumes
    consumers = {"phenom": deferred["phenom-disc"]}
    threads = []
    for tag, steps in refresh:
        ps = post.get(tag, ())
        if tag == "prelude":
            threads.append(threading.Thread(
                target=gated(tag, steps, dry, done_event=tenants_ready)))
        elif tag in tenant_waiters:
            threads.append(threading.Thread(
                target=gated(tag, steps, dry, wait_event=tenants_ready)))
        elif tag == "getro":
            threads.append(threading.Thread(
                target=gated(tag, steps, dry, done_event=getro_done,
                             post_steps=ps)))
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
            threads.append(threading.Thread(target=gated(
                tag, steps, dry, wait_event=consumers.get(tag),
                post_steps=ps)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # ---- normalize + build (sequential: these are the memory-heavy steps).
    # fail_soft: each builder is an independent local transform, so a family
    # with no JSONL yet must not stop all_jobs_build from running.
    t_refresh_end = time.time()
    # Measured BEFORE norm_dedupe, which rewrites these files in place.
    growth1 = _norm_sizes()
    if not dry:
        _rowcheck_sweep()
    _set_phase("build")
    run_chain("build", tail, dry, fail_soft=True)
    _set_phase("report")

    # ---- join the backgrounded discovery chains
    # They have had the whole refresh + build to finish (3-6h against their
    # measured ~50 min), so this is normally instant.  It must still happen:
    # the run cannot exit while they are mid-write, or the next run inherits a
    # half-written candidate CSV and a state file that never got stamped.
    if _bg_threads:
        alive = sum(1 for t in _bg_threads if t.is_alive())
        if alive:
            say(f"[disc] waiting on {alive} background discovery chain(s)")
        _t_bg = time.time()
        for t in _bg_threads:
            t.join()
        say(f"[disc] background discovery joined "
            f"({(time.time() - _t_bg) / 60:.1f} min waited)")
        # Same contract as the foreground phase: discovery is best-effort and
        # its failures must never decide the run's exit status.  BY TAG ONLY --
        # a blanket _failed.clear() here would also swallow every refresh and
        # build failure, which are exactly the ones that must still exit 1.
        with _failed_lock:
            for _tag in _bg_tags:
                if _tag in _failed:
                    _discovery_failed[_tag] = _failed.pop(_tag)

    # ---- report
    _end = time.time()
    say(f"run_all finished in {(_end - t0) / 60:.1f} min "
        f"(discovery {(t_disc_end - t0) / 60:.1f} / "
        f"refresh {(t_refresh_end - t_disc_end) / 60:.1f} / "
        f"build {(_end - t_refresh_end) / 60:.1f})")
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
        # Stamp the mtime.  Under --csv-only this branch would otherwise
        # report a workbook an EARLIER run wrote today as if it were this
        # run's output -- the same shape as the stale-master trap the chain
        # gate exists to catch.
        xmt = datetime.fromtimestamp(os.path.getmtime(xlsx), tz=timezone.utc)
        say(f"  All_Jobs_{snap}.xlsx: {os.path.getsize(xlsx) / 1e6:.1f} MB, "
            f"written {xmt.strftime('%Y-%m-%d %H:%M')}Z")
    elif os.environ.get("ALL_JOBS_XLSX") == "1":
        say(f"  All_Jobs_{snap}.xlsx: MISSING (ALL_JOBS_XLSX=1 was set)")
    else:
        say("  All_Jobs xlsx: skipped (ALL_JOBS_XLSX=1 to write it)")
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
            # Which discovery channel delivers live boards nobody else found
            # (wayback / altdisc / srdir / guess ...).  Seconds; reads only
            # candidate CSVs, board lists and the ats yield ledgers.
            say("[report] slug_channel_report — live boards per channel")
            subprocess.run([sys.executable, "slug_channel_report.py"],
                           cwd=BASE, timeout=600, check=False)
            say("[report] host_census — un-piped apply-url apexes")
            subprocess.run([sys.executable, "host_census.py", p_master,
                            "--top", "40"], cwd=BASE, timeout=1800, check=False)
            # The apex census cannot see company careers subdomains at all:
            # it collapses careers.bankofamerica.com to bankofamerica.com and
            # its SKIP_LABEL drops exactly the jobs/careers/apply/talent
            # labels.  --subdomain buckets by full host instead.
            say("[report] host_census --subdomain — company careers hosts")
            subprocess.run([sys.executable, "host_census.py", p_master,
                            "--subdomain", "--top", "40", "--min", "50"],
                           cwd=BASE, timeout=1800, check=False)
        except (OSError, subprocess.SubprocessError) as e:
            say(f"[report] host_census skipped: {e}")
    if not dry:
        try:
            _summary(t0, t_disc_end, t_refresh_end, growth0, growth1, medians)
        except Exception as e:  # noqa: BLE001 — a report must not wedge the marker
            say(f"[summary] WARN failed: {type(e).__name__}: {e}")
    watch_stop.set()
    _set_phase("done")
    if _discovery_failed:
        say(f"discovery steps that failed (non-fatal): {_discovery_failed}")
    if _failed:
        say(f"FAILED steps: {_failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
