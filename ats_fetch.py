#!/usr/bin/env python3
"""
ats_fetch.py — re-fetch the first-party ATS boards discovered in the Getro
dump (getro_ats_slugs.json) and keep only contract / temp / freelance roles,
using each ATS's structured employment-type field (far better recall than the
title-only pass the Getro records allowed).

Platforms + endpoints (all unauthenticated, verified 2026-07-19):
  ashby            GET  api.ashbyhq.com/posting-api/job-board/{slug}
  smartrecruiters  GET  api.smartrecruiters.com/v1/companies/{slug}/postings
  recruitee        GET  {slug}.recruitee.com/api/offers/
  breezy           GET  {slug}.breezy.hr/json
  bamboohr         GET  {slug}.bamboohr.com/careers/list

Added 2026-07-28, verified live the same day.  slug_discover_boards.py already
crawled these three apexes (jazzhr 632 CDX pages, workable 244+547,
teamtailor 190) and wrote their slugs to wayback_ats_slugs.json, but nothing
consumed them — 63% of an 8-hour sweep produced candidate lists no fetcher
read.  Corpus census: workable 482 boards, jazzhr 433, teamtailor 112.
  workable         GET  apply.workable.com/api/v1/widget/accounts/{slug}
                          ?details=true
                        One request per board, and the richest payload of the
                        three: employment_type, telecommuting, city/state/
                        country, published_on, application_url.
  jazzhr           GET  {slug}.applytojob.com/apply/            (HTML)
                        No JSON anywhere on the board: /apply/jobs.json 404s
                        and /apply/rss is HTTP 410.  TWO server-rendered
                        listing shapes, not one — the "one hosted product, one
                        markup" assumption written here originally is what let
                        96.8% of this family go out with no location for months.
                        The legacy resumator-* table (_JZ_ROW_RX) survives on
                        ~60 boards; the rest serve a list-group-item card
                        (_JZ_CARD_RX).  Both give title + department +
                        location per row, on the listing page, free.  employmentType and
                        datePosted live only in the per-job JobPosting
                        ld+json, so details are fetched per job, capped at
                        JZ_DETAIL_CAP and skippable with JZ_NO_DETAIL=1.
  teamtailor       GET  {slug}.teamtailor.com/jobs.rss
                        NOT the HTML board and NOT jobs.json: Teamtailor is a
                        heavily themed careers-site product, so list-page CSS
                        classes vary per customer, and jobs.json (a JSON Feed)
                        carries no location at all.  The RSS extends the feed
                        with tt:locations, tt:department and remoteStatus, in
                        one theme-independent request.  jobs.json is the
                        fallback when the RSS yields nothing.

Threaded (WORKERS), resumable via ats_fetch_state.json (per platform+slug),
one normalized JSONL per platform in contract_norm/.
"""
import concurrent.futures as cf
import csv
import datetime
import html
import json
import glob
import os
import random
import re
import threading
import time
from email.utils import parsedate_to_datetime
from xml.etree.ElementTree import ParseError

import requests
# The teamtailor RSS is third-party XML from ~112 tenant hosts, so it is parsed
# with the hardened frontend: stdlib ElementTree is documented as vulnerable to
# billion-laughs and quadratic-blowup entity expansion.  defusedxml.fromstring
# refuses DTDs and entity declarations outright and is otherwise API-identical.
from defusedxml.ElementTree import fromstring as xml_fromstring
from defusedxml.common import DefusedXmlException

import contract_common as cc
import ats_ledger as al


class Gone(dict):
    """A board the upstream ASSERTS does not exist: HTTP 404 or 410.

    Empty and falsy, so every existing consumer — `d.get("jobs") or []`,
    `d.get("content") or []`, `d if isinstance(d, list) else ...` — keeps
    working with no change to any f_* fetcher.

    It exists because gj() used to return a bare {} for four different
    situations that mean different things: the upstream saying "no such board",
    a 403 that is parked-or-Cloudflare depending on the platform, a 200 whose
    body will not parse, and a genuinely empty live board.  Only the first is
    the upstream speaking, and only the first may be allowed to delete rows we
    already have.  Everything else is our inference, and an inference does not
    get to destroy data.
    """


class GoneText(str):
    """gt()'s equivalent.  GoneText("") == "" and is falsy, so `if not body:`
    and `if body:` behave exactly as before."""


BASE = os.path.dirname(os.path.abspath(__file__))
SLUGS = os.path.join(BASE, "getro_ats_slugs.json")
# ATS_KEEP_ALL=1 -> keep full-time roles too (fresh state + ats_all_* output)
KEEP_ALL = os.environ.get("ATS_KEEP_ALL") == "1"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
PREFIX = "ats_all_" if KEEP_ALL else "ats_"
# ATS_STATE overrides the state path.  It exists because the state path was
# derived from KEEP_ALL ALONE, not from ATS_PLATFORMS, while run_all runs the
# 7-platform chain and the workable chain IN PARALLEL.  Both load the whole
# dict at import and save the whole dict at exit, so the later finisher erased
# the earlier one's completion record: on 2026-08-10 the main chain finished
# 24,836 boards at 05:48 and workable's save at 07:15 left a state file holding
# 1,194 keys, every one of them "workable:".  Give each chain its own file.
STATE = os.path.join(BASE, os.environ.get("ATS_STATE") or
                     ("ats_all_fetch_state.json" if KEEP_ALL
                      else "ats_fetch_state.json"))
# 14 was sized for the ~4k boards the getro seed yields.  The 2026-07-28 slug
# sweep raised that to ~91k, so the pool is widened to match; the per-platform
# token buckets below are what actually protect each host.
#
# 20 -> 64 after measuring the post-sweep run: 2.5 boards/s, ~8s of wall time
# per board, with the aggregate bucket capacity (55 rps) barely touched.  A
# worker is held for a WHOLE board, and boards are not uniform — one live
# jazzhr board is 1 + up to 80 requests, one big smartrecruiters board pages
# 100 postings at a time — so the pool runs out of SLOTS long before it runs
# out of rate.  Raising thread count cannot hurt the hosts: throttle() spaces
# every request per platform no matter how many workers queue behind it.
# 64 -> 320 after measuring the 2026-07-28 full-backlog drain: 750 boards in
# 11 min = 1.14 boards/s, i.e. ~56s of wall time per board.  A worker is held
# for a WHOLE board including its throttle wait, so with 85k boards queued the
# pool itself — not the token buckets — was the binding constraint: 64 workers
# / 56s = 1.14/s exactly.  The buckets still cap every host (workable stays at
# 2 rps and so still needs ~2.8h for its 20k boards), so more workers only lets
# the FAST platforms drain concurrently instead of queueing behind the slow
# ones.  Override with ATS_WORKERS.
WORKERS = int(os.environ.get("ATS_WORKERS") or 320)
SR_MAX_PAGES = 200      # cap SmartRecruiters pagination (100/page).  Was 30,
                        # which silently truncated any company past 3,000
                        # postings — and SmartRecruiters customers are large.
PLATFORMS = ["ashby", "smartrecruiters", "recruitee", "breezy", "bamboohr",
             "workable", "jazzhr", "teamtailor"]
# ATS_PLATFORMS=a,b limits a run to those platforms.  The state file is keyed
# "platform:slug", so a filtered run and a later run over the rest compose
# cleanly — no board is lost or double-counted.
#
# Why this exists: measured per-board wall time on a random sample of PENDING
# boards, 2026-07-29, with throttle() disabled so it measures the HOST:
#     bamboohr 0.5s · recruitee 0.6s · smartrecruiters 0.6s · breezy 0.9s
#     teamtailor 1.1s · jazzhr 2.5s · workable 43.4s
# 43.4s is precisely TRIES exhaustion (2+4+6+8+10+12 = 42s of backoff sleeps),
# i.e. EVERY workable board was failing all six attempts and then raising.
# apply.workable.com is one shared Cloudflare-fronted host, not per-tenant
# subdomains, so a sustained sweep 429s the whole platform and never recovers
# inside a run.  Workable is 26% of an 80k queue, so it held its concurrency
# permits for 43s a board while contributing errors instead of rows, and
# dragged the aggregate to ~1 board/s.  Run the other seven without it and they
# drain at their own speed; give workable its own slow pass afterwards.
_only = [x for x in os.environ.get("ATS_PLATFORMS", "").split(",") if x]
if _only:
    unknown = [x for x in _only if x not in PLATFORMS]
    if unknown:
        raise SystemExit(f"unknown platform(s) {unknown}; choose from "
                         f"{', '.join(PLATFORMS)}")
    PLATFORMS = _only

# Per-platform request budget.  One flat pool of WORKERS threads is shared
# across all eight, so without this the fan-out lands on whichever host is
# slowest.  api.smartrecruiters.com is the one that 429s under 14 workers,
# hence the tightest budget.  jazzhr is the next tightest because it is the
# only platform here that spends more than one request per board.
# Raised 2026-07-28 for the post-sweep board counts (bamboohr 22.8k, workable
# 21.2k, jazzhr 14.2k): at the old caps those three alone were 2.5h of pure
# throttle.  smartrecruiters deliberately STAYS at 3.0 — it is the one host
# measured to 429 under load, and slow_down() halving its rate mid-run costs
# far more than the cap saves.
#
# workable REVERTED 8.0 -> 2.0 the same day: raising it was a mistake.
# apply.workable.com rate-limits the widget API hard per IP — a 25-board
# sample at 8 rps came back 25/25 HTTP 429, and every one of those tripped
# slow_down(), so the platform sat in a 20s pause with a degraded interval
# more or less permanently.  It is a single shared host, not per-tenant
# subdomains, so it gets one global budget and 2 rps is what it tolerates.
#
# 2026-07-29, the part that note was missing: the penalty is PER IP AND
# DURABLE, not a per-request throttle.  Sequence measured on one machine:
#   * 60 concurrent requests to known-LIVE tenants  -> 60/60 HTTP 200
#   * a few hours of sweeping                        -> everything 429s
#   * 4 min cooldown, then 1 request every 2 seconds to those SAME known-live
#     tenants                                        -> 4/4 HTTP 429,
#     each a 377,552-byte Cloudflare HTML page
# So once the edge decides against you, backing off inside the run does not
# clear it, and every board — live or dead — then looks like a 43s TRIES
# exhaustion (2+4+6+8+10+12s of sleeps) before raising.  That single platform
# dragged an 80k-board run from ~15 boards/s to ~1.
# TREAT WORKABLE AS A SEPARATE, LATER, LOW-RATE RUN FROM A RESTED IP:
#   ATS_PLATFORMS=workable ATS_RPS=workable:2 ATS_CONC=workable:4
# and check it returns 200 for a KNOWN-LIVE slug before committing to a sweep.
RPS = {"ashby": 8.0, "smartrecruiters": 3.0, "recruitee": 8.0,
       "breezy": 8.0, "bamboohr": 8.0,
       "workable": 6.0, "jazzhr": 6.0, "teamtailor": 6.0}

# Max threads allowed inside one platform at a time — see the long note in
# work().  These bound BURST; RPS above bounds average rate.  A platform can
# never hold more than its share of the pool, so one slow host can no longer
# starve the other seven.  workable's 8 is deliberately conservative: it is the
# one host measured to answer 429 under a burst.
CONCURRENCY = {"ashby": 24, "smartrecruiters": 12, "recruitee": 24,
               "breezy": 24, "bamboohr": 24,
               "workable": 8, "jazzhr": 24, "teamtailor": 24}
# ATS_RPS / ATS_CONC = "plat:val,plat:val" overrides, so a platform that needs
# a gentler pass can get one without editing the defaults.  Used for the
# workable-only run: apply.workable.com is a single shared host and answers
# clean 200s at moderate concurrency, but once a sweep pushes it into 429 it
# never recovers inside a run — so it gets its own low-and-slow pass.
for _kv in os.environ.get("ATS_RPS", "").split(","):
    if ":" in _kv:
        _p, _v = _kv.split(":", 1)
        if _p in RPS:
            RPS[_p] = float(_v)
for _kv in os.environ.get("ATS_CONC", "").split(","):
    if ":" in _kv:
        _p, _v = _kv.split(":", 1)
        if _p in CONCURRENCY:
            CONCURRENCY[_p] = int(_v)
_gate = {p: threading.Semaphore(CONCURRENCY.get(p, 12)) for p in PLATFORMS}

# jazzhr only: a board with a pathological posting count must not spend the
# whole run's budget on one company.  Boards are SMB-sized (median well under
# 20 postings), so this bounds the tail rather than truncating normal boards.
JZ_DETAIL_CAP = int(os.environ.get("JZ_DETAIL_CAP") or 80)
# bamboohr's list endpoint publishes no date; its per-job detail endpoint does.
# Off by default — 80,751 jobs at 1 request each is ~2.8h at 8 rps.  BH_DETAIL=1
# turns it on, capped per board like jazzhr's.
BH_DETAIL = os.environ.get("BH_DETAIL") == "1"
BH_DETAIL_CAP = int(os.environ.get("BH_DETAIL_CAP") or 80)
JZ_NO_DETAIL = os.environ.get("JZ_NO_DETAIL") == "1"
# Detail fetching is affordable on a seed-sized run (491 boards) and ruinous on
# a sweep-sized one (14,182): it turns each live board from 1 request into up
# to 81, at 6 rps, while holding a worker the entire time.  So it is switched
# on board COUNT rather than left to a human to remember — small run keeps the
# posted dates, big run buys breadth instead.  main() prints which way it went;
# force either with JZ_NO_DETAIL=1 or JZ_DETAIL=1.
JZ_DETAIL_MAX_BOARDS = int(os.environ.get("JZ_DETAIL_MAX_BOARDS") or 3000)
JZ_FORCE_DETAIL = os.environ.get("JZ_DETAIL") == "1"

# Wall-clock budget.  The 2026-07-28 sweep handed this script 88k boards, the
# overwhelming majority of them historical slugs that are long dead, and the
# measured drain rate makes that a ~10h step sitting directly in front of the
# build tail.  The value is front-loaded (live boards are found throughout a
# SHUFFLED queue, so any prefix is a fair sample), and unreached boards stay
# pending rather than being marked done, so successive runs keep chipping away
# at the tail.  0 disables the limit.
BUDGET_MIN = int(os.environ.get("ATS_BUDGET_MIN") or 25)
TIMEOUT = 30
TRIES = 6

# Consecutive 404/410 sightings before a board's rows may be delisted.  One is
# a DNS blip; two on consecutive runs is the upstream meaning it.  Hot boards
# are fetched every run, so this is a next-day confirmation.
GONE_STRIKES = int(os.environ.get("ATS_GONE_STRIKES") or 2)

STAGE_DIR = os.path.join(cc.NORM_DIR, ".ats_staging")
# Which platforms may DELIST — drop the old rows of a board they authoritatively
# re-fetched.  Defaults to NONE.  "1"/"all" enables every platform; a
# comma-separated list enables exactly those.  Default-off because this is the
# one setting that can delete rows, and the rollout enables it one platform at a
# time (ashby first: it is also covered by the native ashby pipe, so even a
# total loss there is recoverable from another source).
# What delisting DOES when it fires.  "drop" removes the stale rows (the
# original behaviour).  "flag" keeps them and stamps delisted_on=<run date>,
# so the sheet keeps its row count and the lanes can sort or filter on truth
# instead of silently carrying dead postings forever.  Append-only merging was
# measured at +78-81% per platform per run with `dropped 0`, and `retired: 0`
# across all 71,659 ledger boards -- nothing has ever left this corpus.
DELIST_MODE = (os.environ.get("ATS_DELIST_MODE") or "drop").strip().lower()
_dl = (os.environ.get("ATS_DELIST") or "0").strip()
DELIST = (set(PLATFORMS) if _dl in ("1", "all")
          else set() if _dl in ("0", "") else
          {x for x in _dl.split(",") if x})
# Compute and print the merge, install nothing.  The pre-flight for a platform
# about to be switched on.
STAGE_ONLY = os.environ.get("ATS_STAGE_ONLY") == "1"
# Refuse to install a merge that lost more than this fraction of the rows it
# started with.  A hard refusal that falls back to a plain append.
MAX_SHRINK = float(os.environ.get("ATS_MAX_SHRINK") or 0.25)
# Per-board collapse detector.  A board that held >= COLLAPSE_MIN rows and now
# returns <= COLLAPSE_FRAC of that must repeat the result COLLAPSE_STRIKES times
# before its old rows are dropped.
COLLAPSE_MIN = int(os.environ.get("ATS_COLLAPSE_MIN") or 10)
COLLAPSE_FRAC = float(os.environ.get("ATS_COLLAPSE_FRAC") or 0.10)
COLLAPSE_STRIKES = int(os.environ.get("ATS_COLLAPSE_STRIKES") or 2)
# Identifies the RUN, not the attempt.  run_all passes one value across all of a
# step's supervised retries, so .prev keeps the generation from before the run
# instead of being overwritten by attempt 2 with attempt 1's output.
RUN_ID = os.environ.get("ATS_RUN_ID") or ""
# The date stamped into delisted_on.  Derived from RUN_ID when run_all supplies
# one (so every supervised retry of a step agrees), else today.
RUN_DATE = ((RUN_ID[:8] and "%s-%s-%s" % (RUN_ID[:4], RUN_ID[4:6], RUN_ID[6:8]))
            if RUN_ID[:8].isdigit()
            else datetime.datetime.now(
                 datetime.timezone.utc).strftime("%Y-%m-%d"))

_lock = threading.Lock()
_writers = {}
_state = cc.load_state(STATE)
_ledger = al.load()
_counts = {}
_failed_now = set()     # boards that failed in THIS run (drives the exit code)
_outcomes = {}          # board key -> ok | gone | empty | err  (this run)
_authoritative = {}     # platform -> {slug} whose rows this run may replace

# ---- delta-only detail fetching (jazzhr, bamboohr) -------------------------
# datePosted lives ONLY in the per-job payload for both platforms, so a date
# costs one extra request per posting.  Both all-or-nothing settings are wrong:
# fetching every one is ruinous (138,829 jazzhr rows is ~6.4h at 6 rps, which is
# why run_all pinned JZ_NO_DETAIL=1), and fetching none is what that produced --
# 1.8% of jazzhr rows and 0.7% of bamboohr's carry a date.  Those are the two
# LARGEST board inventories in the pipeline (bamboohr 24,254 boards, jazzhr
# 14,830) and triage/lanes.py:441 sorts undated rows last in every lane, so
# ~229k rows exist and are unreachable in practice.
#
# Delta-only makes the steady state cheap: a posting already dated in a previous
# generation is never re-fetched, so only genuinely new postings cost a request
# (~5k/run, ~14 min).  ATS_DETAIL_BUDGET paces the one-time backfill across runs
# rather than spending six hours in one step -- at the default that is ~17 runs
# to date the existing corpus, and the budget is a hard ceiling per RUN, not per
# board, so it cannot be blown by one enormous employer.
DETAIL_BUDGET = int(os.environ.get("ATS_DETAIL_BUDGET") or 8000)
_detail_spent = {"n": 0, "skipped_known": 0, "over_budget": 0}
_dated = set()          # (platform, board, job_id) that already carry a date


def _load_dated(platforms):
    """Index every (platform, board, job_id) that already carries a date.

    Read from the LIVE jsonl, which at this point is the previous generation --
    merge_generation has not run yet.  A row with a blank posted is deliberately
    NOT indexed, so it stays eligible and the backfill converges.
    """
    for plat in platforms:
        path = _live(plat)
        if not os.path.exists(path):
            continue
        n = 0
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if not (d.get("posted") or "").strip():
                    continue
                jid = d.get("job_id") or ""
                if jid:
                    _dated.add((plat, d.get("board") or "", str(jid)))
                    n += 1
        print(f"  {plat}: {n:,} posting(s) already dated — delta-only detail",
              flush=True)


def _need_detail(platform, board, job_id):
    """True when this posting has no date yet AND the run can still afford one.

    Counts the budget at the point of DECISION, under the shared lock, because
    the fetchers are threaded (WORKERS defaults to 320) and an unsynchronised
    counter would overshoot by roughly the worker count.
    """
    if not job_id:
        return False
    if (platform, board, str(job_id)) in _dated:
        _detail_spent["skipped_known"] += 1
        return False
    with _lock:
        if _detail_spent["n"] >= DETAIL_BUDGET:
            _detail_spent["over_budget"] += 1
            return False
        _detail_spent["n"] += 1
    return True


_collapsed = []         # (key, rows_before, rows_now) held back for a re-check
_held = {}              # platform -> {slug} whose rows THIS RUN are distrusted
_visited = set()        # in-process re-entry guard (see work())

# A board that has never produced a row does not deserve six attempts.  TRIES=6
# spends 2+4+6+8+10+12 = 42s of pure backoff sleep before giving up, and the
# ~40k zero-yield boards are precisely the population that exhausts it — that
# 42s is the whole reason a "1,000 boards/min" queue of mostly-dead slugs
# actually measured 1.14 boards/s.  Cutting cold boards to 2 attempts is a 7x
# saving on exactly the boards that waste the budget.
TRIES_COLD = int(os.environ.get("ATS_TRIES_COLD") or 2)
# Consecutive failed runs before a board is retired.  The 32 junk wayback slugs
# that cost 2h16m on 2026-08-16 (bamboohr:hr, breezy:sys, teamtailor:email)
# would have retired themselves on run 4.
MAX_FAILS = int(os.environ.get("ATS_MAX_FAILS") or 3)
# ...but only if the platform was mostly healthy this run.  A host-wide outage
# must not retire thousands of good boards in one go.
FAIL_RATE_MAX = float(os.environ.get("ATS_FAIL_RATE_MAX") or 0.5)

# Per-board first-call outcome, recorded by gj()/gt().
#
# Only the FIRST request of a board counts.  jazzhr spends 1 listing request +
# up to 80 per-job detail requests, and teamtailor tries RSS and then falls back
# to jobs.json — a 404 on jazzhr's 37th job detail, or on a teamtailor RSS whose
# jobs.json fallback then returns work, must not be read as "this board is
# gone".  The first request is the board's own front door on every platform.
_seen = threading.local()


def _saw(kind):
    """Record an outcome for the board currently being fetched on this thread.
    First writer wins; later calls within the same board are ignored."""
    if getattr(_seen, "kind", None) is None:
        _seen.kind = kind


def _tries():
    """Retry budget for the board on this thread — cold boards get fewer."""
    return TRIES_COLD if getattr(_seen, "tier", None) == "cold" else TRIES

# ---- per-platform token bucket (the greenhouse_fetch_jobs.py pattern, keyed
# by platform because one pool serves five hosts) ---------------------------
_rate_lock = threading.Lock()
_next_t = {k: 0.0 for k in RPS}
_interval = {k: 1.0 / v for k, v in RPS.items()}
_pause_until = {k: 0.0 for k in RPS}
_n429 = {}
_ok_streak = {}   # consecutive clean responses per platform; see speed_up()


class Transient(Exception):
    """A retryable failure.  Raised so work() records err: instead of done —
    the whole point being that a rate-limited board must never be recorded as
    a completed empty board."""


def throttle(platform):
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[platform]:
                wait = _pause_until[platform] - now
            else:
                wait = _next_t[platform] - now
                if wait <= 0:
                    _next_t[platform] = max(
                        _next_t[platform] + _interval[platform], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


# The 429 response, rebuilt 2026-09-16 after measuring what the old one cost.
#
# It used to be a 20-second PLATFORM-WIDE pause plus a ratchet capped at 0.5s.
# Both halves were wrong, and together they are where workable's throughput
# went -- not to the host.
#
#   * throttle() blocks EVERY worker on a platform while _pause_until is in the
#     future, so ONE 429 on ONE board stopped all four workers for 20 seconds.
#   * the ratchet was INERT at the configured rate.  run_all passes
#     ATS_RPS=workable:2, so _interval starts at 1/2 = 0.5s, which IS the old
#     cap -- min(0.5 * 1.5, 0.5) == 0.5.  speed_up's floor is 1.0/RPS = 0.5
#     too.  The interval could never move in either direction.
#
# So a 429 bought 20 seconds of global silence and changed the sustained rate
# by exactly nothing.  Reconstructed from logs/run_all_summary.json and the
# per-run logs: 715 boards x 0.5s = 358s of bucket-active time inside an
# 1,820s run, i.e. 80% of the run was self-imposed blackout, and 75-85% across
# all 20 logged runs.  Measured against a host that answers in 59ms at p50
# (30 serial requests, max 177ms).  That is also why raising the budget 30 ->
# 90 min made throughput FALL from ~26 to 7.9 boards/min: more time meant more
# 429s meant more blackout, while the rate that provoked them never changed.
#
# The replacement is an ordinary control loop: a SHORT pause to let a burst
# clear, and a ratchet that can actually reduce the sustained rate, with
# speed_up walking it back after 40 clean responses.  Recovery stays slower
# than backoff (0.9x per 40 clean vs 1.5x per 429), so a host that is genuinely
# refusing still wins the argument.
PAUSE_S = float(os.environ.get("ATS_429_PAUSE") or 2.0)
# Must be comfortably ABOVE every platform's configured interval or the ratchet
# is inert again -- that is the bug this replaces, and it is easy to reintroduce
# by lowering this to something that looks tidy.
INTERVAL_CAP = float(os.environ.get("ATS_INTERVAL_CAP") or 4.0)


def slow_down(platform):
    with _rate_lock:
        _pause_until[platform] = time.time() + PAUSE_S
        _interval[platform] = min(_interval[platform] * 1.5, INTERVAL_CAP)
        _n429[platform] = _n429.get(platform, 0) + 1


def speed_up(platform):
    """Walk a platform's interval back toward its configured rate after a run
    of clean responses.

    slow_down() is one-way: each 429 multiplies the interval by 1.5 up to the
    0.5s floor, and nothing ever lowers it again.  On an 80k-board queue that
    is effectively permanent — a handful of 429s in the first minutes pin every
    platform at 2 rps for the remaining hours, whatever RPS says.  Measured
    2026-07-28 mid-run: 195 threads, 140 concurrency permits, but only 12
    established connections and 1.6 boards/s against ~45 rps of configured
    capacity.  Recovery has to be slower than the backoff (0.9x per 40 clean
    responses vs 1.5x per 429) so a host that is genuinely refusing still wins
    the argument.
    """
    with _rate_lock:
        n = _ok_streak.get(platform, 0) + 1
        if n < 40:
            _ok_streak[platform] = n
            return
        _ok_streak[platform] = 0
        floor = 1.0 / RPS[platform]
        if _interval[platform] > floor:
            _interval[platform] = max(_interval[platform] * 0.9, floor)


def gj(s, platform, url):
    """Throttled GET -> parsed JSON.

    Unlike cc.get_json, which hands back a {"__error__": ...} sentinel that
    every caller then treats as an empty board, this RAISES on a transient
    failure.  A 429 used to mean 0 rows emitted and the board marked "done"
    forever; now it means the board is retried.
    """
    last = None
    tries = _tries()
    for attempt in range(tries):
        throttle(platform)
        try:
            r = s.get(url, timeout=TIMEOUT)
            if r.status_code == 200:
                speed_up(platform)
                try:
                    return r.json()
                except ValueError:
                    # 200 with a body that will not parse is a PARKED host, not
                    # a transient failure — 14 of 25 sampled bamboohr slugs
                    # answer exactly this (wildcard DNS, tenant exists, no
                    # careers page) and no number of retries turns HTML into
                    # JSON.  Six retries with escalating sleeps cost ~33s per
                    # board across 26% of an 88k-board run.  One retry is kept
                    # so a genuine 200-wrapped error page still gets a second
                    # chance (the failure mode slug_common.validate exists for);
                    # after that the board is recorded dead.
                    if attempt >= 1:
                        _saw("empty")
                        return {}
                    last = "bad-json"
            elif r.status_code in (404, 410):
                # The upstream is ASSERTING non-existence.  This is the only
                # signal that earns the right to delist a board's existing
                # rows, and even then only after ATS_GONE_STRIKES consecutive
                # sightings — one 404 is indistinguishable from a DNS blip or
                # an edge misroute, and the downside of believing it is
                # deleting real postings.
                _saw("gone")
                return Gone()
            elif r.status_code in (400, 401, 403):
                # 403 moved here from the rate-limit branch 2026-07-28.  On a
                # per-tenant subdomain it means the board is PARKED, not that
                # we are going too fast: 9 of 12 sampled dead breezy slugs
                # answer 403.  Treating it as a 429 made every dead board call
                # slow_down() — 20s platform-wide pause plus a permanent
                # interval increase — six times over, ~160s per board, and it
                # still ended as an error rather than done.  That one
                # misclassification is what took the 88k-board run from
                # ~55 boards/s of bucket capacity down to a measured 1.04.
                #
                # It stays NON-authoritative on purpose.  "Parked" is our
                # reading of a 403; the same status is what a live
                # Cloudflare-fronted host returns under a per-IP penalty, which
                # is precisely what apply.workable.com does (see the note at the
                # RPS table).  Believing it would delete a live board's rows.
                _saw("empty")
                return {}          # empty for this run — NOT proof of deletion
            elif r.status_code in (429, 418):
                slow_down(platform)
                last = f"HTTP {r.status_code}"
            else:
                last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient(f"{url} failed after {tries} tries: {last}")


def gt(s, platform, url, accept=None):
    """Throttled GET -> response text.  The gj() contract exactly, for the two
    platforms that serve HTML/RSS rather than JSON: "" means a genuinely empty
    or retired board, a transient failure RAISES so the board is retried
    instead of being recorded as done-and-empty."""
    last = None
    tries = _tries()
    for attempt in range(tries):
        throttle(platform)
        try:
            r = s.get(url, timeout=TIMEOUT,
                      headers={"Accept": accept} if accept else None)
            if r.status_code == 200:
                speed_up(platform)
                return r.text or ""
            elif r.status_code in (404, 410):
                _saw("gone")
                return GoneText("")     # upstream asserts non-existence
            elif r.status_code in (400, 401, 403):
                _saw("empty")
                return ""          # parked board — see the note in gj()
            elif r.status_code in (429, 418):
                slow_down(platform)
                last = f"HTTP {r.status_code}"
            else:
                last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient(f"{url} failed after {tries} tries: {last}")


def _live(platform):
    return os.path.join(cc.NORM_DIR, f"{PREFIX}{platform}.jsonl")


def _prev(platform):
    return os.path.join(cc.NORM_DIR, f"{PREFIX}{platform}.prev.jsonl")


def _stage(platform):
    return os.path.join(STAGE_DIR, f"{PREFIX}{platform}.jsonl")


def w(platform):
    """This run's rows go to a STAGING file, not straight to the live one.

    The staging area is a SUBDIRECTORY (contract_norm/.ats_staging/), never a
    `<name>.new.jsonl` sibling.  cc.norm_files() globs contract_norm/*.jsonl and
    skips only `.prev`, so a sibling would be picked up as a whole new source
    stem and every row in it counted twice by contract_build/contract_pool.  A
    non-recursive glob cannot see into a subdirectory.
    """
    if platform not in _writers:
        os.makedirs(STAGE_DIR, exist_ok=True)
        _writers[platform] = cc.Writer(_stage(platform))
    return _writers[platform]


def emit(platform, d):
    with _lock:
        w(platform).write(d)
        _counts[platform] = _counts.get(platform, 0) + 1


# ---- per-platform fetchers: return list[normalized row] --------------------
def f_ashby(s, slug):
    d = gj(s, "ashby",
           f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    out = []
    for j in (d.get("jobs") or []):
        et = CLASSIFY(j.get("employmentType"), j.get("title"))
        if not et:
            continue
        loc = j.get("location") or ""
        out.append(cc.row(
            source="ashby", source_type="ats", company=slug,
            title=j.get("title"), employment_type=et,
            work_mode=cc.work_mode(j.get("isRemote"),
                                   text=j.get("workplaceType") or loc),
            location=loc, remote=bool(j.get("isRemote")),
            posted=cc.ymd(iso=j.get("publishedAt")),
            apply_url=j.get("applyUrl") or j.get("jobUrl") or "",
            job_id=j.get("id"), board=slug,
            board_url=f"https://jobs.ashbyhq.com/{slug}"))
    return out


def f_smartrecruiters(s, slug):
    out = []
    offset = 0
    for _ in range(SR_MAX_PAGES):
        d = gj(
            s, "smartrecruiters",
            f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
            f"?limit=100&offset={offset}")
        content = d.get("content") or []
        if not content:
            break
        for p in content:
            et = CLASSIFY((p.get("typeOfEmployment") or {}).get("label"),
                             p.get("name"))
            if not et:
                continue
            loc = p.get("location") or {}
            city = loc.get("city") or ""
            region = loc.get("region") or ""
            country = loc.get("country") or ""
            locstr = loc.get("fullLocation") or ", ".join(
                x for x in (city, region, country) if x)
            out.append(cc.row(
                source="smartrecruiters", source_type="ats",
                company=(p.get("company") or {}).get("name") or slug,
                title=p.get("name"), employment_type=et,
                work_mode=cc.work_mode(loc.get("remote"), loc.get("hybrid")),
                location=locstr, city=city, state=region,
                country=country, remote=bool(loc.get("remote")),
                posted=cc.ymd(iso=p.get("releasedDate")),
                apply_url=f"https://jobs.smartrecruiters.com/{slug}/"
                          f"{p.get('id')}",
                job_id=p.get("id"), board=slug,
                board_url=f"https://jobs.smartrecruiters.com/{slug}"))
        total = d.get("totalFound")
        offset += 100
        # `or 0` here used to make a missing totalFound mean "stop after one
        # page" (100 >= 0), silently truncating the board.  When the field is
        # absent, page on and let the empty-content check above terminate.
        if total is not None and offset >= total:
            break
    return out


def f_recruitee(s, slug):
    d = gj(s, "recruitee", f"https://{slug}.recruitee.com/api/offers/")
    out = []
    for o in (d.get("offers") or []):
        et = CLASSIFY(o.get("employment_type_code"),
                         o.get("title") or o.get("position"))
        if not et:
            continue
        out.append(cc.row(
            source="recruitee", source_type="ats",
            company=o.get("company_name") or slug,
            title=o.get("title") or o.get("position"), employment_type=et,
            work_mode=cc.work_mode(hybrid=o.get("hybrid"),
                                   text=o.get("location") or ""),
            location=o.get("location") or ", ".join(
                x for x in (o.get("city"), o.get("country")) if x),
            city=o.get("city") or "", country=o.get("country") or "",
            # published_at first: created_at is when the draft was made, and a
            # req can sit unpublished for weeks.  Both arrive in Recruitee's
            # "2026-07-23 19:13:42 UTC" shape, which Python 3.9's strict
            # fromisoformat refused until cc.ymd learned to drop the trailing
            # timezone name — every one of these 64,906 rows was undated.
            posted=cc.ymd(iso=o.get("published_at")
                          or o.get("created_at")),
            apply_url=o.get("careers_apply_url") or o.get("careers_url") or "",
            # careers_apply_url is the tenant's CUSTOM domain and is the only
            # URL the API offers -- but a tenant can retire that domain's
            # careers section while leaving the board published, which is a
            # 404 on the one link the row carries.  The Recruitee-hosted twin
            # exists for as long as status == "published" and serves the same
            # form, so record it as the fallback rather than trusting a host
            # we do not control.  Built from o["slug"], not parsed back out of
            # careers_apply_url: the custom domain is free to use any path.
            job_url=(f"https://{slug}.recruitee.com/o/{o['slug']}/c/new"
                     if o.get("slug") else ""),
            job_id=o.get("id"), board=slug,
            board_url=f"https://{slug}.recruitee.com"))
    return out


def f_breezy(s, slug):
    d = gj(s, "breezy", f"https://{slug}.breezy.hr/json")
    positions = d if isinstance(d, list) else (d.get("positions") or [])
    out = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        et = CLASSIFY((p.get("type") or {}).get("name"), p.get("name"))
        if not et:
            continue
        loc = p.get("location") or {}
        locstr = loc.get("name") or ", ".join(
            x for x in (loc.get("city"), loc.get("state"),
                        loc.get("country")) if x) if isinstance(
            loc, dict) else str(loc)
        out.append(cc.row(
            source="breezy", source_type="ats",
            company=(p.get("company") or {}).get("name")
            if isinstance(p.get("company"), dict) else slug,
            title=p.get("name"), employment_type=et,
            work_mode=cc.work_mode(text=locstr
                                   + " " + str((loc or {}).get("is_remote"))),
            location=locstr, remote=bool((loc or {}).get("is_remote")),
            posted=cc.ymd(iso=p.get("published_date")),
            apply_url=p.get("url") or "", job_id=p.get("id"), board=slug,
            board_url=f"https://{slug}.breezy.hr"))
    return out


def _bh_detail(s, slug, jid):
    """datePosted for one bamboohr job, or "".

    /careers/list carries no date field at all — verified live 2026-08-16, its
    ten keys are all identity/location/employment.  /careers/<id>/detail does,
    under result.jobOpening.datePosted.  That is one request PER JOB: 80,751
    jobs at 8 rps is ~2.8h, so it is off by default, exactly like jazzhr's.

    It is worth having available because the dates are the point: the first job
    sampled came back datePosted 2022-08-25.  bamboohr rows are undated, and
    all_jobs_build keeps undated rows unconditionally, so a 2022 posting sits in
    the corpus forever.  With a date the existing cutoff removes it for free.
    """
    try:
        d = gj(s, "bamboohr",
               f"https://{slug}.bamboohr.com/careers/{jid}/detail")
    except Transient:
        return ""
    jo = (d.get("result") or d or {}).get("jobOpening") or {}
    return cc.ymd(iso=jo.get("datePosted"))


def _bh_place(r):
    """(location, city, state, country) out of a /careers/list row.

    NOT `r.get("atsLocation") or r.get("location")`.  That was the bug: on most
    boards atsLocation is {"country":null,"state":null,"province":null,
    "city":null} — a NON-EMPTY dict, therefore TRUTHY — so `or` never fell
    through to `location`, and the join over its all-None values produced "".
    The real place was sitting unread in the very next key.

    Measured on the 2026-08-24 crawl: 66,751 of 75,690 rows (88.2%) came out
    with no geography at all, across 9,573 boards, while 2,576 boards worked.
    The 2,576 are exactly the ones whose atsLocation IS populated — which is
    why this looked like sparse employer data rather than a bug for so long.

    So: prefer whichever dict actually YIELDS something, atsLocation first
    (it is the structured one, and carries country where `location` does not).
    Emitting city/state/country as their own fields is the other half of the
    fix — remote_us_classify.us_basis proves the US from a country/state pair
    directly, and only falls back to parsing free text when it has to.
    """
    best = {}
    for key in ("atsLocation", "location"):
        v = r.get(key)
        if not isinstance(v, dict):
            continue
        got = {k: str(val).strip() for k, val in v.items()
               if val not in (None, "") and str(val).strip()}
        if got and not best:
            best = got
        elif got:
            # Fill only the holes: atsLocation wins where both speak.
            for k, val in got.items():
                best.setdefault(k, val)
    if not best:
        v = r.get("atsLocation") or r.get("location") or ""
        return (str(v).strip() if isinstance(v, str) else ""), "", "", ""
    city = best.get("city", "")
    state = best.get("state") or best.get("province") or ""
    country = best.get("country", "")
    # Narrowest-first ("Marion, Iowa"), NOT the widest-first string bamboohr's
    # own working boards used to emit ("United States, Iowa, Marion").  Two
    # reasons.  The legacy order cannot be preserved anyway — those boards now
    # return a null atsLocation, so the country is gone from the live payload
    # whatever this does.  And `location` is read and displayed ACROSS sources,
    # where "Marion, Iowa" is the universal form; matching the rest of the
    # corpus beats matching this family's own history.
    #
    # Order is free here, which is the only reason there is a choice to make:
    # us_basis proves the US off the `state` FIELD, ajb.regions_of matches the
    # whole string, and local_build.where_fit segments on [;|] rather than on
    # commas, so town and state marker stay in one segment either way.
    loc = ", ".join(p for p in (city, state, country) if p)
    return loc, city, state, country


def f_bamboohr(s, slug):
    d = gj(s, "bamboohr", f"https://{slug}.bamboohr.com/careers/list")
    out = []
    n_det = 0
    for r in (d.get("result") or []):
        et = CLASSIFY(r.get("employmentStatusLabel"),
                         r.get("jobOpeningName"))
        if not et:
            continue
        loc, city, state, country = _bh_place(r)
        posted = ""
        if BH_DETAIL and _need_detail("bamboohr", slug, r.get("id")):
            posted = _bh_detail(s, slug, r.get("id"))
            n_det += 1
        out.append(cc.row(
            posted=posted,
            source="bamboohr", source_type="ats",
            company=slug.replace("-", " ").title(),
            title=r.get("jobOpeningName"), employment_type=et,
            work_mode=cc.work_mode(r.get("isRemote"),
                                   text=str(r.get("locationType") or "")),
            location=loc, city=city, state=state, country=country,
            # `remote` stays False-by-absence, and that is a decision, not an
            # oversight.  isRemote is null on all 567 jobs sampled across 40
            # boards (2026-08-25), and locationType — its only candidate stand-in
            # — takes three values (0:341, 1:96, 2:130) none of which correlates
            # with "remote" appearing in the location text.  So which value means
            # remote is UNKNOWN, and guessing is not free: geo_of runs its remote
            # leg before the onsite one, so a wrong flag admits foreign rows on
            # the strength of a made-up signal.  The Toronto board in the fixture
            # set is exactly what that would let through.
            remote=bool(r.get("isRemote")),
            apply_url=f"https://{slug}.bamboohr.com/careers/{r.get('id')}",
            job_id=r.get("id"), board=slug,
            board_url=f"https://{slug}.bamboohr.com/careers"))
    return out


def f_workable(s, slug):
    # No ?details=true.  It adds exactly one field per job -- `description` --
    # which this function never reads and contract_common.COLUMNS has no column
    # for, so it was downloaded and discarded on every job of every board.
    # Measured 2026-09-16 on carefound-home-care (45 jobs): 7,033 -> 1,658 wire
    # bytes, 4.2x, with the parsed rows byte-identical on every field this
    # function touches and `description` the ONLY key that differs.  Larger
    # boards are worse: pavago (2,159 jobs) is 27.8x.
    #
    # Be honest about what this does NOT do: the host sends
    # cf-cache-status: DYNAMIC, so every byte is origin work, but a few MB
    # across a multi-hour drain is far too little traffic to be what the rate
    # limiter reacts to.  This is free and lossless, not a fix for the 429s.
    d = gj(s, "workable",
           f"https://apply.workable.com/api/v1/widget/accounts/{slug}")
    company = d.get("name") or slug
    out = []
    for j in (d.get("jobs") or []):
        if not isinstance(j, dict):
            continue
        et = CLASSIFY(j.get("employment_type"), j.get("title"))
        if not et:
            continue
        city = j.get("city") or ""
        state = j.get("state") or ""
        country = j.get("country") or ""
        locstr = ", ".join(x for x in (city, state, country) if x)
        code = j.get("shortcode") or j.get("code") or ""
        # The widget hands back the SHORT link (apply.workable.com/j/{id}),
        # which all_jobs_build.canon_key does not parse — it matches only the
        # org-qualified /{org}/j/{id} form.  Emitting the short form would cost
        # the canonical-ATS merge against the ~482 workable boards Getro
        # already syndicates, so the long form is rebuilt here (verified 200,
        # and it is what 322 of the 376 workable URLs in the master already
        # look like).
        apply_url = (f"https://apply.workable.com/{slug}/j/{code}/" if code
                     else (j.get("application_url") or j.get("shortlink")
                           or j.get("url") or ""))
        out.append(cc.row(
            source="workable", source_type="ats", company=company,
            title=j.get("title"), employment_type=et,
            work_mode=cc.work_mode(j.get("telecommuting"),
                                   text=locstr + " " + (j.get("title") or "")),
            location=locstr, city=city, state=state, country=country,
            remote=bool(j.get("telecommuting")),
            posted=cc.ymd(iso=j.get("published_on") or j.get("created_at")),
            apply_url=apply_url,
            job_id=code, board=slug,
            board_url=f"https://apply.workable.com/{slug}/"))
    return out


# The listing table.  Anchored on the resumator-* class names rather than on
# table geometry, because the surrounding page is customer-branded while the
# table itself is emitted by JazzHR.
_JZ_ROW_RX = re.compile(
    r'<td[^>]*class="[^"]*resumator-job-title-column[^"]*"[^>]*>\s*'
    r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>.*?'
    r'<td[^>]*class="[^"]*resumator-department-column[^"]*"[^>]*>(.*?)</td>'
    r'.*?'
    r'<td[^>]*class="[^"]*resumator-job-location-column[^"]*"[^>]*>(.*?)</td>',
    re.S | re.I)
# The MODERN listing theme, and the reason it exists.  JazzHR now serves most
# boards as a <ul> of cards instead of the resumator-* <table> above, and
# _JZ_ROW_RX matches NOTHING on those pages.  Measured on the 2026-08-24 crawl
# that was 4,833 of 4,893 boards: rows still arrived (via _JZ_LINK_RX, below)
# but with no location at all, so 96.8% of the family — 112,048 rows — could
# never clear any geo gate and was invisible to triage regardless of content.
#
# The card carries location on the LISTING page, so this costs zero extra
# requests.  Both facts are on <li> elements distinguished only by their icon:
#
#   <li class="list-group-item">
#     <h3 class='list-group-item-heading'><a href="URL">TITLE</a></h3>
#     <ul class='list-inline list-group-item-text'>
#       <li><i class='fa fa-map-marker'></i>Bentonville, AR</li>
#       <li><i class='fa fa-sitemap'></i>Central 1002</li>
#
# Anchored on the fa-* icon class rather than on <li> ORDER: a board with no
# department emits only the map-marker item, and positional matching would then
# read its location as a department on exactly the rows that have the least
# data to spare.  Quotes are ['"] throughout because JazzHR emits both.
_JZ_CARD_RX = re.compile(
    r'<li[^>]*class=[\'"][^\'"]*list-group-item[^\'"]*[\'"][^>]*>(.*?)</ul>',
    re.S | re.I)
_JZ_CARD_LINK_RX = re.compile(
    r'<h3[^>]*class=[\'"][^\'"]*list-group-item-heading[^\'"]*[\'"][^>]*>\s*'
    r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
_JZ_CARD_LOC_RX = re.compile(
    r'<li[^>]*>\s*<i[^>]*fa-map-marker[^>]*>\s*</i>(.*?)</li>', re.S | re.I)
_JZ_CARD_DEPT_RX = re.compile(
    r'<li[^>]*>\s*<i[^>]*fa-sitemap[^>]*>\s*</i>(.*?)</li>', re.S | re.I)

_JZ_LINK_RX = re.compile(
    r'href="(https?://[^"]*?applytojob\.com/apply/([A-Za-z0-9]{6,})/[^"]*)"'
    r'[^>]*>(.*?)</a>', re.S | re.I)
_TAG_RX = re.compile(r"<[^>]+>")
_LD_RX = re.compile(
    r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', re.S | re.I)


def _text(fragment):
    return html.unescape(_TAG_RX.sub(" ", fragment or "")).strip(" \t\r\n ")


def _squash(v):
    return re.sub(r"\s+", " ", v or "").strip()


def _jz_detail(s, url):
    """employmentType + datePosted from the job page's JobPosting ld+json."""
    body = gt(s, "jazzhr", url)
    if not body:
        return {}
    for blob in _LD_RX.findall(body):
        try:
            d = json.loads(blob)
        except (ValueError, TypeError):
            continue
        if isinstance(d, list):
            d = next((x for x in d if isinstance(x, dict)
                      and x.get("@type") == "JobPosting"), None)
        if not isinstance(d, dict) or d.get("@type") != "JobPosting":
            continue
        addr = {}
        jl = d.get("jobLocation")
        if isinstance(jl, list):
            jl = jl[0] if jl else None
        if isinstance(jl, dict):
            addr = jl.get("address") or {}
            if not isinstance(addr, dict):
                addr = {}
        return {
            "employment_type": d.get("employmentType"),
            "posted": cc.ymd(iso=d.get("datePosted")),
            "city": addr.get("addressLocality") or "",
            "state": addr.get("addressRegion") or "",
            "country": addr.get("addressCountry") or "",
        }
    return {}


def f_jazzhr(s, slug):
    body = gt(s, "jazzhr", f"https://{slug}.applytojob.com/apply/")
    if not body:
        return []
    seen = {}
    for url, title, dept, loc in _JZ_ROW_RX.findall(body):
        t = _squash(_text(title))
        if t:
            seen.setdefault(url.strip(), (t, _squash(_text(dept)),
                                          _squash(_text(loc))))
    if not seen:
        # The modern card theme.  Tried BEFORE the bare-link fallback because
        # it carries location and department; the fallback carries neither, and
        # silently preferring it is what blinded this family (see _JZ_CARD_RX).
        for card in _JZ_CARD_RX.findall(body):
            m = _JZ_CARD_LINK_RX.search(card)
            if not m:
                continue
            t = _squash(_text(m.group(2)))
            if not t:
                continue
            mloc = _JZ_CARD_LOC_RX.search(card)
            mdept = _JZ_CARD_DEPT_RX.search(card)
            seen.setdefault(m.group(1).strip(),
                            (t, _squash(_text(mdept.group(1))) if mdept else "",
                             _squash(_text(mloc.group(1))) if mloc else ""))
    if not seen:
        # Board rendered without either listing shape (a single posting, or a
        # theme we have not seen).  Fall back to the apply links themselves so
        # the board is not silently recorded as empty.  These rows carry NO
        # location — they are the ones the no-geo census in triage_gap.py
        # counts, and a rising count here means a third theme has shipped.
        for url, _code, title in _JZ_LINK_RX.findall(body):
            t = _squash(_text(title))
            if t:
                seen.setdefault(url.strip(), (t, "", ""))
    company = slug.replace("-", " ").title()
    m = re.search(r'"@type"\s*:\s*"Organization".*?"name"\s*:\s*"([^"]+)"',
                  body, re.S)
    if m:
        company = html.unescape(m.group(1))
    out = []
    for i, (url, (title, dept, loc)) in enumerate(sorted(seen.items())):
        url = html.unescape(url).replace("http://", "https://")
        code = ""
        mm = re.search(r"/apply/([A-Za-z0-9]{6,})/", url)
        if mm:
            code = mm.group(1)
        det = {}
        # `i < JZ_DETAIL_CAP` used to cap by POSITION in the board, which spent
        # the allowance on the first 80 postings of every board and re-fetched
        # the same already-dated ones every run.  Delta-only spends it on
        # postings that have no date yet, wherever they sit.
        if not JZ_NO_DETAIL and _need_detail("jazzhr", slug, code):
            det = _jz_detail(s, url)
        # (none) is JazzHR's literal placeholder for an unset department.
        if dept.lower() in ("(none)", "none"):
            dept = ""
        et = CLASSIFY(det.get("employment_type") or dept, title)
        if not et:
            continue
        out.append(cc.row(
            source="jazzhr", source_type="ats", company=company,
            title=title, employment_type=et,
            work_mode=cc.work_mode(text=loc + " " + title),
            location=loc, city=det.get("city") or "",
            state=det.get("state") or "", country=det.get("country") or "",
            remote="remote" in (loc + " " + title).lower(),
            posted=det.get("posted") or "",
            apply_url=url, job_id=code, board=slug,
            board_url=f"https://{slug}.applytojob.com/apply/"))
    return out


def _rfc822_ymd(v):
    """RSS pubDate is RFC-822 ('Mon, 01 Jun 2026 09:49:51 +0200'), which
    cc.ymd's datetime.fromisoformat cannot read — without this every teamtailor
    row would carry an empty posted date and land in the date-filtered cuts as
    undated."""
    if not v:
        return ""
    try:
        return parsedate_to_datetime(v).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return cc.ymd(iso=v)


def _tt_local(tag):
    """'{http://...}location' -> 'location'.  The feed declares the tt:
    namespace, so ElementTree reports fully-qualified tags."""
    return tag.rsplit("}", 1)[-1].lower()


def _tt_find(node, name):
    for el in node.iter():
        if _tt_local(el.tag) == name and (el.text or "").strip():
            return el.text.strip()
    return ""


def f_teamtailor(s, slug):
    base = f"https://{slug}.teamtailor.com"
    body = gt(s, "teamtailor", f"{base}/jobs.rss")
    out = []
    if body:
        try:
            root = xml_fromstring(body.encode("utf-8", "replace"))
        except (ParseError, DefusedXmlException):
            root = None
        # <channel><title> is the real company name ("Aignostics"), which the
        # slug only approximates — and the slug is sometimes disambiguated
        # ("asklocala-1671530238"), so it makes a poor display name.
        company = slug
        for ch in (root.iter() if root is not None else []):
            if _tt_local(ch.tag) == "channel":
                company = _squash(_tt_find(ch, "title")) or slug
                break
        for item in (root.iter() if root is not None else []):
            if _tt_local(item.tag) != "item":
                continue
            title = _squash(_tt_find(item, "title"))
            if not title:
                continue
            link = _tt_find(item, "link")
            mode = (_tt_find(item, "remotestatus") or "").lower()
            dept = _tt_find(item, "department")
            locs = [el for el in item.iter()
                    if _tt_local(el.tag) == "location"]
            names, cities, countries = [], [], []
            for el in locs:
                nm = _tt_find(el, "name")
                if nm:
                    names.append(nm)
                ct = _tt_find(el, "city")
                if ct:
                    cities.append(ct)
                co = _tt_find(el, "country")
                if co:
                    countries.append(co)
            locstr = "; ".join(dict.fromkeys(names)) or "; ".join(
                dict.fromkeys(cities))
            if mode.startswith("fully") or mode == "remote":
                remote = True
            else:
                remote = False
            et = CLASSIFY(dept, title)
            if not et:
                continue
            out.append(cc.row(
                source="teamtailor", source_type="ats", company=company,
                title=title, employment_type=et,
                work_mode=cc.work_mode(remote or None,
                                       hybrid=(mode == "hybrid") or None,
                                       text=locstr),
                location=locstr, city=cities[0] if cities else "",
                country=countries[0] if countries else "",
                remote=remote,
                posted=_rfc822_ymd(_tt_find(item, "pubdate")),
                apply_url=link, job_id=_tt_find(item, "guid"), board=slug,
                board_url=f"{base}/jobs"))
    if out:
        return out
    # Fallback: the JSON Feed.  No location or department, but it proves the
    # board is alive and carries title/url/date, so an RSS shape we cannot
    # parse does not turn a live board into an empty one.
    d = gj(s, "teamtailor", f"{base}/jobs.json")
    for it in (d.get("items") or []):
        if not isinstance(it, dict):
            continue
        title = _squash(it.get("title"))
        if not title:
            continue
        et = CLASSIFY(None, title)
        if not et:
            continue
        out.append(cc.row(
            source="teamtailor", source_type="ats", company=slug,
            title=title, employment_type=et,
            work_mode=cc.work_mode(text=title),
            posted=cc.ymd(iso=it.get("date_published")),
            apply_url=it.get("url") or "", job_id=it.get("id") or "",
            board=slug, board_url=f"{base}/jobs"))
    return out


FETCH = {"ashby": f_ashby, "smartrecruiters": f_smartrecruiters,
         "recruitee": f_recruitee, "breezy": f_breezy, "bamboohr": f_bamboohr,
         "workable": f_workable, "jazzhr": f_jazzhr,
         "teamtailor": f_teamtailor}


def work(platform, slug, tier="hot"):
    key = f"{platform}:{slug}"
    # Eligibility is decided ONCE, in main(), from the ledger.  This is only a
    # re-entry guard for the current process.  It used to be
    # `if _state.get(key) == "done": return`, which is the single line that kept
    # seven platforms frozen: once a board's first crawl succeeded it could
    # never be fetched again, and by 2026-08-16 all 71,592 keys said "done".
    #
    # Having the skip test here and the queue test in main() also meant two
    # predicates that had to agree and did not — main() queued everything
    # != "done" while state_refresh preserved err: values as corpses, so a
    # permanently broken board was re-queued forever.  One decision, one place.
    with _lock:
        if key in _visited:
            return
        _visited.add(key)
    _seen.tier = tier
    # Per-platform concurrency gate.  throttle() spaces requests in TIME but
    # puts no ceiling on how many threads are in flight at once, and that is
    # the difference that matters to a Cloudflare-fronted host: a burst of 80
    # simultaneous connections trips its per-IP limiter even when the average
    # rate is well inside the budget.  Measured 2026-07-28 against
    # apply.workable.com: 60 concurrent live tenants -> 60x HTTP 200, but 40
    # concurrent issued moments after another burst -> 40x HTTP 429.  Each of
    # those 429s called slow_down(), which pauses the WHOLE platform for 20s;
    # with ~24% of an 84k-board queue being workable and a 320-thread pool,
    # the platform sat in a near-permanent stall-then-burst oscillation.  The
    # symptom was unmistakable once measured: 323 live threads but only ~26
    # established TCP connections, every per-platform rate 10x under its own
    # cap, and an aggregate of 0.91 boards/s against ~41/s of bucket capacity.
    with _gate[platform]:
        _work_one(platform, key, slug)


def _work_one(platform, key, slug):
    s = cc.session()
    _seen.kind = None
    today = datetime.date.today().isoformat()
    try:
        rows = FETCH[platform](s, slug)
        # Rows always beat a gone signal.  This is what makes first-call-wins
        # safe for teamtailor, whose RSS can 404 while its jobs.json fallback
        # returns real postings.
        titled = [r for r in rows
                  if str(r.get("title") or "").strip()]
        if titled:
            outcome = "ok"
        elif getattr(_seen, "kind", None) == "gone":
            outcome = "gone"
        else:
            outcome = "empty"
        for r in rows:
            emit(platform, r)
        with _lock:
            e = _ledger.setdefault(key, {})
            e["last_try"] = today
            if outcome == "ok":
                _state[key] = f"done:{today}"
                before = int(e.get("rows") or 0)
                now = len(titled)
                e["rows"] = now
                e["last_ok"] = today
                e["runs"] = int(e.get("runs") or 0) + 1
                for f in ("empty_streak", "gone_streak", "fail_streak"):
                    e.pop(f, None)
                # A board that had real depth and now returns almost nothing is
                # the signature of a parked page or an interstitial that happens
                # to parse: run_all sets ATS_KEEP_ALL=1, so cc.classify_any
                # never rejects a row and a single junk title is enough to look
                # "authoritative".  Require the collapse to repeat before
                # believing it; until then the board keeps its existing rows.
                if before >= COLLAPSE_MIN and now <= before * COLLAPSE_FRAC:
                    n = int(e.get("collapse_streak") or 0) + 1
                    e["collapse_streak"] = n
                    e["rows"] = before          # do not let one bad read demote it
                    if n < COLLAPSE_STRIKES:
                        _collapsed.append((key, before, now))
                        # Its rows this run are distrusted, so they are dropped
                        # from the staged output too.  Keeping the old rows but
                        # ALSO installing the suspect one leaves the board with
                        # a mix of the two, which is neither the old truth nor
                        # the new one — held back has to mean untouched.
                        _held.setdefault(platform, set()).add(slug)
                    else:
                        e["rows"] = now
                        e.pop("collapse_streak", None)
                        _authoritative.setdefault(platform, set()).add(slug)
                else:
                    e.pop("collapse_streak", None)
                    _authoritative.setdefault(platform, set()).add(slug)
            elif outcome == "gone":
                n = int(e.get("gone_streak") or 0) + 1
                e["gone_streak"] = n
                e.pop("fail_streak", None)
                if n >= GONE_STRIKES:
                    # Confirmed by the upstream on N consecutive fetches.  Only
                    # now may its rows be dropped.  Prefixed "dead:" and not
                    # "gone:" because state_refresh.DEAD is matched with
                    # startswith and contains "dead" but not "gone" — a
                    # "gone:..." value would be silently discarded as a done
                    # marker, forgetting a delisting that cost real network
                    # time to learn.
                    _state[key] = f"dead:gone:{today}"
                    e["gone"] = True
                    e["rows"] = 0
                    _authoritative.setdefault(platform, set()).add(slug)
                else:
                    _state[key] = f"empty:{today}"
            else:
                # Empty but not proven gone: 403, unparseable 200, or a live
                # board with nothing posted.  Keeps whatever rows it already
                # has, and keeps its `rows` count so one bad day cannot demote
                # it out of the HOT tier.
                _state[key] = f"empty:{today}"
                e["empty_streak"] = int(e.get("empty_streak") or 0) + 1
            _outcomes[key] = outcome
    except Exception as e:  # noqa: BLE001 - keep the pool alive
        with _lock:
            _state[key] = f"err:{type(e).__name__}"
            _failed_now.add(key)
            _outcomes[key] = "err"
            le = _ledger.setdefault(key, {})
            le["last_try"] = today
            le["fail_streak"] = int(le.get("fail_streak") or 0) + 1
    finally:
        s.close()


def load_slugs():
    """getro_ats_slugs.json, plus every *_ats_slugs.json sidecar.

    contract_mine_getro.py rewrites getro_ats_slugs.json from scratch on each
    getro refresh, so slugs found by the Wayback discovery layer have to live
    in sidecar files and be merged here — writing them back into the getro file
    would just get them clobbered on the next run.
    """
    merged = {}
    # smoke_* sidecars are test artifacts (SMOKE=<n> prefixes every file it
    # writes) and must never feed a production queue: smoke_wayback_ats_
    # slugs.json was being read here, harmless only because its 430 slugs
    # were rippling, which is not in PLATFORMS.
    paths = [SLUGS] + sorted(
        q for q in glob.glob(os.path.join(BASE, "*_ats_slugs.json"))
        if os.path.abspath(q) != os.path.abspath(SLUGS)
        and not os.path.basename(q).startswith("smoke_"))
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            print(f"  WARNING: {os.path.basename(path)} unreadable ({e}) "
                  f"— skipped", flush=True)
            continue
        if not isinstance(d, dict):
            continue
        n = 0
        for platform, slugs in d.items():
            # Value may be a {slug: count} dict or a plain list; both iterate
            # to slugs, which is all we need.
            tgt = merged.setdefault(platform, {})
            for slug in (slugs or {}):
                if slug and slug not in tgt:
                    tgt[slug] = 1
                    n += 1
        print(f"  slugs from {os.path.basename(path)}: +{n} new", flush=True)
    return merged


def _report_outcomes():
    """Per-platform outcome histogram + what this run is authoritative for.

    This is the operator's only window into the new machinery, and it exists
    because the previous summary line was `kept per platform: {...}` — a raw
    emit count with no denominator.  A run that fetched 88 rows from seven
    platforms printed exactly the same SHAPE of line as a healthy one, which is
    how the pipes stayed frozen without anyone noticing.
    """
    if not _outcomes:
        return
    per = {}
    for key, out in _outcomes.items():
        plat = key.split(":", 1)[0]
        per.setdefault(plat, {}).setdefault(out, 0)
        per[plat][out] += 1
    print(f"\n  {'platform':18s} {'ok':>7s} {'empty':>7s} {'gone':>6s} "
          f"{'err':>6s} {'authoritative':>14s}")
    for plat in sorted(per):
        d = per[plat]
        print(f"  {plat:18s} {d.get('ok', 0):>7,} {d.get('empty', 0):>7,} "
              f"{d.get('gone', 0):>6,} {d.get('err', 0):>6,} "
              f"{len(_authoritative.get(plat, ())):>14,}")
    if _collapsed:
        print(f"\n  {len(_collapsed)} board(s) collapsed vs their known row "
              f"count and were HELD BACK from delisting pending a re-check:")
        for key, before, now in sorted(_collapsed,
                                       key=lambda x: -x[1])[:10]:
            print(f"    {key:44s} {before:>6,} -> {now:,}")
    if DELIST:
        print(f"\n  delisting ENABLED for: {', '.join(sorted(DELIST))} "
              f"(mode={DELIST_MODE}"
              + (f", stamping delisted_on={RUN_DATE})" if DELIST_MODE == "flag"
                 else ", stale rows are REMOVED)"))
    else:
        print("\n  delisting disabled (ATS_DELIST unset) — append-only merge")


def retire_repeat_failures():
    """Retire boards that have failed MAX_FAILS runs running — but only on a
    platform that was otherwise healthy this run.

    Without the health gate a single host-wide outage would retire thousands of
    perfectly good boards in one pass, and nothing would ever revisit them.
    With it, "this board is junk" and "this host was down" stay distinguishable,
    which is a distinction this file already knows matters (see the durable
    per-IP workable penalty in the RPS notes).

    Retired boards get state "err:retired:<Cls>".  The "err" prefix is not
    cosmetic: state_refresh.DEAD and coverage_report.DEAD_MARKERS both match
    with startswith, so an err-prefixed value is preserved as a corpse and
    counted as dead by both, with no change needed to either file.
    """
    seen = {}
    for key, out in _outcomes.items():
        plat = key.split(":", 1)[0]
        d = seen.setdefault(plat, [0, 0])
        d[0] += 1
        if out == "err":
            d[1] += 1
    healthy = {p for p, (n, bad) in seen.items()
               if n and bad / n < FAIL_RATE_MAX}
    sick = sorted(set(seen) - healthy)
    if sick:
        print(f"  platform failure rate >= {FAIL_RATE_MAX:.0%} on "
              f"{', '.join(sick)} — NOT retiring any of their boards this run",
              flush=True)
    n = 0
    for key in _failed_now:
        plat = key.split(":", 1)[0]
        if plat not in healthy:
            continue
        e = _ledger.get(key) or {}
        if int(e.get("fail_streak") or 0) < MAX_FAILS:
            continue
        e["retired"] = True
        _ledger[key] = e
        cls = str(_state.get(key) or "err:Unknown").split(":", 1)[-1]
        _state[key] = f"err:retired:{cls}"
        n += 1
    if n:
        print(f"  retired {n:,} board(s) after {MAX_FAILS} consecutive failed "
              f"runs — they will not be queued again", flush=True)


def recover_staging():
    """A staging file left behind by a crashed attempt is appended WHOLESALE.

    Its boards are already marked authoritative in the state file, but the merge
    that would have honoured that never ran, so the safe move is to keep
    everything: over-keeping duplicates rows that norm_dedupe collapses in the
    build tail, while under-keeping loses postings outright.  This repo already
    commits to that direction — "collapsing an append-only file is always safe;
    emptying one to refill it is not".
    """
    for plat in PLATFORMS:
        st = _stage(plat)
        if not os.path.exists(st) or not os.path.getsize(st):
            continue
        n = 0
        with open(_live(plat), "a", encoding="utf-8") as out, \
                open(st, encoding="utf-8", errors="replace") as src:
            for line in src:
                out.write(line)
                n += 1
        os.remove(st)
        print(f"  recovered {n:,} staged rows for {plat} from an interrupted "
              f"run (appended whole, not board-scoped)", flush=True)


def merge_generation():
    """Install this run's rows, board-scoped.

    Contract: the live JSONL becomes THIS run's rows for every board this run
    was authoritative for, plus every pre-existing row whose board it was NOT.

    This replaces backfill_from_prev(), which appended the entire rotated-away
    generation whenever the live file came out smaller.  That made the live file
    a superset of yesterday's and leaned on downstream dedupe — which is why a
    posting deleted upstream was resurrected on every run and never died.  On
    2026-08-16 four of the eight files came out byte-identical to their .prev.

    A board is authoritative only if it returned at least one titled row, or was
    confirmed gone GONE_STRIKES times running.  Everything else — untouched,
    transient failure, empty-but-not-provably-gone — keeps the rows it has.
    That is deliberate and it is what makes this survivable: run_all sets
    ATS_KEEP_ALL=1, cc.classify_any never returns None, and so there is no
    row-level admission test anywhere in the f_* fetchers.  A parked page that
    parses as one titled row would otherwise make a board "authoritative" and
    delete its real postings.  Hence also the two guards below, which are hard
    refusals, not warnings.
    """
    for plat in PLATFORMS:
        live, prev, stage = _live(plat), _prev(plat), _stage(plat)
        auth = set(_authoritative.get(plat, ()))
        if plat not in DELIST:
            auth = set()        # append-only for this platform
        have_stage = os.path.exists(stage) and os.path.getsize(stage)
        if not have_stage and not auth:
            continue

        live_n = _count_lines(live)
        stage_n = _count_lines(stage) if have_stage else 0

        # Disk guard: the rewrite needs room for a full second copy.
        need = (os.path.getsize(live) if os.path.exists(live) else 0) + \
               (os.path.getsize(stage) if have_stage else 0)
        try:
            v = os.statvfs(cc.NORM_DIR)
            free = v.f_bavail * v.f_frsize
        except Exception:
            free = need + 1
        if free < need:
            print(f"  !! REFUSED {plat}: need {need / 1e6:.0f}MB, "
                  f"{free / 1e6:.0f}MB free — leaving live file untouched",
                  flush=True)
            continue

        # NOTE there is deliberately no "delisted boards <= N% of boards" guard.
        # It reads as prudent and is actively wrong here: HOT is 12% of boards
        # but 80% of rows, and per platform it ranges to bamboohr's 4,154 of
        # 10,275 row-bearing boards — 40%.  A healthy hot run legitimately
        # delists a large share of boards and writes them straight back.  What
        # must never happen is delisting them and writing back far less, which
        # is exactly what the row-shrink guard below measures.  The per-board
        # collapse check in _work_one covers the parked-page case.

        tmp = live + ".tmp"
        kept = dropped = flagged = 0
        # flag mode needs to know which postings this run actually SAW, or it
        # would keep a stale copy of every row alongside its own fresh copy and
        # double the file.  Identity is (board, job_id) -- the same key
        # bh_build_combined dedupes on.
        fresh_ids = set()
        if DELIST_MODE == "flag" and auth and have_stage:
            with open(stage, encoding="utf-8", errors="replace") as src:
                for line in src:
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    if d.get("board") in auth:
                        fresh_ids.add((d.get("board"),
                                       d.get("job_id") or d.get("apply_url")))
        with open(tmp, "w", encoding="utf-8") as out:
            # THIS RUN'S ROWS FIRST.  norm_dedupe and bh_build_combined are both
            # first-wins on identity, so whichever copy appears first is the one
            # that survives.  Fresh-first keeps the ordering the old
            # append-then-backfill produced; reversing it would silently make
            # every duplicated job resolve to its stale copy.
            held = _held.get(plat, set())
            if have_stage:
                with open(stage, encoding="utf-8", errors="replace") as src:
                    for line in src:
                        if held:
                            try:
                                if json.loads(line).get("board") in held:
                                    stage_n -= 1
                                    continue
                            except Exception:
                                pass
                        out.write(line)
            if os.path.exists(live):
                with open(live, encoding="utf-8", errors="replace") as src:
                    for line in src:
                        try:
                            d = json.loads(line)
                        except Exception:
                            out.write(line)     # never lose a row we cannot read
                            kept += 1
                            continue
                        b = d.get("board")
                        if b in auth:
                            jid = d.get("job_id") or d.get("apply_url")
                            if DELIST_MODE != "flag" or (b, jid) in fresh_ids:
                                # drop mode, or this run re-fetched the very
                                # same posting -- the fresh copy above wins.
                                dropped += 1
                                continue
                            # The board answered and this posting was not in
                            # the answer: it is gone.  Keep the row, date the
                            # absence, and never re-date one already stamped --
                            # the FIRST time it vanished is the true date.
                            if not d.get("delisted_on"):
                                d["delisted_on"] = RUN_DATE
                                flagged += 1
                            out.write(json.dumps(d, ensure_ascii=False) + "\n")
                            kept += 1
                            continue
                        out.write(line)
                        kept += 1

        out_n = stage_n + kept
        shrink = 1 - (out_n / live_n) if live_n else 0
        # Print the DELTA, signed the way a reader expects: + means the file
        # grew.  `shrink` is the guard's own quantity and has the opposite sign,
        # which is fine internally but reads as a 50% loss when a run in fact
        # gained 50%.
        delta = -shrink
        msg = (f"  merge {plat}: +{stage_n:,} fresh, kept {kept:,}, "
               f"dropped {dropped:,} from {len(auth):,} authoritative boards "
               + (f"flagged {flagged:,} delisted, " if flagged else "")
               + f"-> {out_n:,} rows (was {live_n:,}, {delta * 100:+.1f}%)")

        if live_n and shrink > MAX_SHRINK:
            os.remove(tmp)
            # Print the SAME accounting a successful merge prints.  It is
            # already computed above and was being discarded here, which is why
            # bamboohr and jazzhr refused on 2026-08-26 with nothing in the log
            # to say whether the cause was a short fetch, a delisting wave, or
            # a bad live_n -- the one line that answers it existed and was
            # thrown away.  A refusal is the case that MOST needs the numbers.
            print(f"  !! REFUSED {plat}: would lose {shrink * 100:.1f}% of rows "
                  f"(cap {MAX_SHRINK * 100:.0f}%) — appending instead",
                  flush=True)
            print(f"     would have been: {msg.strip()}", flush=True)
            # A one-off refusal is a bad night; a STREAK means this platform's
            # delisting has silently stopped reconciling and the live file is
            # only ever growing.  Counted in the ledger so the next run can say
            # so out loud instead of repeating an isolated-looking warning.
            with _lock:
                rm = al.meta(_ledger).setdefault("refused", {})
                r = rm.setdefault(plat, {})
                r["streak"] = int(r.get("streak") or 0) + 1
                r["last"] = RUN_DATE
                r["shrink"] = round(shrink, 4)
                r["live_n"], r["stage_n"] = live_n, stage_n
                if r["streak"] > 1:
                    print(f"     !! {plat} has now refused {r['streak']} runs "
                          f"in a row — delisting is not reconciling",
                          flush=True)
            if have_stage:
                _append(stage, live)
                os.remove(stage)
            continue
        with _lock:
            al.meta(_ledger).get("refused", {}).pop(plat, None)

        if STAGE_ONLY:
            os.remove(tmp)
            print(msg + "   [STAGE_ONLY: not installed; staging kept, the "
                        "next run appends it whole]", flush=True)
            continue

        # One generation of rollback, taken once per RUN rather than once per
        # attempt — otherwise attempt 2 would overwrite .prev with attempt 1's
        # output and the pre-run state would be gone.
        m = al.meta(_ledger)
        pg = m.setdefault("prev_gen", {})
        if os.path.exists(live) and (not RUN_ID or pg.get(plat) != RUN_ID):
            os.replace(live, prev)
            pg[plat] = RUN_ID or datetime.date.today().isoformat()
        os.replace(tmp, live)
        if have_stage:
            os.remove(stage)
        print(msg, flush=True)


def _count_lines(path):
    if not path or not os.path.exists(path):
        return 0
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n


def _append(src, dst):
    with open(dst, "a", encoding="utf-8") as out, \
            open(src, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            out.write(line)


def build_queue(slugs):
    """[(platform, slug, tier)] for this run.

    The ONE place eligibility is decided.  Previously main() queued everything
    whose state was != "done" and work() skipped everything == "done"; two
    predicates that had to agree, and which disagreed for any board sitting in
    err: — state_refresh preserves those as corpses while this queued them
    forever.  On 2026-08-16 that was 32 junk wayback slugs re-fetched on all 40
    of run_all's supervised attempts.

    Boards the ledger has never seen are NEW and always eligible: they are
    freshly discovered slugs, and a discovery run must not have to wait for a
    cold slice to come round.  Capped so a big sweep cannot swamp a refresh.
    """
    if os.environ.get("ATS_TIERS") == "0":
        # Escape hatch: the pre-ledger behaviour, everything not done.
        return [(p, s, "hot") for p in PLATFORMS
                for s in (slugs.get(p) or {})
                if s and _state.get(f"{p}:{s}") != "done"]

    known = {k for k in _ledger if k != "__meta__"}
    el = {(p, s): t for p, s, t in al.eligible(_ledger, PLATFORMS)}
    new = []
    for p in PLATFORMS:
        for s in (slugs.get(p) or {}):
            if s and f"{p}:{s}" not in known:
                new.append((p, s, "new"))
    # The cap protects a REFRESH run from being swamped by a discovery sweep.
    # A platform still doing its first crawl has no refresh to protect and its
    # real limiter is the wall-clock budget, so it is exempt — capping workable
    # at 5,000 while it can only reach ~780 boards a run just makes the log lie
    # about what is queued.
    uncapped = [j for j in new if j[0] in al.FIRST_CRAWL]
    cappable = [j for j in new if j[0] not in al.FIRST_CRAWL]
    if len(cappable) > al.NEW_CAP:
        print(f"  {len(cappable):,} newly discovered slugs, capping at "
              f"{al.NEW_CAP:,} for this run", flush=True)
        cappable = cappable[:al.NEW_CAP]
    new = uncapped + cappable
    have = {(p, s) for p, s, _ in new}
    jobs = [(p, s, t) for (p, s), t in el.items() if (p, s) not in have]
    jobs += new
    # Tier order is submission order.  ThreadPoolExecutor submits in sequence,
    # so hot drains first and the budget truncates the COLD tail — which is
    # exactly the population worth dropping.  Shuffling happens WITHIN a tier so
    # the platform interleaving that random.shuffle was added for is preserved.
    # Platforms whose 'new' tier is ordered by the live-rate prior rather than
    # left to chance.  Comma list; empty disables it entirely.
    prio_plats = [x.strip() for x in
                  (os.environ.get("ATS_PRIORITY") or "workable").split(",")
                  if x.strip()]
    prio = {pl: _priority(pl) for pl in prio_plats}
    out = []
    for tier in ("hot", "new", "warm", "cold"):
        chunk = [j for j in jobs if j[2] == tier]
        random.shuffle(chunk)
        if tier == "new" and any(prio.values()):
            # Shuffle FIRST, then stable-sort by priority.  That keeps the
            # random order inside each band, so the platform interleaving the
            # shuffle exists for survives -- sorting a sorted list would hand
            # one platform the whole head of the queue.
            chunk.sort(key=lambda j: prio.get(j[0], {}).get(j[1].lower(), 0))
        out += chunk
    return out


def _priority(platform):
    """slug -> 0 (fetch first) or 1, from the candidate file's `sources` column.

    Only for platforms named in ATS_PRIORITY.  Everything else keeps the plain
    shuffle, so this cannot reorder a platform nobody measured.

    WHY, measured 2026-09-16 on workable against ats_workable_yield_ledger.json
    joined to workable_candidates.csv:

        slug carries a corpus tag   probed  2,822   live 2,754 (97.6%)  29.4 rows/probed
        wayback-only                probed 10,241   live   296 ( 2.9%)   0.4 rows/probed

    Of 9,121 unreached slugs, 1,298 are corpus-tagged and 7,823 are
    wayback-only, so 92% of the remaining ROWS sit in 14% of the remaining
    BOARDS.  A corpus tag means the slug was seen in a live posting somewhere;
    a wayback-only slug is an archive artifact that may never have been a board.

    build_queue shuffled the whole 'new' tier at random, so a 30-minute run
    spent ~86% of its ~715 boards on slugs with a 2.9% chance of carrying
    anything, and the near-certain ones waited for run #12.  This does not make
    the fetcher faster; it changes WHICH boards a truncated budget reaches.
    """
    path = os.path.join(BASE, f"{platform}_candidates.csv")
    if not os.path.exists(path):
        return {}
    out = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                slug = (r.get("slug") or r.get("token") or "").strip().lower()
                if not slug:
                    continue
                src = r.get("sources") or ""
                out[slug] = 0 if ("alljobs" in src or "getro" in src) else 1
    except OSError:
        return {}
    return out


def main():
    recover_staging()
    slugs = load_slugs()
    jobs = build_queue(slugs)
    per = {}
    for p, _s, t in jobs:
        per.setdefault(p, {}).setdefault(t, 0)
        per[p][t] += 1
    print(f"pending boards: {len(jobs)} across {PLATFORMS}", flush=True)
    if per:
        print(f"  {'platform':18s} {'hot':>7s} {'new':>7s} {'warm':>7s} "
              f"{'cold':>7s}")
        for p in sorted(per):
            d = per[p]
            print(f"  {p:18s} {d.get('hot', 0):>7,} {d.get('new', 0):>7,} "
                  f"{d.get('warm', 0):>7,} {d.get('cold', 0):>7,}", flush=True)
    # Submitted in PLATFORMS order, the pool drains one platform at a time and
    # every worker ends up blocked on that platform's token bucket while the
    # other seven sit idle.  Measured 2026-07-28 on the 91,738-board post-sweep
    # run: 3.2 boards/s for 15 minutes, the whole pool queued behind
    # smartrecruiters' deliberate 3 rps cap — a 7.7h projection against ~55 rps
    # of aggregate bucket capacity going unused.  Interleaving lets all eight
    # drain concurrently.  Same fix, same reason, as the random.shuffle in
    # slug_discover_boards.crawl().
    # (Queue order is set in build_queue: shuffled within tier, hot first.)

    global JZ_NO_DETAIL
    n_jz = sum(1 for p, _, _t in jobs if p == "jazzhr")
    if JZ_FORCE_DETAIL:
        JZ_NO_DETAIL = False
    elif not JZ_NO_DETAIL and n_jz > JZ_DETAIL_MAX_BOARDS and not DETAIL_BUDGET:
        # Only when the run is UNBOUNDED.  This switch exists because detail
        # used to be capped per BOARD, so a big queue meant unbounded total
        # cost and the only defence was to turn it off wholesale.  With
        # ATS_DETAIL_BUDGET set the cost is bounded by the budget no matter how
        # many boards are queued, so disabling on board count now buys nothing
        # and costs every date -- jazzhr has 14,830 boards, so this branch fired
        # on essentially every run and is half of why 98.2% of jazzhr rows carry
        # no date (run_all's JZ_NO_DETAIL=1 was the other half).
        JZ_NO_DETAIL = True
        print(f"  jazzhr: {n_jz} boards > {JZ_DETAIL_MAX_BOARDS} and no "
              f"ATS_DETAIL_BUDGET — per-job detail fetch DISABLED for this run "
              f"(rows lose posted date, employmentType and the "
              f"city/state/country TRIPLE; the listing `location` string "
              f"survives on both board themes).  Force with JZ_DETAIL=1.",
              flush=True)
    elif not JZ_NO_DETAIL:
        print(f"  jazzhr: {n_jz} boards — per-job detail fetch enabled "
              f"(delta-only, budget {DETAIL_BUDGET or 'unbounded'})",
              flush=True)

    # Index what is already dated, AFTER the switch above has settled so this
    # is skipped when no detail will be fetched at all.
    _want_dated = [pl for pl in ("jazzhr", "bamboohr") if pl in PLATFORMS
                   and ((pl == "jazzhr" and not JZ_NO_DETAIL)
                        or (pl == "bamboohr" and BH_DETAIL))]
    if _want_dated:
        _load_dated(_want_dated)

    done = 0
    stopped_early = False
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, p, sl, t) for p, sl, t in jobs]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 250 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                    print(f"  {done}/{len(jobs)} boards  kept={_counts}",
                          flush=True)
                if deadline and time.time() > deadline:
                    stopped_early = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        # The tail runs on EVERY exit path — budget, exception, KeyboardInterrupt,
        # SIGTERM.  It used to sit inline after the pool, so anything escaping
        # cf.as_completed skipped save_state, wr.close() AND the merge in one go,
        # leaving this run's rows stranded in staging.  Harmless while the
        # fetcher only appended; not once the live file is rewritten.
        #
        # Writers close BEFORE the state is persisted.  cc.Writer flushes every
        # 500 rows on top of Python's own buffer, so with the old order a kill
        # between the two persisted a "done" marker for up to 499 rows that were
        # never written — and a done marker is what authorises dropping a
        # board's previous rows.
        for wr in _writers.values():
            wr.close()
        retire_repeat_failures()
        cc.save_state(STATE, _state)
        al.save(_ledger)
        _report_outcomes()
        merge_generation()
        al.save(_ledger)    # merge_generation stamps __meta__.prev_gen
    if _n429:
        print(f"  rate-limit backoffs: {_n429}", flush=True)
    if _detail_spent["n"] or _detail_spent["skipped_known"]:
        # Say the backfill's remaining depth out loud.  "still undated" is what
        # tells you whether this is converging or stuck; the budget is a per-run
        # ceiling, so a flat number across runs means the cap is binding and
        # ATS_DETAIL_BUDGET should rise.
        print(f"  detail (delta-only): {_detail_spent['n']:,} fetched, "
              f"{_detail_spent['skipped_known']:,} already dated, "
              f"{_detail_spent['over_budget']:,} deferred to the next run "
              f"(ATS_DETAIL_BUDGET={DETAIL_BUDGET})", flush=True)
        if _detail_spent["over_budget"]:
            print("  INCOMPLETE: postings still undated — they stay queued "
                  "for the next run", flush=True)
    print(f"DONE ats_fetch. kept per platform: {_counts}", flush=True)
    if stopped_early:
        # Boards not reached are left ABSENT from the state file, never marked
        # done — the next run picks them up exactly where this one stopped.
        # Exit 0 so run_all's supervisor moves on to the build instead of
        # relaunching into the same budget 40 times.
        left = len(jobs) - done
        print(f"BUDGET REACHED after {BUDGET_MIN}min: {done}/{len(jobs)} "
              f"boards fetched, {left} still pending and still queued for the "
              f"next run.  Raise or remove with ATS_BUDGET_MIN=<n> (0 = no "
              f"limit).", flush=True)
        return
    # Non-zero so run_all.py's supervisor actually retries.  Without this the
    # 429 handling above would be inert: boards left in err: state would never
    # be revisited, which is the bug it was written to fix.
    if _failed_now:
        raise SystemExit(f"{len(_failed_now)} boards failed — re-run to retry")


if __name__ == "__main__":
    main()
