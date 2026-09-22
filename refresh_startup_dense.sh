#!/bin/bash
# refresh_startup_dense.sh — re-fetch ONLY the startup-dense sources, then
# rebuild the all-jobs master CSV (NOT the monolithic workbook).
#
# Families: getro · lever · yc · greenhouse · ashby
# These are the five that actually feed the startup tiers; every other source
# keeps the JSONL it already has on disk, so nothing else is re-pulled.
#
# Rotation follows run_all.rotate_family: each family's JSONL moves to
# *.prev.jsonl (one generation kept) and its fetch state is deleted, so the
# fetchers do a true re-fetch instead of resuming a finished crawl.
# The *_posted_ledger.json files are NEVER touched — they exist so a
# first-seen posted date survives every refresh.
#
# Discovery (Wayback CDX board hunts) is deliberately SKIPPED: it costs hours,
# is rate-limited and flaky, and finds new *boards* — new *postings* come from
# re-fetching the boards already known.
#
# Detach with nohup; plain background jobs get killed mid-run here.
# Progress: refresh.status + logs/refresh_2026-07-30/<stage>.out
cd /Users/michaelfornal/Documents/jb || exit 1

LOGD=logs/refresh_2026-07-30
mkdir -p "$LOGD"
STATUS=refresh.status

say() { echo "[$(date -u '+%F %T')] $*" >> "$STATUS"; }

run_stage() { # <script.py> <tag> <max_attempts>
  local SCRIPT=$1 TAG=$2 MAX=$3 i=0 rc=1
  while [ $i -lt "$MAX" ]; do
    i=$((i+1))
    echo "=== [$TAG] attempt $i $(date -u '+%F %T') ===" >> "$LOGD/$TAG.out"
    python3 "$SCRIPT" >> "$LOGD/$TAG.out" 2>&1
    rc=$?
    echo "=== [$TAG] attempt $i exit $rc ===" >> "$LOGD/$TAG.out"
    [ $rc -eq 0 ] && { say "$TAG ok (attempt $i)"; return 0; }
    sleep 5
  done
  say "$TAG GAVE UP after $i attempts (continuing)"
  return $rc
}

rotate() { # <jsonl> <state...>
  local J=$1; shift
  [ -f "$J" ] && mv -f "$J" "${J%.jsonl}.prev.jsonl" && say "rotated $J"
  for S in "$@"; do rm -f "$S"; done
}

say "START refresh (getro,lever,yc,greenhouse,ashby)"

# ---- rotate everything up front, so a family that finishes early cannot be
# merged against another family's half-rotated file.
rotate getro_all_jobs.jsonl     getro_fetch_state.json getro_pdl_state.json
rotate lever_all_jobs.jsonl     lever_fetch_state.json
rotate yc_all_jobs.jsonl        yc_fetch_state.json
rotate greenhouse_all_jobs.jsonl greenhouse_fetch_state.json
rotate ashby_all_jobs.jsonl     ashby_fetch_state.json

# ---- fetch phase: five chains in parallel (each is network-bound on a
# different host, so they do not contend).  getro is the long pole.
(
  run_stage getro_check_live.py gh_getro_live 3   # refresh the live-board census
  run_stage getro_fetch_jobs.py getro_fetch 40
  run_stage getro_fetch_pdl.py  getro_pdl   40    # appends to the same JSONL
  say "getro chain done"
) &
GETRO=$!

( run_stage lever_fetch_jobs.py      lever_fetch 40;  say "lever chain done" ) &
LEVER=$!
( run_stage yc_fetch_jobs.py         yc_fetch 40;     say "yc chain done" ) &
YC=$!
( run_stage greenhouse_fetch_jobs.py gh_fetch 40;     say "greenhouse chain done" ) &
GH=$!
( run_stage ashby_fetch_jobs.py      ash_fetch 40;    say "ashby chain done" ) &
ASH=$!

wait $GETRO $LEVER $YC $GH $ASH
say "ALL FETCHES DONE"

# ---- build phase: sequential (these are the memory-heavy steps)
run_stage getro_build_sheet.py      getro_build 2
run_stage lever_build_sheet.py      lever_build 2
run_stage greenhouse_build_sheet.py gh_build    2
run_stage ashby_build_sheet.py      ash_build   2
run_stage yc_build_sheet.py         yc_build    2
say "family masters rebuilt"

# --csv-only: regenerate all_jobs_master.csv and STOP. The 569 MB monolithic
# workbook is deliberately not rebuilt.
echo "=== [all_jobs_build] $(date -u '+%F %T') ===" >> "$LOGD/all_jobs_build.out"
python3 all_jobs_build.py --csv-only >> "$LOGD/all_jobs_build.out" 2>&1
say "all_jobs_build --csv-only rc=$?"

say "ALL DONE"
