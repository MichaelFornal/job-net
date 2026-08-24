#!/bin/bash
# refresh_gh_ashby.sh — re-fetch Greenhouse + Ashby, in parallel, then rebuild
# their masters.  These two are the gap in run_all.py: its build tail runs
# greenhouse_build_sheet.py / ashby_build_sheet.py but NOTHING re-fetches their
# JSONLs, so a bare run_all merges whatever stale CSV happens to be on disk.
#
# Run this to completion BEFORE run_all.py.  Running it concurrently would let
# run_all's build tail read a half-written JSONL.
#
# Discovery is deliberately skipped for both: ash_run_all.sh would run
# ashby_discover_boards.py first (Wayback CDX, hours, flaky), and it finds new
# *boards* — new *postings* come from re-fetching the boards already known.
# This mirrors refresh_startup_dense.sh, which calls ashby_fetch_jobs.py direct.
#
# Rotation follows run_all.rotate_family: JSONL -> *.prev.jsonl, fetch state
# deleted.  The *_posted_ledger.json files are NEVER touched.
#
# Detach with nohup; plain background jobs get killed mid-run here.
cd "$(cd "$(dirname "$0")" && pwd)" || exit 1

LOGD=logs/refresh_$(date -u +%F)
mkdir -p "$LOGD"
STATUS=refresh_gh_ashby.status

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

say "START refresh (greenhouse,ashby)"
rotate greenhouse_all_jobs.jsonl greenhouse_fetch_state.json
rotate ashby_all_jobs.jsonl      ashby_fetch_state.json

( run_stage greenhouse_fetch_jobs.py gh_fetch 40; say "greenhouse fetch done" ) &
GH=$!
( run_stage ashby_fetch_jobs.py      ash_fetch 40; say "ashby fetch done" ) &
ASH=$!
wait $GH $ASH
say "FETCHES DONE"

# Sequential: these are the memory-heavy steps.  run_all.py's tail rebuilds
# both again later, which is harmless — but doing it here means the masters are
# already consistent if run_all is interrupted.
run_stage greenhouse_build_sheet.py gh_build  2
run_stage ashby_build_sheet.py      ash_build 2
say "ALL DONE"
