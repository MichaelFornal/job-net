#!/bin/bash
# gh_refresh.sh — refresh Greenhouse jobs only (no re-discovery): rotate the
# JSONL + state so the fetch is a true refresh, re-fetch every live board from
# the existing greenhouse_job_boards.csv, then rebuild the master CSV + sheet.
# Mirrors the rotate-then-fetch semantics run_all.py uses for getro/lever.
cd /Users/michaelfornal/Documents/jb || exit 1

run_stage() { # <script.py> <tag> <max_attempts>
  local SCRIPT=$1 TAG=$2 MAX=$3 i=0 rc=1
  while [ $i -lt "$MAX" ]; do
    i=$((i+1))
    echo "=== [$TAG] attempt $i $(date -u '+%F %T') ===" >> "$TAG.out"
    python3 "$SCRIPT" >> "$TAG.out" 2>&1
    rc=$?
    echo "=== [$TAG] attempt $i exit $rc ===" >> "$TAG.out"
    [ $rc -eq 0 ] && return 0
    sleep 5
  done
  echo "=== [$TAG] gave up after $i attempts (continuing) ===" >> "$TAG.out"
  return $rc
}

echo "START $(date -u '+%F %T')" >> gh_refresh.status
[ -f greenhouse_all_jobs.jsonl ] && mv -f greenhouse_all_jobs.jsonl greenhouse_all_jobs.prev.jsonl
rm -f greenhouse_fetch_state.json
echo "rotated $(date -u '+%F %T')" >> gh_refresh.status
run_stage greenhouse_fetch_jobs.py gh_fetch 25
echo "fetch rc=$? $(date -u '+%F %T')" >> gh_refresh.status
run_stage greenhouse_build_sheet.py gh_build 3
echo "build rc=$? $(date -u '+%F %T')" >> gh_refresh.status
echo "ALL DONE $(date -u '+%F %T')" >> gh_refresh.status
