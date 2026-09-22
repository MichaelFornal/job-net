#!/bin/bash
# ash_run_all.sh — Ashby pipeline orchestrator: discover -> fetch -> build,
# each stage under a retry loop (every stage is resumable via its own state
# file, so re-running only redoes missing/failed items). Detach with nohup —
# plain background tasks in this environment get killed mid-run.
#
# There is no probe stage: Ashby's only public board endpoint IS the jobs
# endpoint, so ashby_fetch_jobs.py classifies live/dead as it fetches.
#
# Pass --refresh to rotate the JSONL + state first (a true re-fetch rather
# than a resume). Progress: ash_pipeline.status + per-stage ash_<stage>.out.
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

echo "START $(date -u '+%F %T')" >> ash_pipeline.status
if [ "$1" = "--refresh" ]; then
  [ -f ashby_all_jobs.jsonl ] && mv -f ashby_all_jobs.jsonl ashby_all_jobs.prev.jsonl
  rm -f ashby_fetch_state.json
  echo "rotated $(date -u '+%F %T')" >> ash_pipeline.status
fi
run_stage ashby_discover_boards.py ash_discover 25
echo "discover rc=$? $(date -u '+%F %T')" >> ash_pipeline.status
run_stage ashby_fetch_jobs.py ash_fetch 25
echo "fetch rc=$? $(date -u '+%F %T')" >> ash_pipeline.status
run_stage ashby_build_sheet.py ash_build 3
echo "build rc=$? $(date -u '+%F %T')" >> ash_pipeline.status
echo "ALL DONE $(date -u '+%F %T')" >> ash_pipeline.status
