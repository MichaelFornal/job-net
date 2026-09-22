#!/bin/bash
# Greenhouse pipeline orchestrator: discover -> probe -> fetch -> build,
# each stage under a retry loop (every stage is resumable via its own state
# file, so re-running only redoes missing/failed items). Detach with nohup —
# plain background tasks in this environment get killed mid-run.
# Progress: gh_pipeline.status + per-stage gh_<stage>.out logs.
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
  echo "=== [$TAG] gave up after $i attempts (continuing pipeline) ===" >> "$TAG.out"
  return $rc
}

echo "START $(date -u '+%F %T')" >> gh_pipeline.status
run_stage greenhouse_discover_boards.py gh_discover 25
echo "discover rc=$? $(date -u '+%F %T')" >> gh_pipeline.status
run_stage greenhouse_probe_boards.py gh_probe 25
echo "probe rc=$? $(date -u '+%F %T')" >> gh_pipeline.status
run_stage greenhouse_fetch_jobs.py gh_fetch 25
echo "fetch rc=$? $(date -u '+%F %T')" >> gh_pipeline.status
run_stage greenhouse_build_sheet.py gh_build 3
echo "build rc=$? $(date -u '+%F %T')" >> gh_pipeline.status
echo "ALL DONE $(date -u '+%F %T')" >> gh_pipeline.status
