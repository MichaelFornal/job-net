#!/bin/bash
# full_refresh_chain.sh — the 2026-08-20 end-to-end refresh, in the one order
# that is correct.
#
#   WAIT_PID=<pid> nohup ./full_refresh_chain.sh > logs/full_chain.out 2>&1 &
#
# WAIT_PID is optional: if set, the chain blocks until that process exits
# before starting.  It exists so this can be queued behind a running Wayback
# sweep.  Wait on a real PID, never on a `pgrep -f` pattern — the pattern
# matches this script's own shell and the monitor exits instantly.
#
# WHY THIS ORDER
#   1. workday_discover_boards  reads the slug sweep's cached CDX rows, probes
#      the new (tenant, pod, site) triples through the CXS API and merges the
#      live ones into tenants/workday.csv.  It must precede run_all or the new
#      Workday tenants are not in the board list the fetch reads.
#   2. extract_baseline_keys    snapshots the CURRENT master's job_key column.
#      Its own header says RUN THIS BEFORE ANY REBUILD — all_jobs_build opens
#      all_jobs_master.csv for writing, so afterwards the old keys are gone and
#      the new-jobs diff has nothing to diff against.
#   3. run_all                  everything else.  Since 2026-08-20 this also
#      covers greenhouse+ashby, usajobs, smallremote and the six new families,
#      so there is no longer a refresh_gh_ashby.sh step to remember.
#
# Every stage is resumable and none is fatal to the next: a partial sweep still
# yields tenants, and a budget-capped fetch queues the rest for tomorrow.
cd /Users/michaelfornal/Documents/jb || exit 1
mkdir -p logs
STATUS=logs/full_chain.status
say() { echo "[$(date -u '+%F %T')] $*" | tee -a "$STATUS"; }

if [ -n "$WAIT_PID" ]; then
  say "waiting for pid $WAIT_PID to finish"
  while kill -0 "$WAIT_PID" 2>/dev/null; do sleep 60; done
  say "pid $WAIT_PID gone"
fi

stage() { # <tag> <cmd...>
  local TAG=$1; shift
  say "START $TAG"
  "$@" >> "logs/chain_$TAG.out" 2>&1
  local RC=$?
  say "END   $TAG rc=$RC"
  return $RC
}

say "=== full refresh chain start ==="
stage workday_discover python3 workday_discover_boards.py
stage baseline         python3 extract_baseline_keys.py
stage run_all          python3 run_all.py
say "=== full refresh chain done (run_all rc above) ==="
