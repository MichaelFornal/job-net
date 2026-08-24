#!/bin/bash
# run_monolith_guarded.sh — run all_jobs_build.py with a disk guard.
#
# The 2026-07-29 expansion roughly doubled the corpus (~3.8M raw rows in,
# ~2.4M unique expected).  The largest merge this machine has completed is
# 1.77M raw, and the recorded rule of thumb is ~1.7M rows per 16GB with string
# interning — so this run is expected to lean on swap.  Swap comes out of the
# SAME free space the outputs need, and an out-of-disk during the CSV write
# would truncate all_jobs_master.csv rather than fail cleanly.  So: watch free
# space, and kill the build while there is still room to recover.
#
# Usage: ./run_monolith_guarded.sh [extra all_jobs_build.py args...]
cd "$(cd "$(dirname "$0")" && pwd)" || exit 1
MIN_FREE_GB=${MIN_FREE_GB:-6}
LOG=logs/all_jobs_build_final.log

python3 all_jobs_build.py "$@" > "$LOG" 2>&1 &
PID=$!
echo "all_jobs_build pid $PID (guard: kill if free disk < ${MIN_FREE_GB} GiB)"

PEAK_RSS=0
while kill -0 $PID 2>/dev/null; do
  FREE=$(df -k . | tail -1 | awk '{printf "%d", $4/1048576}')
  RSS=$(ps -o rss= -p $PID 2>/dev/null | tr -d ' ')
  [ -n "$RSS" ] && [ "$RSS" -gt "$PEAK_RSS" ] && PEAK_RSS=$RSS
  if [ "$FREE" -lt "$MIN_FREE_GB" ]; then
    echo "GUARD TRIPPED: only ${FREE} GiB free — killing build to protect the disk"
    kill -9 $PID 2>/dev/null
    echo "GUARD KILLED BUILD at $(date -u '+%F %T')" >> "$LOG"
    exit 2
  fi
  sleep 20
done
wait $PID
RC=$?
echo "all_jobs_build exit $RC | peak RSS $(echo "$PEAK_RSS" | awk '{printf "%.1f", $1/1048576}') GiB | free disk $(df -k . | tail -1 | awk '{printf "%d", $4/1048576}') GiB"
tail -25 "$LOG"
exit $RC
