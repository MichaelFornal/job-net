#!/bin/bash
# refresh_ashby_portals.sh — one-shot driver for this refresh:
#   1. Ashby: true re-fetch (rotate JSONL + state) -> ashby_jobs_master.csv
#   2. Portals: run_all.py --only portals (rotates + refetches Bullhorn OSCP,
#      Phenom, Ashby/SmartRecruiters/Recruitee/Breezy/BambooHR, boards), whose
#      build tail rebuilds every master and all_jobs_master.csv
# Sequential on purpose: run_all's tail reaches all_jobs_build while the Ashby
# build would still be writing its master CSV, and a half-written CSV merges
# silently. Sequencing costs ~15 min and removes the race.
cd "$(cd "$(dirname "$0")" && pwd)" || exit 1

echo "=== ashby refresh $(date -u '+%F %T') ==="
./ash_run_all.sh --refresh
echo "=== ashby rc=$? $(date -u '+%F %T') ==="

echo "=== portals refresh $(date -u '+%F %T') ==="
python3 run_all.py --only portals
echo "=== portals rc=$? $(date -u '+%F %T') ==="
echo "=== ALL DONE $(date -u '+%F %T') ==="
