#!/bin/bash
# CDX crawlers are STRICTLY serial: archive.org is one rate-limited host and
# running two at once just manufactures the ConnectionErrors we already saw.
cd /Users/michaelfornal/Documents/jb || exit 1
echo "[$(date -u +%H:%M:%S)] === GREENHOUSE (discover -> probe -> fetch -> build) ==="
./gh_run_all.sh; echo "[$(date -u +%H:%M:%S)] greenhouse exit=$?"
echo "[$(date -u +%H:%M:%S)] === LEVER ==="
python3 lever_discover_boards.py && python3 lever_probe_boards.py \
  && python3 lever_fetch_jobs.py && python3 lever_build_sheet.py
echo "[$(date -u +%H:%M:%S)] lever exit=$?"
echo "[$(date -u +%H:%M:%S)] === ASHBY ==="
./ash_run_all.sh --refresh; echo "[$(date -u +%H:%M:%S)] ashby exit=$?"
echo "[$(date -u +%H:%M:%S)] DISCOVERY CHAIN COMPLETE"
