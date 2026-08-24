#!/bin/bash
# lanes_chain.sh — everything between a finished run_all.py and a usable triage UI.
#
# On 2026-08-18 this was an ad-hoc inline heredoc and nothing was saved, so the
# steps had to be reconstructed from a `set -x` log.  Written down here instead.
#
# Deliberately does NOT wait on a run_all pid: gate this manually on run_all's
# final report (11 masters present, FAILED steps empty or icims-only).  Running
# it against a truncated master would load a broken corpus over a good one.
#
# Order is load-bearing at three points:
#   * startup_boards  BEFORE the lane builders — startup tiering reads its verdicts
#   * ma_remote_build BEFORE triage_chain      — the chain cannot load without its CSV
#   * ANALYZE         AFTER  the load          — no sqlite_stat1 makes the partial
#                                                indexes inert (lane menu 2.7s -> 12.8s)
cd "$(cd "$(dirname "$0")" && pwd)" || exit 1
set -x

echo "=== STARTUP_BOARDS $(date -u +%T) ==="
python3 startup_boards.py    || exit 1

echo "=== MA_REMOTE $(date -u +%T) ==="
python3 ma_remote_build.py   || exit 1

echo "=== TRIAGE CHAIN $(date -u +%T) ==="
./triage_chain.sh            || exit 1

echo "=== ANALYZE $(date -u +%T) ==="
sqlite3 triage.db 'ANALYZE;' || exit 1

echo "=== SERVER $(date -u +%T) ==="
pkill -f 'python3 triage/server.py' 2>/dev/null
sleep 2
nohup python3 triage/server.py > logs/triage_server_2026-08-19.out 2>&1 &
disown
sleep 4
curl -s -o /dev/null -w 'server HTTP %{http_code}\n' http://127.0.0.1:8766/

echo "=== LANES CHAIN COMPLETE $(date -u +%T) ==="
