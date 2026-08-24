#!/bin/bash
# ats_full_chain.sh — 2026-08-16 one-off: finish the ATS unfreeze end to end.
#
#   verify run (already running) -> collapse its appends -> baseline snapshot
#   -> full delisting sweep, 7 platforms, no budget cap
#   -> workable's first-crawl pass
#   -> rebuild every master + the monolith
#   -> rebuild the triage corpus and every lane
#
# norm_dedupe MUST run between the verify pass and the delisting pass.  The
# verify pass was append-only, so it left each live file inflated with
# duplicates; the delisting merge measures its shrink guard against the CURRENT
# live row count, and an inflated denominator would make a healthy sweep look
# like a 50% loss and trip a refusal on every platform.
cd "$(cd "$(dirname "$0")" && pwd)" || exit 1
set -x
RID="full-$(date -u +%Y%m%dT%H%M%SZ)"

# 1. wait out the verify pass
while kill -0 92361 2>/dev/null; do sleep 20; done
echo "=== VERIFY PASS DONE $(date -u +%T) ==="
grep -E "BUDGET REACHED|DONE ats_fetch|^  merge |retired " logs/ats_verify_2026-08-16.log | tail -6

# 2. collapse the verify pass's duplicate appends
python3 norm_dedupe.py 2>&1 | tail -20 || exit 1

# 3. preserve the pre-rebuild key set (runbook: always before a build tail)
python3 extract_baseline_keys.py 2>&1 | tail -6 || exit 1

# 4. the real thing: full sweep with delisting on, no wall-clock cap
echo "=== FULL DELISTING SWEEP $(date -u +%T) ==="
ATS_KEEP_ALL=1 \
ATS_PLATFORMS=ashby,smartrecruiters,recruitee,breezy,bamboohr,jazzhr,teamtailor \
ATS_RUN_ID="$RID" JZ_NO_DETAIL=1 ATS_DELIST=all ATS_BUDGET_MIN=45 \
  python3 ats_fetch.py 2>&1 | tail -40

# 5. workable — still on its first crawl, tier expiry does not apply
echo "=== WORKABLE PASS $(date -u +%T) ==="
ATS_KEEP_ALL=1 ATS_PLATFORMS=workable \
ATS_STATE=ats_all_workable_state.json \
ATS_LEDGER=ats_workable_yield_ledger.json \
ATS_RPS=workable:2 ATS_CONC=workable:4 ATS_WORKERS=8 \
ATS_RUN_ID="$RID" ATS_DELIST=all ATS_BUDGET_MIN=30 \
  python3 ats_fetch.py 2>&1 | tail -20

# 6. into main: every master + the monolith, no network
echo "=== BUILD $(date -u +%T) ==="
python3 run_all.py --build-only 2>&1 | tail -30 || exit 1

# 7. triage corpus + all lanes
echo "=== MA_REMOTE $(date -u +%T) ==="
python3 ma_remote_build.py 2>&1 | tail -12 || exit 1
echo "=== TRIAGE CHAIN $(date -u +%T) ==="
./triage_chain.sh 2>&1 | tail -45
echo "=== ATS FULL CHAIN COMPLETE $(date -u +%T) ==="
