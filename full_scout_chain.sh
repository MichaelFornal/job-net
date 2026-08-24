#!/bin/bash
# The scout chain: master -> gated corpus -> corpus.db -> the in-memory index.
#
# Run AFTER the existing triage chain, never instead of it.  It touches nothing
# triage.db owns: the thirty existing lanes read `jobs` and this writes only
# corpus.db and scout_index.npz.  triage/lane_snapshot.py is the proof of that
# and is run at both ends.
#
#   ./full_scout_chain.sh              # rebuild everything
#   ./full_scout_chain.sh --index-only # weights/features changed, corpus did not
set -euo pipefail
cd "$(dirname "$0")"

BASE=".lane_baseline.json"
[ -f "$BASE" ] || python3 triage/lane_snapshot.py --write "$BASE"

if [ "${1:-}" != "--index-only" ]; then
  echo "== 1/3 corpus_build =="
  python3 corpus_build.py
  echo "== 2/3 corpus_load =="
  python3 corpus_load.py
fi

echo "== 3/3 scout_index =="
python3 scout_index.py

echo "== verify =="
python3 score_features.py --selftest
python3 score_profile.py --verify
python3 triage/lane_snapshot.py --check "$BASE"
echo
echo "done -- restart triage/server.py to pick up the new index"
