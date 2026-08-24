#!/bin/bash
cd "$(cd "$(dirname "$0")" && pwd)" || exit 1
set -x
# Must precede the load: it re-joins the posted-date ledgers to contract_norm/*.jsonl
# and writes date_basis_index.json, which is how triage_load tells a real workday/icims
# date from our own crawl date. Skip it and those rows load as employer-published.
python3 date_basis_index.py             || exit 1
python3 triage_load.py --feed main      || exit 1
python3 lanes_build.py                  || exit 1
python3 ai_agency_build.py              || exit 1
python3 geo_lane.py                     || exit 1
python3 level.py                        || exit 1
# LAST of the main-feed builders, because it carries the ANALYZE.  Without
# sqlite_stat1 SQLite ignores the partial score indexes entirely and every lane
# COUNT(*) in the menu falls back to a full table scan -- measured at 352 ms for
# one lane's count before ANALYZE against 1 ms after.  Stats have to be gathered
# after the last score column is written, which is why the order matters.
python3 ai_startup_build.py             || exit 1
python3 contract_pool.py                || exit 1
python3 triage_load.py --feed contract  || exit 1
echo "TRIAGE CHAIN COMPLETE"
