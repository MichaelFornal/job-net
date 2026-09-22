#!/bin/bash
cd /Users/michaelfornal/Documents/jb || exit 1
set -x
# Must precede the load: it re-joins the posted-date ledgers to contract_norm/*.jsonl
# and writes date_basis_index.json, which is how triage_load tells a real workday/icims
# date from our own crawl date. Skip it and those rows load as employer-published.
python3 date_basis_index.py             || exit 1
python3 triage_load.py --feed main      || exit 1
# --days 90 and --per-company 8 are now swath_build.DAYS / PER_COMPANY too, so
# both flags are REDUNDANT.  They stay spelled out because these are the two
# numbers that decide how big every tag lane is, and the 2026-08-27 audit found
# both of them costing rows while living somewhere nobody was looking.
#
# --days 90, not the built-in 21.  Measured 2026-08-25 with triage_gap.py: at
# 21 days every one of the twelve lanes is window-starved between 2.3x and 4.7x
# (picks 1,629 -> 5,099 titles, aiexpert 159 -> 747), because the candidate pool
# itself goes 35,619 -> 89,003 rows.  90d is the knee: it reaches 92% of the
# all-dates supply, and the rows it adds sort behind fresher ones anyway --
# lanes.ORDER_TERMS puts `date_weak ASC, posted DESC` after the score, so an
# older or intermediary-dated row can never outrank a fresh employer-dated one.
# This used to be set ONLY here, which is how picks_build kept rebuilding the
# 21-day version over the top of it whenever it was run by hand.
#
# --per-company 8, not 3.  The 3 was measured (uncapped, Prolific alone is 52%
# of the F3 pool) but it was sized for small employers and it hid the large
# local ones: MGB Health Plan has 1,499 live rows, UMass Memorial 1,089, BMC
# 1,041, and a cap of 3 is a 99.8% cut on exactly the employers the MA and
# level lanes exist to surface.
python3 lanes_build.py --days 90 --per-company 8  || exit 1
# THE ROSTER SFL'S THIRD DOOR NEEDS.  miami_roster.py writes miami_orgs.csv,
# which miami_orgs.py:82 turns into a silent {} when it is missing -- so the
# door collapses with no message and sfl just gets smaller.  Its only other
# caller was miami_chain.sh, which nothing invokes, so the file stayed current
# purely by hand.  NON-FATAL on purpose: a stale roster costs one sfl door, a
# hard exit here costs every lane below.
python3 miami_roster.py || echo "WARN: miami_roster failed — sfl's Miami door falls back to the previous roster"
python3 sfl_build.py                    || exit 1
python3 ai_startup_build.py             || exit 1
python3 ai_agency_build.py              || exit 1
python3 local_build.py                  || exit 1
python3 level_build.py                  || exit 1
python3 ff_build.py                     || exit 1
# The wide automation lane.  Runs BEFORE legalai_build so that one stays last
# and keeps the ANALYZE; this builder deliberately carries none.
python3 autoeng_build.py                || exit 1
# Ranks by cost-to-apply rather than by fit, so it wants the same candidate set
# as the lanes above and no output of theirs.  Placed here for the same reason
# autoeng_build is: legalai_build stays last and keeps the ANALYZE.
python3 fastapply_build.py              || exit 1
# The AI-lab / accelerator / fund-network census.  Reads no output of the
# builders above and none of them reads its column, so it sits here for the
# same reason the two before it do: legalai_build stays last and keeps the
# ANALYZE.  Its employer universe is ai_lab_orgs.py, a hand-checked list.
python3 ai_lab_build.py                 || exit 1
# The low-competition lane.  Reads MA_US_Remote_<date>.csv -- the same file
# triage_load just loaded -- for n_sources / board_count / source_type / sources /
# delisted_on, which triage_load.FIELDS drops, and joins on job_key (100.00% hit,
# measured 2026-09-04).  Its denominators come from the UNGATED master, never from
# corpus.db, whose three gates make a 400-req employer read as a 3-req one.
#
# Reads no output of the builders above and none of them reads lowcomp_score, so
# it sits here for the same reason autoeng, fastapply and ai_lab do: legalai_build
# stays last and keeps the ANALYZE.  This builder deliberately carries none.
python3 lowcomp_build.py                || exit 1
# `legalai` reads the 3.6M master, not triage.db, so it needs its two harvest
# passes present.  They are NOT regenerated here: a full master scan is ~8 min
# and the vendor universe changes far more slowly than the feed does.  Re-run
# legalai_harvest.py / legalai_harvest_emp.py after a master rebuild.
#
# Last main-feed builder, so it carries the ANALYZE that ff_build.py used to.
# Without sqlite_stat1 the partial score indexes are inert and every lane
# COUNT(*) in the menu falls back to a full scan.
# MOVED HERE FROM discover_full_chain.sh (2026-09-07).  legalai_build.py:429
# sys.exit()s without these two CSVs and says nothing when they are merely
# stale, so they were given their own stage in the outer chain.  That put four
# files between a producer and its only consumer; here they cannot drift apart.
# Both are full-master scans, so they must still follow all_jobs_build -- which
# they do, since lanes_chain.sh runs only after the chain's master gate passes.
# NON-FATAL, exactly as the outer stage was: a failed harvest leaves legalai a
# day old, it does not justify killing contract_pool and the feed load below.
python3 legalai_harvest.py     || echo "WARN: legalai_harvest failed — legalai uses the previous harvest"
python3 legalai_harvest_emp.py || echo "WARN: legalai_harvest_emp failed — legalai uses the previous harvest"
python3 legalai_build.py --no-xlsx      || exit 1
python3 contract_pool.py                || exit 1
python3 triage_load.py --feed contract  || exit 1
echo "TRIAGE CHAIN COMPLETE"
