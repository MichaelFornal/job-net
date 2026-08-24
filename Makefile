# Everything here runs with no network, no API keys, and no corpus.
.PHONY: help demo test scan fetch-demo clean

PY ?= python3
FIXTURE := fixtures/MA_US_Remote_demo.csv

help:
	@echo "make demo       seed a synthetic database and serve the triage UI"
	@echo "make test       run every hermetic self-test"
	@echo "make scan       fail if anything personal reached the tree"
	@echo "make fetch-demo crawl one real public ATS board (needs network)"
	@echo "make clean      remove generated fixtures and databases"

$(FIXTURE):
	$(PY) triage/fixture.py --rows 3000 --out $@

# The loader owns the rows; each builder owns ONE score column and clears only
# its own scope.  They have to re-run after every load -- the CSV carries no
# score columns, so a load silently empties every builder-backed lane.
triage.db: $(FIXTURE)
	$(PY) triage_load.py --feed main --csv $(FIXTURE)
	$(PY) lanes_build.py
	$(PY) ai_agency_build.py
	$(PY) geo_lane.py
	$(PY) level.py
	$(PY) ai_startup_build.py   # last: carries the ANALYZE (see triage_chain.sh)

# http://127.0.0.1:8766 -- localhost only, by design.  The scout half needs a
# built corpus and a numpy index; without them those routes degrade to a
# message and the ~30 SQL lanes work regardless.
# The scout half needs a corpus and a numpy index.  Without them those routes
# degrade to a message and the SQL lanes still work -- but the weights panel is
# the most interesting screen in the app, so the demo builds a small one.
corpus.db: $(FIXTURE)
	$(PY) tools/demo_corpus.py

demo: triage.db corpus.db
	$(PY) triage/server.py

test:
	$(PY) triage/selftest.py
	$(PY) triage/scout_selftest.py
	$(PY) score_features.py --selftest
	$(PY) score_profile.py --verify
	$(PY) swath_vocab.py --selftest
	$(PY) -m compileall -q .

scan:
	./tools/scan_secrets.sh

# One unauthenticated board, ~100 rows, no key required.  Proves the crawler
# half without a multi-hour run.
fetch-demo:
	$(PY) tools/fetch_demo.py

clean:
	rm -rf fixtures triage.db triage.db-wal triage.db-shm corpus.db scout_index.npz
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
