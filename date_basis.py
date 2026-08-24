#!/usr/bin/env python3
"""date_basis.py — who actually produced the `posted` date on a row.

The bug this exists to stop: on 2026-08-18, 57% of the rows stamped
`posted = 2026-08-18` were not posted that day.  The date was the day some
INTERMEDIARY first saw the job — Getro's talent network, an aggregator's
crawler, or ours.  The underlying posting can be arbitrarily old, so a queue
sorted `posted DESC` puts stale and already-filled roles at the very top.

Measured over the 3,748 triage rows dated 2026-08-18:

    employer's own ATS published it              1,625   43%
    Getro created_at (network ingest)            1,001   27%
    aggregator ingest (himalayas/jobgether/...)    868   23%
    our crawler's first sight                      253    7%

Two places in this repo say so outright:

  * getro_build_sheet.py sets posted from `job.created_at`, and its own
    legend at :284 calls the column "First-seen posting date (UTC)".  Getro is
    87.7% of the master corpus.
  * workday_fetch.py falls back to TODAY_S when `postedOn` carries no
    signal.  Same shape in icims_fetch.py and atstail_fetch.posted_for().

The fetchers ALREADY know which dates are real: every *_posted_ledger.json
carries a `precision` per job.  `precision` is not a column in
all_jobs_master.csv, so it dies at the fetcher.  date_basis_index.py recovers
it for the mixed sources; this module holds the per-source part.

DO NOT REUSE startup_build.EXCL_SRC FOR THIS.
That set is imported as AGGREGATORS by triage/lanes.py and
triage_load.py, and it is wrong here twice over: it lists workday, oracle,
icims and adp (employer ATSes, not republishers) and it does NOT list getro —
the single largest mis-dated bucket.  Reusing it would mark 1,001 rows
trustworthy and 585 rows untrustworthy, both backwards.  EXCL_SRC denies a
startup TIER; it was never a statement about dates.
"""

# ---------------------------------------------------------------------------
# The four bases.  Ordered by how much the date can be trusted to mean "this
# role opened on this day".
# ---------------------------------------------------------------------------
EMPLOYER = "employer"      # the employer's own ATS/board published this date
NETWORK = "network"        # a talent network's ingest date (Getro created_at)
AGGREGATOR = "aggregator"  # a republisher's listing date
FIRST_SEEN = "first_seen"  # the day OUR crawler first saw it; not a real date

# Everything except EMPLOYER means "this is not when the employer posted it".
WEAK = frozenset((NETWORK, AGGREGATOR, FIRST_SEEN))

# Human-readable pill text for the triage card.
LABEL = {
    EMPLOYER: "",                    # the normal case needs no pill
    NETWORK: "network ingest date",
    AGGREGATOR: "aggregator listing date",
    FIRST_SEEN: "first-seen date",
}

# ---------------------------------------------------------------------------
# Sources whose `posted` comes from the employer's own posting record.  A blank
# date here means the ATS published none — which is honest, and the
# `(posted = '') ASC` guard in lanes.posted_terms() already handles it.
# ---------------------------------------------------------------------------
_EMPLOYER_SRC = {
    # verified: real ISO/epoch date straight off the employer's ATS
    "greenhouse", "lever", "ashby", "smartrecruiters", "breezy", "workable",
    "recruitee", "oracle", "adp", "jazzhr", "teamtailor", "bullhorn",
    "eightfold",
    # verified via their ledgers' `precision` — a real upstream field, frozen
    # only so it cannot drift forward on a re-run
    "paycom",   # datePosted
    "gem",      # firstPublished
    "comeet",   # time_updated
    "notion",   # created_time on the company's own Notion board
    "dover",    # created
    "yc",       # relative text ("about 2 months"), coarse but employer-derived
    # bamboohr leaves posted BLANK unless the per-job detail call is enabled
    # (ats_fetch.py) — undated, never fabricated.
    "bamboohr",
    "phenom",   # postedDate off the employer's own Phenom career site
    "usajobs",  # positionOpenDate / PublicationStartDate, published by the agency
    # 2026-08-20 additions.  All three read a real upstream field and keep NO
    # ledger, so there is nothing to fall back to and nothing to demote:
    "ukg",          # PostedDate off the UKG Pro board (496 distinct dates)
    "neogov",       # PostingDate, published by the agency (436 distinct)
    "cornerstone",  # postingEffectiveDate (689 distinct)
}

# ---------------------------------------------------------------------------
# Talent networks.  Not republishers in the aggregator sense — these are real
# curated boards — but the date is when the job entered the NETWORK, not when
# the employer opened it.  A company joining a Getro network gets every one of
# its already-open roles stamped with the day it joined.
# ---------------------------------------------------------------------------
_NETWORK_SRC = {"getro"}

# ---------------------------------------------------------------------------
# Republishers and job boards: the date is when THAT platform listed the job.
# Most are already is_agg=1 via EXCL_SRC and therefore already sort last; they
# are enumerated anyway so the basis pill is right and so `--audit` can tell a
# deliberate classification from an unseen source.
# ---------------------------------------------------------------------------
_AGGREGATOR_SRC = {
    "himalayas", "jobgether", "careerjet", "torre", "remoteleaf", "themuse",
    "adzuna", "jobicy", "remoteio", "hubstafftalent", "powertofly",
    "braintrust", "arbeitsagentur", "web3career", "cryptojobslist",
    "cryptocurrencyjobs", "remoteok", "weworkremotely", "remotive",
    "workingnomads", "justremote", "dynamitejobs", "jobspresso", "nofluffjobs",
    "getonbrd", "arbeitnow", "pythonorg", "larajobs", "vuejobs", "dribbble",
    "mindtheproduct", "elixirjobs", "authenticjobs", "landingjobs",
}

# ---------------------------------------------------------------------------
# Sources with NO upstream date at all — the ledger date IS the crawl date.
# rippling_build_sheet.py and personio_build_sheet.py both say so in as
# many words ("first-seen date ... NOT blank").
# ---------------------------------------------------------------------------
_FIRST_SEEN_SRC = {"rippling", "personio", "jobvite"}

# ---------------------------------------------------------------------------
# Mixed sources: a real date when the board publishes one, our crawl date when
# it does not.  Per-ROW resolution only — date_basis_index.py rebuilds the set
# of job ids that fell back, straight out of the ledgers' `precision` field.
#
#   workday  workday_fetch.py   ledger, precision relative|30plus|first_seen
#   icims    icims_fetch.py         ledger, precision portal|api|first_seen
#   the rest atstail_fetch.posted_for() real date wins, else the ledger; a job
#            is only WRITTEN to that ledger when the real date was missing, so
#            presence alone means the date was fabricated.
# ---------------------------------------------------------------------------
MIXED_SRC = {"workday", "icims", "pinpoint", "keka", "join", "zohorecruit",
             "taleo", "wttj",
             # 2026-08-20.  Each keeps a *_posted_ledger.json whose `precision`
             # says per row whether the date was published or invented:
             #   manatal  first_seen 850 / date_posted 89   (90% fabricated)
             #   radancy  rss_feed 30,901 / list_date 10,708 /
             #            first_seen 28,491                 (41% fabricated)
             #   dayforce first_seen when the portal publishes none
             # Without these here they would read as clean employer dates.
             "manatal", "radancy", "dayforce"}

# A source nobody has classified is assumed to be a republisher.  This fails in
# the safe direction: an unclassified source gets demoted rather than promoted
# into the fresh cuts, and the basis pill makes it visible so it gets fixed.
DEFAULT = AGGREGATOR

_MAP = {}
for _s in _EMPLOYER_SRC:
    _MAP[_s] = EMPLOYER
for _s in _NETWORK_SRC:
    _MAP[_s] = NETWORK
for _s in _AGGREGATOR_SRC:
    _MAP[_s] = AGGREGATOR
for _s in _FIRST_SEEN_SRC:
    _MAP[_s] = FIRST_SEEN

SOURCE_BASIS = dict(_MAP)


def basis_of(source, posted="", fabricated=False):
    """-> one of EMPLOYER / NETWORK / AGGREGATOR / FIRST_SEEN.

    `fabricated` is the per-row answer for a MIXED_SRC source, from
    date_basis_index: True means the ledger shows this job id fell back to a
    crawl date.  It is ignored for every other source, whose basis is a
    property of the source itself.

    A blank `posted` is left EMPLOYER-clean on purpose: blank is not a wrong
    date, it is the absence of one, and lanes.posted_terms() already sorts
    those out separately.  Marking blanks weak would double-penalise them.
    """
    src = (source or "").strip().lower()
    if src in MIXED_SRC:
        return FIRST_SEEN if fabricated else EMPLOYER
    return _MAP.get(src, DEFAULT)


def is_weak(basis):
    """1 when the date is not the employer's, for the int column lanes sort on."""
    return 1 if basis in WEAK else 0


def audit(sources):
    """-> sorted list of source names with no deliberate classification.

    Drift check: a new fetcher lands, its source falls to DEFAULT, and nothing
    complains.  run_all can call this and print what it finds.
    """
    known = set(_MAP) | MIXED_SRC
    return sorted({(s or "").strip().lower() for s in sources} - known - {""})


if __name__ == "__main__":
    import os
    import sqlite3
    import sys

    db = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "triage.db")
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    srcs = [r[0] for r in con.execute("SELECT DISTINCT source FROM jobs")]
    missing = audit(srcs)
    print(f"{len(srcs)} sources in {os.path.basename(db)}; "
          f"{len(srcs) - len(missing)} classified")
    counts = dict(con.execute(
        "SELECT source, COUNT(*) FROM jobs GROUP BY source"))
    for b in (EMPLOYER, NETWORK, AGGREGATOR, FIRST_SEEN):
        n = sum(counts.get(s, 0) for s in srcs
                if s and basis_of(s) == b and s not in MIXED_SRC)
        print(f"  {b:11s} {n:>8,} rows")
    print(f"  {'mixed':11s} {sum(counts.get(s, 0) for s in MIXED_SRC):>8,} rows"
          f"  (per-row, via date_basis_index.py)")
    if missing:
        print(f"\nUNCLASSIFIED (falling back to {DEFAULT}):")
        for s in missing:
            print(f"  {s:24s} {counts.get(s, 0):>7,} rows")
