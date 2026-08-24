#!/usr/bin/env python3
"""Lane definitions — the single place that decides what you get shown, and in
what order.

Imported by both triage_load.py (to report counts after a load) and
triage/server.py (to serve the queue), so the menu and the loader can never
disagree about what a lane means.

A lane is a SQL predicate over `jobs`, nothing more.  Every lane is implicitly
also filtered to "live and not yet decided" by `queue_sql()`, so the number
next to a lane IS the work remaining in it — burn it to zero and it stays at
zero until genuinely new rows arrive.

Ordering inside a lane is fixed for every lane:

    real employer boards before aggregators   (is_agg)
    proven-US rows before inferred-US ones    (basis_weak)
    strongest AI signal first                 (rank_tier: 1..5, then 9 = none)
    newest first                              (posted DESC)

...unless a posted-date sort is asked for, in which case posted leads and the
lane's own ranking above becomes the tiebreak.  See posted_terms(): the sort is
a prefix on the ORDER BY, never a replacement for it, so a lane that ranks on a
score keeps that score deciding ties without needing a case of its own here.

`basis_weak` is 1 when ma_remote_build could only INFER that a remote row is
US-based (blank country, but a US-posting company or a USD salary) rather than
read it off the posting.  Those rows are real finds — the strict rule was
dropping ~24k of them, AI Forward-Deployed-Engineer roles among them — but the
evidence behind them is weaker, so they sit behind the proven ones in every
lane instead of being mixed in or thrown away.

`is_agg`, `basis_weak` and `rank_tier` are all precomputed columns on `jobs` —
see triage_load.py — because sorting on a 40-element IN-list, on a LIKE, or on
CAST(ai_tier) across 178k rows is the difference between an instant queue and a
stuttering one.
"""
import datetime as dt
import os
import sys

# Self-sufficient path bootstrap: this module
# lives in triage/ but imports from the jb/ root, and it is imported by things
# that run from both places.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# The aggregator list is startup_build's, not a second copy that drifts from
# it.  These sources republish other boards' postings: they are still real jobs,
# they just go behind the employer-direct rows in every queue.
try:
    from startup_build import EXCL_SRC as AGGREGATORS
except ImportError:                                   # pragma: no cover
    AGGREGATORS = set()

# 21 days was a display window that got read as supply. On 2026-08-09 the AI lane showed
# zero undecided band-1/2 rows and that was recorded as "the AI lane is exhausted" -- but the
# identical predicate returned 903 rows at 42 days and 2,752 at all dates. Nothing had been
# consumed; the window had moved. Tier-1/2 inflow is running ~42/weekday against ~21
# applications/day, so the lane produces more than it can be drained by. 42 keeps a posting
# in view for six weeks, which is roughly how long employer-direct reqs stay fillable.
RECENT_DAYS = 42

# The `fresh` cut lane.  Three days, not the 21 the lane labels call "recent":
# this lane exists to answer "what went up since I last looked", and at 21 days
# it would just be a slower copy of `rest`.
FRESH_DAYS = 3

# The floor for the `pay` cut lane, in USD per year.  Only ~37% of the corpus
# publishes a salary at all, so this doubles as a quality filter: a board that
# posts a real number is a board that has decided what the job is worth.
PAY_FLOOR = 150000

# ...and the ceiling, which exists to protect the ORDER BY rather than to filter
# out opportunities.  `pay` sorts on salary_min DESC, so the first cards you see
# are whatever the corpus's worst parse produced: a "Senior Strategic Enterprise
# SDR" at $475,000,000, a corrosion supervisor at $96,400,000 (its max says
# $120,500 — the min lost its decimal point), a $999,999 posting literally titled
# "Create Your Own".  Eleven such rows in 13k, all at the top, on the one lane
# whose whole proposition is "read it from the top".
#
# 900k is measured, not picked: the highest CREDIBLE rows in the corpus are an
# Anthropic research engineer at $850k, Netflix and HubSpot infra leadership in
# the $700s, and remote overnight radiology at $625–800k — that last group is
# why this is not set anywhere near $500k, which would have read as a safe round
# number while quietly cutting the best-paid real jobs in the file.
PAY_CEIL = 900000


def recent_cutoff(days=RECENT_DAYS, today=None):
    """ISO date `days` back — the '· last Nwks' in the lane labels."""
    today = today or dt.date.today()
    return (today - dt.timedelta(days=days)).isoformat()


# (key, label, predicate-SQL, n_params).  `?` binds the recent cutoff.
# `rest` is the complement of everything above it and is built by lanes().
#
# These describe the MAIN corpus only.  lanes() scopes every one of them to
# feed='main', including `rest`, so a second feed cannot leak into a lane whose
# label promises something else — a contract gig is not an "AI 1-2" row and
# "Everything else" must not become a dumping ground the moment a feed is
# added.  Each extra feed brings its own lane instead.
_DEFS = [
    ("ai12", f"AI 1-2 · last {RECENT_DAYS // 7}wks",
     "rank_tier <= 2 AND posted >= ?", 1),
    ("ai_all", "AI any · all dates",
     "ai_band <> ''", 0),
    ("ma", f"MA local · last {RECENT_DAYS // 7}wks",
     "bucket = 'MA local' AND posted >= ?", 1),
    ("startup12", f"Startup 1-2 · last {RECENT_DAYS // 7}wks",
     "(startup_tier LIKE '1%' OR startup_tier LIKE '2%') AND posted >= ?", 1),
    # The metro lane.  All dates and no tier floor on purpose — it is the full
    # sweep of South Florida, and geo_score is what makes that usable rather
    # than a cutoff.  `bucket = 'South FL'` is on-site/hybrid by construction
    # (ma_remote_build.geo_of runs the remote leg first), so this does not
    # restate what ai12/startup12 already cover from the remote side.
    ("sfl", "South FL · AI & startup · all dates",
     "bucket = 'South FL' AND (ai_band <> '' OR startup_tier <> '')", 0),
]

# Held as bare terms, not as finished clauses, so posted_terms() can be spliced
# in front of them without any string surgery on an assembled ORDER BY.  The
# ORDER_BY constants below stay for the callers (and the self-test) that want
# the whole clause.
# `date_weak ASC` sits immediately in front of every `posted` term in this file.
# It is 1 when `posted` is not the employer's date but some intermediary's
# ingest date (triage_load.py writes it from date_basis.py).  Without it the
# rows with the LEAST trustworthy dates sort first: 57% of the rows stamped
# 2026-08-18 were ingested that day, not posted that day, and they were burying
# genuinely-fresh employer postings and opening as expired when clicked.
# It goes AFTER the fit/score terms and BEFORE `posted`, so it only ever breaks
# a date tie -- no lane's ranking changes, only which of two same-day rows wins.
ORDER_TERMS = ("is_agg ASC, basis_weak ASC, rank_tier ASC, "
               "date_weak ASC, posted DESC, job_key ASC")

# The contract pool arrives pre-ranked by contract_pool.py, so its lanes sort
# on those scores instead of on AI strength: rank_tier is 9 for nearly every
# contract row, which would collapse the order to posted-date alone and throw
# away the whole point of scoring it.
CONTRACT_TERMS = "score DESC, date_weak ASC, posted DESC, job_key ASC"
MONEY_TERMS = "money_score DESC, date_weak ASC, posted DESC, job_key ASC"

# `picks` is scored by vocab.py, so it sorts on that score for the same
# reason the contract lanes do: its rows are overwhelmingly rank_tier 9 — the AI
# classifier's verdict on a "Solutions Engineer" or a "Capital Markets Lead" —
# and the default ordering would collapse them all to posted-date, which is the
# ordering that buried them in `rest` in the first place.
PICKS_TERMS = "score DESC, date_weak ASC, posted DESC, job_key ASC"

# `sfl` is scored by the per-metro lane builder into geo_score, a column of its own rather
# than `score` — see triage_load.py's DDL for why.  `is_agg` sits second here
# and does real work: unlike picks, this lane keeps aggregator rows (dropping
# careerjet would hide real Miami jobs from a lane that promised all of them),
# and geo_score ties are common enough that employer-direct rows need a term to
# put them first.
# `geo_score` is this lane's own ranking column, written by a per-metro
# builder.  It is NOT populated by anything shipped here -- the commute-ring
# builder (geo_lane.py) owns `local_score` and deliberately does not touch
# this one, because two geographic builders sharing a column means the
# second run blanks the first lane.  Rows tie at 0 until such a builder
# runs, and the terms after it carry the order.
SFL_TERMS = "geo_score DESC, is_agg ASC, date_weak ASC, posted DESC, job_key ASC"

# `aistartup` is scored by ai_startup_build.py into startup_score, its own
# column for the same reason geo_score is one: lanes_build.py owns score/
# score_why on the main feed and would overwrite this ranking for any row that
# also matched a swath, silently and depending on which builder ran last.
#
# `basis_weak` is here and absent from SFL_TERMS because it can only ever fire
# on a remote row — it marks a row whose US basis was INFERRED rather than read
# off the posting, and every row in this lane is remote by construction, where
# every row in `sfl` is on-site South FL and has basis_weak = 0.  Without it the
# inferred rows interleave with the proven ones, which is the one distinction
# this lane's geography claim rests on.
AISTARTUP_TERMS = ("startup_score DESC, is_agg ASC, basis_weak ASC, "
                   "date_weak ASC, posted DESC, job_key ASC")

# `aiagency` is scored by ai_agency_build.py into desk_score, its own column for
# the same reason the two above have one.
#
# NOTE WHAT IS MISSING: `is_agg`.  Every other lane leads with `is_agg ASC`, and
# for this population that term is not a mild preference — it is the mechanism
# that hides the lane.  65 of the 86 AI rows a staffing desk posted are is_agg=1,
# because agency reqs reach this corpus as jobgether and careerjet spillover
# rather than through a fetcher of their own; sorted employer-direct-first they
# sit below every non-agency row and nobody scrolls to them.  That is precisely
# how they went unnoticed until now.
#
# The usual argument for the term — a republished posting cannot be applied to at
# source — is also weakest here.  The "source" of an agency req IS the agency,
# and its JobDiva portal is where the apply_url already points.
AIAGENCY_TERMS = "desk_score DESC, basis_weak ASC, date_weak ASC, posted DESC, job_key ASC"

# `pay` sorts on the number that got it into the lane, through the same CAST the
# predicate uses.  salary_min is TEXT ('140000', '150000.0', and '80' where the
# period is Hour), so a lexical sort is wrong in general — though not, as it
# happens, wrong here: every value between PAY_FLOOR and PAY_CEIL has a 6-digit
# integer part, and equal-width numerals compare the same either way.  The CAST
# is what keeps that an accident of the bounds rather than something the ordering
# depends on; drop the floor to five digits and a string sort puts $95k on top.
#
# is_agg rides second: a republished posting and a direct one at the same
# advertised floor are not worth the same, because only one of them can be
# applied to at source.
PAY_TERMS = ("CAST(salary_min AS REAL) DESC, is_agg ASC, basis_weak ASC, "
             "date_weak ASC, posted DESC, job_key ASC")

# `local` is scored by geo_lane.py into local_score, its own column for the
# same reason geo_score is one -- and specifically NOT geo_score, which is
# the per-metro lane builder's.  That builder clears geo_score scoped to bucket='South FL'
# (the per-metro lane builder), so two builders sharing the column would each leave
# the other's rows uncleared and stale.
#
# `basis_weak` is absent for the reason it is absent from SFL_TERMS: it marks a
# row whose US basis was inferred rather than read off the posting, and a job
# that names a specific town has its basis on the posting by construction.
#
# `is_agg` is not a mild preference here.  Half this lane (3,001 of 5,719 on the
# 2026-08-18 corpus) is aggregator republish, so without the term the direct
# postings -- the ones with an employer to call -- are shuffled in among them.
LOCAL_TERMS = "local_score DESC, is_agg ASC, date_weak ASC, posted DESC, job_key ASC"

# `level` is scored by level.py into level_score, its own column for the
# same reason local_score is one: lanes_build.py owns score/score_why on the main
# feed, and this lane overlaps the swaths heavily by construction -- it is the
# same families one rung down -- so sharing that pair would hand the ranking to
# whichever builder ran last.
#
# `basis_weak` IS carried here, unlike LOCAL_TERMS.  That lane's rows name a
# Massachusetts town, so their US basis is on the posting by construction; this
# one admits the whole remote surface, where 15.9k rows carry an inferred basis
# and are worth less than a row that said so.
LEVEL_TERMS = ("level_score DESC, is_agg ASC, basis_weak ASC, date_weak ASC, "
               "posted DESC, job_key ASC")

ORDER_BY = "ORDER BY " + ORDER_TERMS
CONTRACT_ORDER_BY = "ORDER BY " + CONTRACT_TERMS
MONEY_ORDER_BY = "ORDER BY " + MONEY_TERMS
PICKS_ORDER_BY = "ORDER BY " + PICKS_TERMS

# The sort control on both pages.  'fit' is every lane's own ranking above,
# unchanged and the default; the other two put posted-date in front of it.
SORTS = ("fit", "new", "old")


def posted_terms(sort=None, p=""):
    """Leading ORDER BY terms for a posted-date sort — '' for 'fit'.

    `p` is the table alias the caller needs ('j.' on the apply queue, nothing
    on the card queue).  Returned with its trailing comma so a caller can
    concatenate it straight onto the terms it already had.

    The `(posted = '') ASC` guard is what keeps the ~2.7k blank-posted rows out
    of the top of the 'old' list.  Blank means the source never published a
    date (Phenom, WeWorkRemotely, Braintrust — see all_jobs_build.py), NOT that
    the job is old, and '' sorts below every real date: under DESC those rows
    already fall to the bottom on their own, but under ASC they would head the
    whole list.  The term is on both so the invariant survives a flip rather
    than depending on which direction happens to be asked for.

    `date_weak ASC` leads for the same class of reason, and it leads in BOTH
    directions.  A date that is really an ingest date is not evidence of
    freshness under 'new' and not evidence of age under 'old' — it is not
    evidence about the posting at all, so it must not head either list.  This
    is the sort where the mis-dating actually hurt: 'newest first' was showing
    Getro/Himalayas ingest dates ahead of real same-day employer postings.
    """
    sort = sort or "fit"
    if sort not in SORTS:
        raise ValueError(f"no such sort: {sort}")
    if sort == "fit":
        return ""
    return (f"{p}date_weak ASC, ({p}posted = '') ASC, "
            f"{p}posted {'DESC' if sort == 'new' else 'ASC'}, ")

# Both contract lanes read ONE feed and are separated by `lane_tags`, not by a
# feed of their own.  A gig can be both a good fit and fast money; giving each
# lane its own feed would need a second job_key for the same job, and you would
# then judge it twice and could reach two different verdicts about it.  Tagged,
# it is one row: deciding it in either lane clears it from both.
#
# The same tag mechanism also carries lanes over the MAIN feed — `picks` is one.
# It is deliberately not a feed of its own: those rows already exist in the main
# corpus with their own job_keys, and a second feed would mint a second key for
# the same job, which is the two-verdicts problem described above arriving from
# the other direction.  Tagged, deciding a pick in `picks` clears it from `rest`.
#
# `picks` is rebuilt by vocab.py, NOT by a load.  triage_load.py upserts
# lane_tags from the CSV and MA_US_Remote_<date>.csv has no such column, so every
# `triage_load.py --feed main` blanks this lane; re-run vocab.py after one.
#
# (key, label, tag, feed, order_terms)
_FEED_LANES = [
    ("picks", "Picks · worth applying to", "picks", "main", PICKS_TERMS),
    ("contract", "Contract gigs · ranked", "contract", "contract",
     CONTRACT_TERMS),
    ("fastmoney", "Fast money · quick hire", "fastmoney", "contract",
     MONEY_TERMS),
    # The eleven F-swaths.  Same mechanism as `picks` and for the same reasons: one main
    # feed, separated by `lane_tags`, scored by lanes_build.py, sorted on that score
    # because these rows are overwhelmingly rank_tier 9 and the default ordering would
    # collapse them to posted-date — which is what buried them in `rest` to begin with.
    #
    # All twelve main-feed lanes are DISJOINT: lanes_build.py scores each row against all
    # of them and writes exactly one tag, so deciding a row in its swath clears it from
    # `rest` and it can never be shown twice.  Ordered here by measured pool size
    # (NICHE-MAP-2026 §1) so the biggest lanes sit nearest the number keys.
    ("presales", "F2 · Solutions Architect / Presales", "presales", "main", PICKS_TERMS),
    ("gtm", "F1 · Revenue Systems / GTM", "gtm", "main", PICKS_TERMS),
    ("analytics", "F5 · Analytics Engineer / BI", "analytics", "main", PICKS_TERMS),
    ("bizops", "F4 · BizOps / Chief of Staff", "bizops", "main", PICKS_TERMS),
    ("fintech", "F9 · Fintech Ops · pay/billing", "fintech", "main", PICKS_TERMS),
    ("automation", "F6 · Automation & Integration", "automation", "main", PICKS_TERMS),
    ("aienable", "F8 · AI Enablement", "aienable", "main", PICKS_TERMS),
    ("aiexpert", "F3 · AI Expert & Evaluation", "aiexpert", "main", PICKS_TERMS),
    ("ecom", "F7 · Ecommerce / DTC", "ecom", "main", PICKS_TERMS),
    ("trust", "F11 · Trust & Safety / Risk", "trust", "main", PICKS_TERMS),
    ("rwa", "F10 · Digital Assets / RWA", "rwa", "main", PICKS_TERMS),
]

# --- the cuts -----------------------------------------------------------------
# Predicate lanes like _DEFS, but sliced on an axis the corpus lanes don't have:
# _DEFS asks what KIND of job a row is, these ask when it landed, what it pays,
# and what it is called.  A row can sit in one of these and in a corpus lane and
# in `rest` all at once, exactly as a `picks` or swath row already does — one
# job_key, one verdict, so deciding it anywhere clears it everywhere.
#
# They are deliberately NOT appended to _DEFS, for two reasons:
#
#   1. `rest` is the complement of _DEFS, so putting them there would carve
#      13k rows out of "Everything else" on a `posted` boundary that moves every
#      day.  A lane whose membership changes overnight is fine; a COMPLEMENT
#      that changes overnight silently reshapes the lane you fall back to.
#   2. The digit keys index lanes() order and only the first ten get one (see
#      LANE_KEYS in index.html).  Four lanes inserted into _DEFS would push
#      `picks` off 7, `contract` off 8 and `fastmoney` off 9, and slide `rest`
#      from 6 to 0 — the four keys with the most decisions behind them after
#      `ai12`.  Emitted last, the cuts take the digits nobody had.
#
# Each is scoped to feed='main' by lanes(), like _DEFS: unscoped, contract rows
# leak into them and selftest's "no contract row reaches a main-feed lane" sweep
# is the thing that would tell you, one section after you stopped reading.
#
# (key, label, predicate-SQL, cutoff_days)  — cutoff_days binds the one `?`.
_CUT_DEFS = [
    # Aggregators stay in.  ORDER_TERMS leads with `is_agg ASC` so they sort to
    # the back on their own, and the same call sfl makes applies here: dropping
    # careerjet would hide real jobs from a lane that promised all of today's.
    # Blank `posted` rows fall out because '' sorts below every date — correct
    # here and nowhere else, since blank means the source never published one
    # and this is the one lane that is only about a known date.
    ("fresh", "Fresh · last 72h",
     "posted >= ?", FRESH_DAYS),
    # Hour/Month/Week periods are excluded rather than annualised: a converted
    # figure would be a number this repo invented, sitting in the same column as
    # ones the employer published.  So the lane means "advertises a real annual
    # floor at or above PAY_FLOOR", which is narrower than "pays well" and is
    # the only one of the two the data can actually support.
    ("pay", f"Pay · ${PAY_FLOOR // 1000}k+ disclosed",
     "salary_currency = 'USD' AND lower(salary_period) IN ('year', 'annual') "
     f"AND salary_min <> '' AND CAST(salary_min AS REAL) >= {PAY_FLOOR} "
     f"AND CAST(salary_min AS REAL) < {PAY_CEIL}", None),
    # The COMPLEMENT of the `ma` lane, not a superset of it — `ma` is MA local
    # inside the 21-day window, this is everything MA local outside it, so the
    # two never show the same card and no MA row is unreachable.  Rows age out
    # of `ma` into here on their own as the window slides.
    #
    # `posted = ''` lands here (blank < every date), and that is deliberate: 404
    # MA rows carry no date, `ma` cannot reach them by construction, and this is
    # the lane that promises the ones it can't.  Hence "undated" in the label.
    ("maold", "MA local · older / undated",
     "bucket = 'MA local' AND posted < ?", RECENT_DAYS),
    # A title lane, and the only one here — the largest coherent block of work
    # the eleven swaths leave untagged (~7k).  It has no entry in LANE_RESUME
    # because there is no F12 draft yet; resume_for() returns None and the card
    # omits its "Send:" line, the same as every corpus lane does today.
    ("csam", "Customer Success / AM",
     "(title LIKE '%Customer Success%' OR title LIKE '%Account Manager%' "
     "OR title LIKE '%Account Executive%' OR title LIKE '%Client Partner%')",
     None),
    # The second title lane, and the one the search was widened INTO on 2026-08-18.
    #
    # An earlier pass measured this pool at 712 rows / $118k median and declined it as
    # "below target" -- but the target was $110-140k, so $118k was never below it, and
    # under "any remote job ASAP" the decline is stale twice over. Re-measured across the
    # undecided live remote non-aggregator set it is ~709 rows at a mean disclosed floor
    # of $136,912, and only 116 of the 1,032-row pool had ever been decided at all.
    #
    # This is the widening that survived review. Down-levelling into CS-rep or data-entry
    # work did not: it needs material the profile does not have, aims at a real pool of
    # only 99 rows, and the presales and automation swaths already cover the same ground.
    #
    # Title-only, like csam, with no geography clause: the `l` chip in the UI already
    # filters MA / remote / South FL, and baking a location in here would make the lane
    # mean two things at once.
    ("support", "Support Eng / TAM / Implementation",
     "(title LIKE '%Support Engineer%' OR title LIKE '%Technical Account Manager%' "
     "OR title LIKE '%Solutions Engineer%' OR title LIKE '%Solutions Consultant%' "
     "OR title LIKE '%Implementation%' OR title LIKE '%Onboarding%' "
     "OR title LIKE '%Professional Services%' OR title LIKE '%Sales Engineer%' "
     "OR title LIKE '%Deployment Engineer%' OR title LIKE '%Customer Engineer%')",
     None),
    # US-remote AI/FDE roles at startups, from EVERY source.
    #
    # The predicate is the score and nothing else, which is what lets the lane
    # mean something no column on `jobs` can express.  `startup_tier` is written
    # only for the employer-direct ATS families — himalayas, jobgether, torre,
    # remoteio and wttj carry 1,375 live US-remote AI rows between them and not
    # one startup tier — so the SQL-only version of this lane,
    # `bucket='Remote' AND rank_tier<=2 AND startup_tier<>''`, silently means
    # "...that happen to be on a board with a fetcher".  ai_startup_build.py
    # resolves the tier through `company_norm` instead, so a startup's posting
    # is admitted wherever it was found, and writes the verdict here.
    #
    # It therefore belongs with the cuts rather than in _DEFS on the merits as
    # well as for the two reasons given above: it is a lane whose membership a
    # reader of _DEFS could not check, since the predicate names a builder's
    # output rather than a property of the job.
    #
    # `> 0` is exact, not a threshold — ai_startup_build refuses to write a
    # zero, so this reads "the builder put it here" and no row can be admitted
    # carrying no number and no reason.
    ("aistartup", "AI / FDE · US remote startups",
     "startup_score > 0", None),
    # AI reqs a staffing desk is trying to fill — client placements, NOT jobs at
    # the agency.  Here for the same reason `aistartup` is: the predicate names a
    # builder's output because no column on `jobs` can express it.  The decisive
    # signal is the apply_url's registrable domain being a staffing-only ATS
    # (JobDiva, Bullhorn, Ceipal, Crelate...), which is proof by construction —
    # those vendors have no non-agency customers — and which a company-name test
    # cannot reach: 387 main-feed rows are agency-posted while reading as an
    # ordinary employer (NTT DATA, ICONMA, Metasys Technologies).
    #
    # This lane also reverses a judgement the repo made elsewhere.
    # the outreach ranker excludes staffing shops on the grounds that there
    # is "no hiring manager to build a relationship with", and that is right about
    # the REQ and wrong about the DESK: Diverse Lynx carries 365 live reqs, 20 of
    # them AI. ai_agency_build.py scores desk depth for exactly that reason, so
    # the lane ranks agencies worth knowing rather than jobs worth applying to.
    #
    # `> 0` is exact, not a threshold — the builder refuses to write a zero.
    ("aiagency", "AI reqs · staffing desks", "desk_score > 0", None),
    # What is hiring within about 25 minutes of home.  Here for the third time
    # for the reason the two above are: the predicate names a builder's output,
    # because no column on `jobs` can express it.  `bucket` collapses every
    # in-state row to one value -- a suburban job and a downtown one are the same
    # row to every other lane in this file, and 17,006 of the 43,923 in-state rows
    # name the metro
    # -- and city never survives the load at all (triage_load.FIELDS keeps
    # `location` and drops city/state/country).  So the town has to be read out
    # of the free-text `location`, which is a builder's job and not a LIKE chain
    # of sixty terms here.
    #
    # `> 0` is exact, not a threshold.  geo_lane scores proximity as a FLOOR
    # -- 12 points inside ring 1, 6 in ring 2 -- so an admitted row cannot
    # score 0 and this predicate means
    # precisely "the builder put it in the region".  That floor is also what
    # lets the lane hold the whole local pool rather than a shortlist: an
    # off-target title scores its proximity and nothing else, so it caps at 12
    # and sits below every real row instead of being filtered away.
    ("local", "Local · commute ring",
     "local_score > 0", None),
    # The level fix, added 2026-08-20.  Of 239 applications sent 08-08 to 08-20,
    # 104 carry a senior-or-above token in the title against a resume with ~4
    # years post-grad and a 13-month gap; 780 of the 821 approvals came from
    # ai12 / aistartup / picks, all one family.  This lane is the corrected cut:
    # the same work at or below the target rung, plus adjacent families -- the
    # eleven swaths, support/TAM/implementation, and CS/AM.
    #
    # Here rather than in _DEFS for the third time for the same two reasons
    # `aistartup` and `aiagency` are: the predicate names a builder's output, so
    # a reader of _DEFS could not check its membership, and a _DEFS entry would
    # shift the digit keys off the four lanes with the most decisions behind
    # them.  `> 0` is exact -- level refuses to write a zero, so this
    # reads "the builder put it here" and no card can arrive with no reason.
    #
    # NO GEOGRAPHY TERM, deliberately, and the same call the `support` lane
    # makes: the `l` chip already filters MA / remote / South FL, and a location
    # clause here would make one lane mean two things.
    ("level", "Right-level · down & sideways",
     "level_score > 0", None),
]

# Predicate lanes (_DEFS and _CUT_DEFS) that rank on a score of their own instead
# of on AI strength.  _FEED_LANES carries its ORDER BY inline because every tag
# lane has one; predicate lanes overwhelmingly want ORDER_TERMS, so the
# exceptions live here rather than widening those tuples for two lanes.
_DEF_TERMS = {"sfl": SFL_TERMS, "pay": PAY_TERMS,
              "aistartup": AISTARTUP_TERMS, "aiagency": AIAGENCY_TERMS,
              "local": LOCAL_TERMS, "level": LEVEL_TERMS}

# Which (score, why) column pair ranked each lane, so the card can show the
# number that actually decided its position.  Anything absent uses the default.
#
# The server resolves this and sends the result as `rank_score`/`rank_why`;
# index.html reads those and never names a lane.  That indirection is the whole
# point — the card used to pick the column with a ternary over lane names, and
# every lane added after it shipped with its ranking invisible until somebody
# remembered to go back and edit the template.
LANE_SCORE = {
    "fastmoney": ("money_score", "money_why"),
    "sfl": ("geo_score", "geo_why"),
    "aistartup": ("startup_score", "startup_why"),
    "aiagency": ("desk_score", "desk_why"),
    "local": ("local_score", "local_why"),
    "level": ("level_score", "level_why"),
}
DEFAULT_SCORE = ("score", "score_why")


def score_cols(key):
    """(score_col, why_col) for a lane — the pair that ordered it."""
    return LANE_SCORE.get(key, DEFAULT_SCORE)

# The headings the lane rail groups under.  Twenty lanes is a menu, not a tab
# strip: rendered as one flat row they wrapped five deep out of a 46px header
# and everything past the first row was clipped and unclickable.
#
# Order here is the order the rail draws them in, and it MATCHES the order
# lanes() returns lanes in — Contract ahead of Swaths, because _FEED_LANES
# lists the two contract lanes before the eleven swaths.  That is not a
# cosmetic choice.  The digit keys address the first ten lanes in lanes()
# order, and the rail prints each lane's digit beside it: group the rail in any
# other order and the numbers run 1-7, then 0 against the first swath, then 8
# and 9 further down.  Every key still works, and the column looks broken.
#
# So: reorder these two lists together or not at all.  The alternative — having
# the digits follow rail order instead — would silently move keys 8/9/0 onto
# different lanes than the ones they have always meant.
GROUPS = [
    ("corpus",   "Corpus"),
    ("picks",    "Picks"),
    ("contract", "Contract"),
    ("swaths",   "Swaths"),
    ("cuts",     "Cuts"),
    ("scout",    "Scout"),
]

# The scout lane is NOT in lanes().  Every other lane is a predicate over
# triage.db.jobs; this one ranks corpus.db in memory against a weight profile
# the page can change between two fetches, so it has no predicate to return and
# no score column to sort on.  It is appended LAST by the server, which keeps
# the digit hotkeys pointing at the same first ten lanes they always have --
# the invariant this file's GROUPS comment is entirely about.
SCOUT_LANE = "scout"


def group_of(key):
    """Which rail heading a lane sits under — DERIVED, never a hand-kept map.

    A twelfth swath appended to _FEED_LANES files itself under Swaths and a
    third contract lane under Contract, with nothing here to remember to edit.
    A literal {lane: group} dict would have been shorter and would have gone
    stale the first time somebody added a lane, which is exactly how the card
    used to ship new lanes with their ranking invisible (see LANE_SCORE).

    Note this is a side map like LANE_SCORE, LANE_RESUME and _DEF_TERMS, not a
    fifth element on the lane tuples: lanes() is unpacked positionally in
    several places that have no interest in how the menu is drawn.
    """
    if key == "picks":
        return "picks"
    for lane_key, _, _, feed, _ in _FEED_LANES:
        if lane_key == key:
            return "contract" if feed == "contract" else "swaths"
    if any(lane_key == key for lane_key, _, _, _ in _CUT_DEFS):
        return "cuts"
    if key == SCOUT_LANE:
        return "scout"
    return "corpus"

# Which resume to send, per lane.  A constant map, not a schema change: the server sends it
# with each card and index.html prints "Send: F2 · Solutions Architect" on it, and
# triage_export.py puts the same column in the Worklist tab from this same source.
#
# Two artifacts per swath and they are not interchangeable:
#   `pdf`   the one-page screening cut, `build.py --variant F2` — for an ATS or a portal
#   `draft` the two-page full argument — for a human or a warm intro
LANE_RESUME = {
    "picks":      ("A1/A2", "Forward Deployed / Applied AI",
                   "apply/resume/out/resume_fde.pdf", None),
    # The same artifact `picks` sends, and deliberately so: this lane is AI
    # bands 1-2 by construction, which is the population the FDE one-pager was
    # cut for.  Sharing it is not a stand-in for a lane-specific resume that
    # does not exist yet — there is no narrower variant to write, because
    # "AI/FDE at a startup" is what A1/A2 already argues.
    "aistartup":  ("A1/A2", "Forward Deployed / Applied AI",
                   "apply/resume/out/resume_fde.pdf", None),
    # The same one-pager again, and here the reasoning is stronger than "no
    # narrower variant exists".  A staffing submittal IS a screening cut — the
    # recruiter keyword-matches it against a client req before a human at the
    # client ever sees it — so the one-page ATS artifact is the right send, and
    # the two-page argument would be read by nobody in that chain.
    "aiagency":   ("A1/A2", "Forward Deployed / Applied AI",
                   "apply/resume/out/resume_fde.pdf", None),
    "presales":   ("F2", "Solutions Architect / Presales",
                   "apply/resume/out/resume_F2.pdf",
                   "apply/resume/out/swaths/F2-solutions-architect-presales.pdf"),
    "gtm":        ("F1", "Revenue Systems / GTM",
                   "apply/resume/out/resume_F1.pdf",
                   "apply/resume/out/swaths/F1-revenue-systems-gtm.pdf"),
    "aiexpert":   ("F3", "AI Expert & Evaluation",
                   "apply/resume/out/resume_F3.pdf",
                   "apply/resume/out/swaths/F3-ai-expert-profile.pdf"),
    "bizops":     ("F4", "BizOps / Chief of Staff",
                   "apply/resume/out/resume_F4.pdf",
                   "apply/resume/out/swaths/F4-bizops-chief-of-staff.pdf"),
    "analytics":  ("F5", "Analytics Engineer / BI",
                   "apply/resume/out/resume_F5.pdf",
                   "apply/resume/out/swaths/F5-analytics-engineer-bi.pdf"),
    "automation": ("F6", "Automation & Integration",
                   "apply/resume/out/resume_F6.pdf",
                   "apply/resume/out/swaths/F6-automation-integration.pdf"),
    "ecom":       ("F7", "Ecommerce / DTC Operator",
                   "apply/resume/out/resume_F7.pdf",
                   "apply/resume/out/swaths/F7-ecommerce-dtc-operator.pdf"),
    "aienable":   ("F8", "AI Enablement / Transformation",
                   "apply/resume/out/resume_F8.pdf",
                   "apply/resume/out/swaths/F8-ai-enablement-transformation.pdf"),
    "fintech":    ("F9", "Fintech Ops · payments",
                   "apply/resume/out/resume_F9.pdf",
                   "apply/resume/out/swaths/F9-fintech-ops-payments.pdf"),
    "rwa":        ("F10", "Digital Assets / RWA",
                   "apply/resume/out/resume_F10.pdf",
                   "apply/resume/out/swaths/F10-digital-assets-rwa.pdf"),
    "trust":      ("F11", "Trust & Safety / Risk Ops",
                   "apply/resume/out/resume_F11.pdf",
                   "apply/resume/out/swaths/F11-trust-safety-risk-ops.pdf"),
    # `fundfinance` is deliberately ABSENT, the same call `csam` makes and for
    # a sharper reason.  Every other entry here maps a lane to one artifact
    # because the lane is one job family; this lane is one MARKET across eight
    # functions, and its top rows split across F1 (BD at Juniper Square,
    # Ontra), F2 (solutions at Ondo, Vega), F9/F10 (Securitize, Fireblocks)
    # and the AI one-pager (Addepar's Forward Deployed Engineer).  A single
    # default here would be wrong for most of the lane, so resume_for() returns
    # None and the card omits its "Send:" line rather than printing a
    # confident answer to a question this lane cannot answer per-row.
}


def resume_for(key):
    """(code, label, pdf, draft) for a lane, or None. `draft` is None where there
    is no two-page version (picks sends the existing fde one-pager)."""
    return LANE_RESUME.get(key)


def lanes(cutoff=None):
    """[(key, label, predicate_sql, params), ...] including `rest` and feeds."""
    cutoff = cutoff or recent_cutoff()
    out = []
    for key, label, sql, n in _DEFS:
        out.append((key, label, f"feed = 'main' AND ({sql})", [cutoff] * n))
    # Everything the named main-feed lanes don't already cover, so no main-feed
    # row is unreachable.
    rest_sql = ("feed = 'main' AND NOT ("
                + " OR ".join(f"({sql})" for _, _, sql, _ in _DEFS) + ")")
    rest_params = [cutoff] * sum(n for _, _, _, n in _DEFS)
    out.append(("rest", "Everything else", rest_sql, rest_params))
    for key, label, tag, feed, _ in _FEED_LANES:
        out.append((key, label, "feed = ? AND lane_tags LIKE ?",
                    [feed, f"%,{tag},%"]))
    # The cuts come last, after the feed lanes, which is what keeps the digit
    # keys on the lanes that have always had them — see the note on _CUT_DEFS.
    for key, label, sql, days in _CUT_DEFS:
        if days is None:
            params = []
        elif days == RECENT_DAYS:
            # `maold` is the exact complement of `ma`, so it has to bind the
            # cutoff the CALLER gave rather than compute a second one here: pass
            # lanes() an explicit cutoff and a locally-recomputed date would put
            # the boundary in two places, leaving a day's worth of MA rows in
            # both lanes or in neither.
            params = [cutoff]
        else:
            params = [recent_cutoff(days)]
        out.append((key, label, f"feed = 'main' AND ({sql})", params))
    return out


def order_for(key, sort=None):
    """The ORDER BY a lane sorts by — not every lane wants the AI ordering.

    `sort` prepends the posted-date terms; 'fit' (the default) leaves the
    lane's own ranking exactly as it was.  Under a posted sort the lane's own
    trailing `posted DESC` is left in place and simply never fires — one
    definition of each lane's ranking is worth more than a tidier clause.
    """
    terms = _DEF_TERMS.get(key, ORDER_TERMS)
    for lane_key, _, _, _, lane_terms in _FEED_LANES:
        if lane_key == key:
            terms = lane_terms
            break
    return "ORDER BY " + posted_terms(sort) + terms


def lane_map(cutoff=None):
    return {key: (label, sql, params) for key, label, sql, params
            in lanes(cutoff)}


UNDECIDED = ("live = 1 AND NOT EXISTS "
             "(SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)")


def queue_sql(predicate, select="*", order=True, limit=None, offset=None):
    """A full queue query for one lane: live, undecided, lane-matching.

    `order` is True for the default AI ordering, False for none, or an explicit
    ORDER BY clause (see order_for) for a lane that ranks on something else.

    `offset` pages past rows the caller is already holding.  The queue normally
    advances without one -- a decided row leaves UNDECIDED, so re-running the
    identical query returns different rows -- and that is enough for a page
    that shows one card at a time and decides it before asking for the next.
    A grid is scrolled rather than decided, so it has to reach row 121 without
    touching rows 1-120, and nothing in the predicate would have moved for it.
    Left None the string is byte-identical to the un-paged one, so no lane's
    ordering, count or fingerprint changes; lane_snapshot.py proves it.
    """
    sql = (f"SELECT {select} FROM jobs "
           f"WHERE {UNDECIDED} AND ({predicate})")
    if order:
        sql += " " + (order if isinstance(order, str) else ORDER_BY)
    if limit:
        sql += f" LIMIT {int(limit)}"
    if offset:
        # SQLite has no bare OFFSET -- it is a suffix of LIMIT -- so an offset
        # asked for without one has to supply the "no limit" limit itself.
        # Every caller here passes a limit; this is so the helper cannot be
        # made to build invalid SQL by one that doesn't.
        if not limit:
            sql += " LIMIT -1"
        sql += f" OFFSET {int(offset)}"
    return sql


def counts(con, cutoff=None):
    """[{key, label, group, remaining}, ...] — one indexed COUNT(*) per lane."""
    out = []
    for key, label, sql, params in lanes(cutoff):
        n = con.execute(queue_sql(sql, "COUNT(*)", order=False),
                        params).fetchone()[0]
        out.append({"key": key, "label": label, "group": group_of(key),
                    "remaining": n})
    return out
