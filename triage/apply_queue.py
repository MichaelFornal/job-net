#!/usr/bin/env python3
"""The apply queue — what you approved, and how far you've got with it.

Same job for `/apply` that triage/lanes.py does for the triage queue: the one
place that defines what the queue means, imported by both triage/server.py and
triage_export.py so the page and the workbook can never disagree about how many
are left.

Two rules make the rest of this simple:

  * **'to apply' is the absence of a row.**  `applications` only ever holds
    jobs you have acted on.  Nothing has to be backfilled when you approve more
    in triage — a new approval is in this queue the moment the page reloads —
    and there is no seeded row to go stale if a job is later un-approved.
  * **Every read joins `jobs` and filters `live = 1`.**  After a corpus rebuild
    carry_forward_applications() gives the status to the job's new job_key and
    deliberately leaves the predecessor row behind, so one ct_key can hold two
    rows.  Only one of them is live, so the live filter is what stops
    "12 applied" quietly becoming "13 applied".

`reason = 'manual'` on the verdict is inherited from triage_export.py: a deny
that came from a company kill is not a judgement on the job, and by the same
token only a hand-made approve belongs in a queue you are going to work.
"""
from triage import lanes as L

# 'to_apply' is a computed value, never a stored one — set_status() deletes the
# row instead of writing it.  Kept in the tuple because it is a legal thing to
# ask for and a legal thing to set back to.
#
# 'applying' is the one in-flight state: the form is open, or half-filled, or
# waiting on a referral, and you have not submitted.  It is deliberately a
# status and not a flag — a job is in exactly one of these at a time, so the
# tab badges still partition the queue and `total` still adds up.  It stamps no
# applied_at (see set_status): the date on the row is the date you *submitted*,
# and a started-but-unsent application has not got one yet.
STATUSES = ("to_apply", "applying", "applied", "skipped", "dead")

# What came BACK, which is a different axis from STATUSES: `status` is what our side did,
# `outcome` is what the employer did, and an application stays 'applied' forever while
# its outcome moves. Ordered weakest to strongest — a mail-sync integration relies on it to
# avoid downgrading an interview to a later form rejection.
OUTCOMES = ("", "acknowledged", "rejected", "interview", "offer", "ghost")

# The apply page's filter bar: {name: {option: predicate}}.  The unset option
# ('' or None) means "any" and contributes nothing to the WHERE, so an unused
# facet costs nothing rather than adding a no-op AND.
#
# `band` rides rank_tier rather than string-matching ai_band.  It is the same
# precomputed int the triage lanes rank on (1..5, 9 = not an AI role) and the
# lead column of idx_jobs_queue, so "band 1-2" here and "AI 1-2" in lanes.py
# cannot drift apart the way two spellings of the same idea would.
#
# `ma` takes in Nearby (RI/NH/CT) alongside MA local: same drive, same
# decision.  Split apart, Nearby would be a chip you would never click alone.
FACETS = {
    "src":  {"direct": "j.is_agg = 0",
             "agg":    "j.is_agg = 1"},
    "band": {"12":     "j.rank_tier <= 2",
             "1":      "j.rank_tier = 1"},
    "loc":  {"ma":     "j.bucket IN ('MA local', 'Nearby (RI/NH/CT)')",
             "remote": "j.bucket LIKE 'Remote%'",
             "sfl":    "j.bucket = 'South FL'"},
}

APPROVED = "j.live = 1 AND d.verdict = 'approve' AND d.reason = 'manual'"

COLS = """
    j.job_key, j.company, j.company_norm, j.title, j.location, j.work_mode,
    j.posted, j.salary_min, j.salary_max, j.salary_currency, j.salary_period,
    j.ai_band, j.startup_tier, j.skills, j.source, j.is_agg,
    j.date_basis, j.date_weak, j.bucket,
    j.employment_type, j.seniority, j.company_size, j.company_industry,
    j.apply_url, j.job_url,
    d.decided_at, d.lane,
    COALESCE(a.status, 'to_apply') AS status,
    COALESCE(a.outcome, '') AS outcome,
    COALESCE(a.outcome_at, '') AS outcome_at,
    COALESCE(a.applied_at, '')     AS applied_at,
    COALESCE(a.note, '')           AS note,
    COALESCE(a.updated_at, '')     AS updated_at
"""

FROM = """
    FROM jobs j
    JOIN decisions d          ON d.job_key = j.job_key
    LEFT JOIN applications a  ON a.job_key = j.job_key
"""

# In-flight first, then unworked; inside that, triage's own ordering (employer
# boards before aggregators, strongest AI signal first, newest first) so the
# best thing to apply to next is at the top.  Worked rows sort most-recent-
# first, which is what you want when reviewing the Applied tab.  a.updated_at
# is NULL for unworked rows, so that term is a no-op tie for them and they fall
# straight through to the triage ordering.
#
# The 'applying' term only bites in the All tab, where every other term would
# bury a half-finished application under a few hundred untouched ones: it has
# an updated_at, so without this it sorts as "worked" and lands below them.
# The thing you are part-way through is the thing you most need to see.
#
# WORK_ORDER is the part that survives every sort: which pile a row is in
# (in-flight, unworked, done) is not a ranking, it is the shape of the queue,
# and a posted-date sort that shuffled a finished row above a to-do one would
# be answering a question nobody asked.  The posted terms therefore go BEHIND
# it and in front of the ranking, never at the very front.
WORK_ORDER = ("(COALESCE(a.status, 'to_apply') = 'applying') DESC, "
              "(a.updated_at IS NULL) DESC")
RECENT_WORK = "a.updated_at DESC"
FIT_TAIL = "j.is_agg ASC, j.rank_tier ASC, j.posted DESC, j.job_key ASC"
# Same as FIT_TAIL minus the posted term, which a posted sort has already
# decided by the time the clause gets this far.
TIEBREAK = "j.is_agg ASC, j.rank_tier ASC, j.job_key ASC"

ORDER_BY = f"ORDER BY {WORK_ORDER}, {RECENT_WORK}, {FIT_TAIL}"


def order_by(sort=None):
    """The queue's ORDER BY under `sort` (see lanes.posted_terms).

    RECENT_WORK is deliberately dropped under a posted sort.  Every worked row
    has its own updated_at, so that term alone fixes the order of the entire
    Applied / Skipped / Dead tabs: leave it in and Newest/Oldest would work in
    'To apply' and then silently do nothing the moment you changed tab, which
    is worse than not offering the control at all.  WORK_ORDER stays, so the
    grouping those tabs rely on is untouched either way.
    """
    terms = L.posted_terms(sort, p="j.")
    if not terms:
        return ORDER_BY
    return f"ORDER BY {WORK_ORDER}, {terms}{TIEBREAK}"


def _facets(sel, skip=None):
    """SQL fragments for the chosen facet options, `skip` left out.

    Every name and value in `sel` is validated even when skipped, so a typo in
    a facet is an error from whichever entry point sees it first rather than a
    filter that quietly does nothing.
    """
    out = []
    for name, value in (sel or {}).items():
        if name not in FACETS:
            raise ValueError(f"no such filter: {name}")
        if value and value not in FACETS[name]:
            raise ValueError(f"no such {name} filter: {value}")
        if value and name != skip:
            out.append(FACETS[name][value])
    return out


def _where(status=None, q=None, sel=None, skip=None):
    """WHERE clause + params for the approved set, narrowed by the filters.

    `skip` drops one facet from the clause (see facet_counts): a chip has to
    count the set you would get by clicking it, which is the current filter
    with that facet's own selection lifted, not the set already on screen.
    """
    sql, params = APPROVED, []
    if status:
        if status not in STATUSES:
            raise ValueError(f"no such status: {status}")
        sql += " AND COALESCE(a.status, 'to_apply') = ?"
        params.append(status)
    if q:
        sql += " AND (j.title LIKE ? OR j.company LIKE ?)"
        params += [f"%{q}%", f"%{q}%"]
    for frag in _facets(sel, skip):
        sql += f" AND {frag}"
    return sql, params


def queue(con, status=None, q=None, limit=None, sel=None, sort=None):
    """[{...}, ...] — the approved jobs, with their apply status attached.

    `sort` only ever reaches the ORDER BY.  It is not a facet and must never
    reach _where(): it does not narrow the set, so letting it in would make
    every chip badge and tab count describe a set nothing is looking at.
    """
    where, params = _where(status, q, sel)
    sql = f"SELECT {COLS} {FROM} WHERE {where} {order_by(sort)}"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [dict(r) for r in con.execute(sql, params)]


def counts(con, q=None, sel=None):
    """{to_apply, applied, skipped, dead, total} over the approved, live set."""
    where, params = _where(None, q, sel)
    out = dict.fromkeys(STATUSES, 0)
    rows = con.execute(
        f"SELECT COALESCE(a.status, 'to_apply') AS status, COUNT(*) n "
        f"{FROM} WHERE {where} GROUP BY 1", params)
    for r in rows:
        out[r["status"]] = r["n"]
    out["total"] = sum(out[s] for s in STATUSES)
    return out


def facet_counts(con, status=None, q=None, sel=None):
    """{facet: {'': n, option: n, ...}} — the number on every chip.

    Each facet is counted with its *own* selection lifted and the others left
    in place, the standard faceted-search rule.  Counted the naive way the
    unselected chips of an active facet would all read 0 and the bar would
    become a dead end you could only leave by clearing it.

    The '' key is that facet set to "any"; it is the total the other facets
    allow, so the chips of one group always sum to it.

    One COUNT(*) per chip rather than a GROUP BY, because `band` nests — 1 is
    inside 1-2 — and a grouped count would have to be re-added by hand per
    facet.  Nine indexed counts over a few hundred approved rows is nothing.
    """
    out = {}
    for name, opts in FACETS.items():
        where, params = _where(status, q, sel, skip=name)
        row = {"": con.execute(f"SELECT COUNT(*) {FROM} WHERE {where}",
                               params).fetchone()[0]}
        for opt, frag in opts.items():
            row[opt] = con.execute(
                f"SELECT COUNT(*) {FROM} WHERE {where} AND {frag}",
                params).fetchone()[0]
        out[name] = row
    return out


def set_status(con, job_key, status, note=None):
    """Record (or clear) what you did with one job.  Returns rows changed.

    Writes nothing for a job_key that isn't in `jobs` — the INSERT draws
    ct_key from there, so an unknown key silently produces no row rather than
    an orphan the carry-forward can never follow.

    `note` of None leaves any existing note alone; pass '' to clear it.
    """
    if status not in STATUSES:
        raise ValueError(f"no such status: {status}")
    before = con.total_changes
    if status == "to_apply":
        # Absence IS 'to apply'.  Deleting rather than storing it keeps one
        # meaning for one state and makes undo exact.
        con.execute("DELETE FROM applications WHERE job_key = ?", (job_key,))
    else:
        con.execute("""
            INSERT INTO applications
                (job_key, ct_key, status, applied_at, note, updated_at)
            SELECT ?, ct_key, ?,
                   CASE WHEN ? = 'applied' THEN date('now', 'localtime')
                        ELSE NULL END,
                   COALESCE(?, ''), datetime('now')
              FROM jobs WHERE job_key = ?
            ON CONFLICT(job_key) DO UPDATE SET
                status = excluded.status,
                -- Set on the first move into 'applied' and never touched
                -- again.  Clearing it on the way out looked tidier but loses
                -- real history: applied -> dead means the listing closed after
                -- you applied, and you still applied on that date.  It also
                -- made applied -> skipped -> applied silently restamp today.
                applied_at = COALESCE(applications.applied_at,
                                      excluded.applied_at),
                note = COALESCE(?, applications.note),
                updated_at = excluded.updated_at
        """, (job_key, status, status, note, job_key, note))
    con.commit()
    return con.total_changes - before


def set_outcome(con, job_key, outcome):
    """Record what came back from one application.  Returns rows changed.

    Deliberately UPDATE-only: an outcome on a job with no application row would be a
    reply to something never sent, which is always a matching bug rather than a fact
    worth storing.  Pass '' to clear.

    Unlike set_status this does not touch `status` — an application that gets rejected
    is still an application that was sent, and collapsing the two axes is what left 188
    rows unable to answer which lane converted.
    """
    if outcome not in OUTCOMES:
        raise ValueError(f"no such outcome: {outcome}")
    before = con.total_changes
    con.execute(
        "UPDATE applications SET outcome = ?, "
        "outcome_at = CASE WHEN ? = '' THEN NULL ELSE date('now', 'localtime') END, "
        "updated_at = datetime('now') WHERE job_key = ?",
        (outcome, outcome, job_key))
    con.commit()
    return con.total_changes - before


def set_many(con, items):
    """Batch of {job_key, status, note}.  Reports what the DB actually changed.

    `written` counts real row changes, not requests, for the same reason
    server.API.decide does: a job_key that isn't in `jobs` writes nothing, and
    reporting the request size would let a click look saved when it wasn't.
    """
    good = [(d.get("job_key"), d.get("status"), d.get("note"))
            for d in items
            if d.get("job_key") and d.get("status") in STATUSES]
    written = 0
    for key, status, note in good:
        written += set_status(con, key, status, note)
    out = {"written": written, "asked": len(items)}
    if written < len(good):
        # A 'to_apply' that cleared nothing is a no-op, not a lost write.
        missing = [k for k, s, _ in good if s != "to_apply" and
                   not con.execute(
                       "SELECT 1 FROM applications WHERE job_key = ?",
                       (k,)).fetchone()]
        if missing:
            out["missing"] = missing
    return out
