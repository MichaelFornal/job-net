# A primary key that moves between rebuilds

The triage database keys rows on `job_key`, the corpus's own identity column.
`job_key` is **not stable across rebuilds**. The corpus builder collapses
duplicates on company + title and keeps whichever source copy wins the sort, so
when a different source wins next time the surviving `job_key` for the same job
changes, and anything keyed on it silently loses its attachment.

The consequences are asymmetric. A review verdict that fails to carry forward
refills the queue with rows already judged — which is how a triage tool stops
getting used. An *applied* flag that fails to carry forward is worse: a reset
applied flag means applying to the same job twice.

## Why a ledger keyed on `job_key` does not work

An earlier attempt in the tree — a CSV ledger keyed on `job_key` — is still
empty, because nothing ever matched. The carry-forward key has to derive from
the row's *content*, not from the identity the corpus happens to mint that day.

## The mechanism

[triage_load.py](../../triage_load.py) stores `ct_key` (`company|title`) on
every decision, and a new `job_key` whose `ct_key` matches a decided one inherits
that decision. Application records ride the same path. Around that:

- The loader **never deletes or rewrites** the decisions table; only an explicit
  undo removes a row.
- Rows that vanish from the corpus go `live = 0` rather than being deleted, so a
  decided job cannot come back as new.
- Two feeds share one jobs table, so decisions and the apply queue need no
  second code path — but the retire sweep must be feed-scoped, or each feed
  retires the other's rows.
- The load stamp is generated per load to microseconds, because retire finds
  vanished rows by asking which ones this load did not touch. A constant frozen
  at import retires nothing when two loads share a process, and second precision
  collides when two runs land in the same second.

A separate hazard sits in the same header: the loader upserts derived ranking
columns from a CSV that does not carry them, so a load blanks every downstream
ranking until the ranking builders re-run — and a skipped builder leaves its
slice of the queue silently unranked, which the UI does not show.
