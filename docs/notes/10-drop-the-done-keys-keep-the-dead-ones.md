# Drop the done keys, keep the dead ones

Several tail fetchers have no refresh flag: `work()` skips any tenant whose
state says `done`, so re-running one is nearly a no-op and its rows stay as old
as the day they were first fetched. The only way to refresh those tenants is to
make the fetcher forget they were done.

## Why deleting the state file is wrong

A state file holds two kinds of knowledge. The `done` markers are cheap to
rebuild — rebuilding them *is* re-fetching. The **dead** markers are a census
that cost real time to learn, and each encodes a platform-specific
discrimination that is easy to get wrong:

- Workday's `403-S22` closed board, versus a 403 from the edge.
- Comeet's 302 to the vendor's marketing site.
- Keka's `/TenantNotFound.html`.
- Zoho Recruit's `403 ORG_NOT_FOUND`, which is terminal — while its **404 means
  the org is alive under a different portal name**.

Deleting the file throws those away, re-probes every corpse, and re-derives the
same distinctions on the next run.

## The mechanism

[state_refresh.py](../../state_refresh.py) keeps every key whose value looks
dead and drops every key that looks done, matching a shared prefix list
(`dead`, `err`, `retired`, `not_a_board`, `blocked`, `migrated`, …). The fetcher
then re-fetches exactly the live tenants and skips the corpses.

Duplicate rows are not a concern: the combining build collapses on
`(source, board, job_id)` keeping `max(posted)`, so a re-fetched posting merges
with its older copy. The cost is disk and build time, not correctness.

Two guards are worth stealing. **Never rewrite a file that could not be read** —
an unreadable state loads as `{}` downstream, and rewriting it would make that
loss permanent — and write through a temp file plus `os.replace`, because a torn
state also reads as `{}`.

The last refinement is *scoping*. One state file is keyed `{family}:{tenant}`
across six families: five had no refresh path and sat frozen, while the sixth
was already refreshed by its own flag and must not be touched from here. A
scoped run considers only keys under the named prefixes and keeps every other
key verbatim. Without it the choice is "unfreeze five" or "leave all six".
