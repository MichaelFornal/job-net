# URL equality is not job identity

Every source publishes the same job with a cosmetically different URL: `/apply`
vs `/application` suffixes, `boards.` vs `job-boards.` hosts, a trailing
`-title-slug`. Keying a merge on the URL string banks the same posting several
times, and the error is large enough to change what the corpus looks like:

- **Dayforce** — the locale segment is optional in
  `/[{locale}/]{client}/{site}/jobs/{id}`. Matched by id, 3,137 postings already
  existed; matched by URL, only 117. **3,020 false uniques** from one optional
  path segment.
- **UKG Pro** — the same posting is served from numbered pods and from
  per-tenant hosts. 8,985 matched by `opportunityId`, 6,748 by URL: 2,237 false
  uniques.
- **NEOGOV** — two shapes for one posting, with and without an `/index/`
  segment. Measured 2026-08-20: 2,059 plain, 820 with `/index/`.

## The mechanism

[all_jobs_build.py](../../all_jobs_build.py) resolves identity in three tiers,
strongest available wins: a canonical `(platform, org, id)` tuple parsed out of
the URL; then `(company, title, apply_url)`; then `(company, title, location)`
when there is no usable URL at all.

`canon_key()` is the tier-1 parser and carries **26 platform ids**, each with
its own grammar. Several branches encode a specific hazard: two Workday hosts
put the tenant in different places (leftmost host label vs path segment), so
reusing one branch for the other would key every tenant on a pod and merge two
unrelated employers that share a requisition id.

## Why tier 2 is wrong for aggregators

Tier 2 keys on the apply URL, which is right when the URL identifies the
posting. An aggregator mints its own id for a job it re-listed from elsewhere,
so keying on it makes every one of its rows permanently unique and guarantees
the job never merges with the copy already held from its real ATS. Measured
2026-07-29 on one aggregator: 12 of 12 sampled redirect URLs resolve back to the
aggregator itself, so 100% of its rows would have landed as re-labelled
duplicates. Those hosts drop to tier 3, which *does* match the existing copy,
while the URL stays in the row.

One implementation note worth keeping: a single-line `setdefault` string intern
is what lets ~1.7M rows fit in 16 GB during the merge.
