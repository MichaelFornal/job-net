# Engineering notes

Ten short postmortems from running this pipeline against dozens of live ATS
platforms. Each one is a failure that was measured before it was fixed, and the
invariant that now prevents it. Most striking first.

| # | Note | The finding |
|---|---|---|
| 01 | [A done flag is not a refresh policy](01-a-done-flag-is-not-a-refresh-policy.md) | All 71,592 board keys read `done`, the run fetched 88 rows, and eight outputs came back byte-identical — eight crawlers had quietly become a one-time crawl. |
| 02 | [`posted` is not a posting date](02-posted-is-not-a-posting-date.md) | 57% of the rows stamped with today's date were not posted today; only 43% carry the employer's own date. |
| 03 | [No two ATSes agree on what dead looks like](03-no-ats-agrees-on-what-dead-looks-like.md) | One vendor ships "this job post is no longer available" in its i18n bundle on *every* page, so naive body-matching scored 9 of 10 live postings dead. |
| 04 | [A throttle penalty with no decay is permanent](04-a-throttle-penalty-with-no-decay-is-permanent.md) | Starting at 3.0 rps instead of 1.5 pinned the interval at its floor and halved throughput — 21 candidates/min against 39. |
| 05 | [The collapse budget has to be measured](05-the-collapse-budget-has-to-be-measured.md) | At the default depth, one archive page collapsed 13 distinct tenants into 1; the right depth differs per platform and only a probe can establish it. |
| 06 | [A closed regex list cannot find what it does not list](06-a-closed-regex-list-cannot-find-what-it-does-not-list.md) | Five ATS platforms with 1,267 boards between them were invisible to every census for a month — not unbuilt, unseen. |
| 07 | [URL equality is not job identity](07-url-equality-is-not-job-identity.md) | One optional locale segment turned 3,137 already-known postings into 3,020 false uniques. |
| 08 | [A primary key that moves between rebuilds](08-a-primary-key-that-moves-between-rebuilds.md) | The corpus re-mints `job_key` when a different source wins the dedupe, so carry-forward has to hang off content — a reset applied flag means applying twice. |
| 09 | [A newline inside a key reports every row as new](09-a-newline-inside-a-key-reports-every-row-as-new.md) | Multi-line source titles put newlines inside tier-3 keys, so a plain one-key-per-line snapshot is silently wrong forever. |
| 10 | [Drop the done keys, keep the dead ones](10-drop-the-done-keys-keep-the-dead-ones.md) | A state file holds two kinds of knowledge: one is cheap to rebuild, and one is a census that cost real time to learn. |

## The shape these share

Six of the ten are the same failure wearing different clothes: **a stored value
that can only ever move in one direction.** A `done` flag that never expires, a
throttle penalty that never decays, a `job_key` that never rematches, a regex
list that never grows, a collapse depth that never deepens, a dead marker that
gets deleted along with everything else. Each one looked correct on the day it
was written and became wrong when the workload it assumed changed underneath it.

The other four are measurement failures: a date that means something other than
what its column name says, a URL treated as an identity, markup read as prose,
a set-difference format that cannot round-trip its own keys.
