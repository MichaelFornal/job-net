# `posted` is not a posting date

On 2026-08-18, **57% of the rows stamped `posted = 2026-08-18` were not posted
that day**. The date was the day some *intermediary* first saw the job — a
talent network, an aggregator, or this pipeline's own crawler. The underlying
posting can be arbitrarily old, so a queue sorted `posted DESC` puts stale and
already-filled roles at the very top.

Measured over the 3,748 rows dated 2026-08-18:

| basis | rows | share |
|---|---:|---:|
| employer's own ATS published it | 1,625 | 43% |
| talent-network ingest date | 1,001 | 27% |
| aggregator ingest | 868 | 23% |
| this crawler's first sight | 253 | 7% |

The evidence was already in the tree: the largest network source — 87.7% of the
master corpus — sets `posted` from `job.created_at`, and its own legend calls
that column "First-seen posting date (UTC)".

## Why reusing the existing aggregator set is wrong

The tree already has an aggregator set (`startup_build.EXCL_SRC`), and reusing
it is wrong twice over: it lists Workday, Oracle, iCIMS and ADP — employer
ATSes, not republishers — and omits the largest mis-dated bucket. It would mark
**1,001 rows trustworthy and 585 untrustworthy, both backwards.** That set
denies a startup *tier*; it was never a statement about dates.

## The mechanism

[date_basis.py](../../date_basis.py) sorts each source into one of four bases —
EMPLOYER, NETWORK, AGGREGATOR, FIRST_SEEN — and everything except EMPLOYER means
"this is not when the employer posted it." That settles 59 of 67 sources.

Eight are *mixed*: they publish a real date when the board has one and fall back
to the crawl date when it does not. Guessing at source level for those would be
the same sin, so [date_basis_index.py](../../date_basis_index.py) resolves them
per row against the `*_posted_ledger.json` files, which carry a `precision` per
job id — a field that dies at the fetcher because it is not a master column.

Two ledger semantics must not be mixed up. Workday and iCIMS write *every* job,
so fabricated means `precision == "first_seen"`, plus `"30plus"` — an unbounded
today-minus-30 floor, 244,269 of 718,483 entries. The long-tail fetcher writes
only when the date was *missing*, so presence is itself the answer: reading that
ledger the Workday way would have marked 29,816 of 40,000 sampled rows
fabricated when their dates were real.
