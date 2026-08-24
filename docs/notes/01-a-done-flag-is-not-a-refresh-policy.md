# A done flag is not a refresh policy

The ATS fetcher's state file answers one question — "have I ever fetched this
board?" — and once the answer is yes it is yes forever. That was correct while a
~70,500-board queue drained for the first time.

On 2026-08-16 the backlog finished draining. All 71,592 keys read `done`, the
seven-platform chain fetched **88 rows**, and its eight JSONL outputs came back
byte-identical to their `.prev` copies. Eight crawlers had quietly become a
one-time crawl — and those sources are 17.9% of the master (568,403 of 3,172,240
rows) and the only route to 18.4% of its companies.

## Why "expire everything and re-crawl" is wrong

A blind sweep costs ~70k boards per run, and 40,290 of them have never produced
a single row in the corpus's lifetime, while 8,943 boards — 12.3% — carry 80% of
every row held. Uniform expiry spends the whole budget on the part of the queue
that is measurably empty.

## The mechanism

[ats_ledger.py](../../ats_ledger.py) records what each board is *worth* and
sells eligibility against it: HOT (the smallest set covering 80% of its
platform's rows) every run, WARM 1 run in 3, COLD 1 run in 10, RETIRED never.
Coverage still closes in ~10 runs, at ~21k boards queued instead of ~70k.

Two details are load-bearing, not preferences:

- Slice membership is `zlib.crc32(key) % cycle`, **not** `hash()`.
  `str.__hash__` is salted per process by `PYTHONHASHSEED`, so a `hash()`-based
  slice lands somewhere different on every attempt of every run and coverage
  never closes. The comparand is date-derived rather than a counter, so
  supervised retries hit the same slice — attempt 2 is a resume, not a sweep.
- `rows` is only ever written by an *authoritative* fetch. A board answering
  empty once keeps its old count and bumps `empty_streak`; letting one bad day
  zero it would demote a live board out of HOT into a 1-in-10 cadence — the
  freeze in miniature.

Ledger files are per-chain, because two chains run concurrently and each saves
its whole dict at exit. On 2026-08-10 the two sharing one file left it holding
1,194 keys of the 24,836 the main chain had just finished — 23,642 lost.
