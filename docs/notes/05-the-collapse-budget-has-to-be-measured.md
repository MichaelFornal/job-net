# The collapse budget has to be measured, not defaulted

Board discovery sweeps the Wayback CDX index and collapses rows on a prefix of
the SURT urlkey, so one archived board does not return ten thousand times. The
collapse depth looks like a politeness knob. It is a data-loss knob.

At the default depth of 12, **one CDX page collapsed 13 Workday tenant triples
to 1**, and another lost 52% of its triples. Measured 2026-08-20 on pages 461
and 922 of 923, triples kept by budget:

| budget | page 922 | page 461 |
|---:|---:|---:|
| 12 | 134 | 1 |
| 40 | 267 | 12 |
| 70 | 276 | 13 |
| 100 | **278** | **13** |

70 still loses 0.7% against 100, over this repo's own 0.5% gate, so Workday sits
at 100. Rippling needed 70 for a different reason: most orgs appear only inside
Next.js data routes shaped `_next/data/{21-char hash}/en-us/{org}/jobs/...`, so
at depth 12 every one collapses to a single row and 45% of orgs vanish. Seven
more apexes moved to 36 after a probe found the default losing slugs above the
0.5% gate on all of them — 15.38% on one, 9.21% on another, 0.88–2.19% on the
rest. Six others measure 0.00–0.27% and stay at 12.

## Why deepening everything is wrong

Depth multiplies pages fetched against a rate-limited host, and the probe that
justifies a deepening re-probes at `budget + 24` — so every escalation
manufactures a fresh "failure" against a deeper reference. Sampling is its own
trap: SURT sorts implementation sandboxes before real tenants, so a page-0-only
probe reads the largest platform on the internet as tiny.

## The mechanism

[slug_common.py](../../slug_common.py) makes each target a declarative `Apex`
record — CDX pattern, SURT prefix, extraction mode (`host` vs `path`), and its
own measured budget — so per-apex differences stay pure data. Changing a
collapse drops that apex's cached pages, so the re-crawl is automatic.

Sharing also fixed a silent corruption that had nine copies: `web.archive.org`
serves an nginx "504 Gateway Time-out" page **with HTTP status 200**. The
per-family copies checked only the status code, wrote that HTML into their slug
file, and recorded the page as done — losing the real slugs permanently, with no
way to notice afterward.
