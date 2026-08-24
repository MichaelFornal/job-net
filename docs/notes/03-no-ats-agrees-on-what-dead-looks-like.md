# No two ATSes agree on what a dead posting looks like

The corpus marks a row live when the last crawl still saw it, and postings decay
fast: **156 of 522 rows had already gone dead** by the time anyone re-checked —
about 30% in two weeks. So `live = 1` means "was live two days ago", which is
not the question that matters.

Probing for the real answer runs into the fact that no ATS announces a dead
posting the same way, and none use 404 reliably. So
[worklist_live.py](../../worklist_live.py) carries one measured detector per
platform:

- **Ashby** — a retired posting still returns 200 with the SPA shell, ~7 KB
  against 24–34 KB live. Size is the only signal in the HTML.
- **Greenhouse** — redirects to the board root carrying `?error=true`.
- **Lever** — 404, or a redirect to the board root.
- **Workday** — 200 for *both*, an empty `<title>` for both, and no "no longer
  available" text anywhere. Against a bogus requisition id on a live tenant:
  dead 6,117 B, live 20,724 B; across 25 sampled live rows, 6.6 KB vs
  15.5–33.5 KB. Before this every dead Workday posting scored live, and Workday
  is ~19% of the queue.

## Why body-matching is wrong

The obvious detector greps the response body for "no longer available". A
**live** Rippling page carries that exact string in its embedded i18n bundle —
on every page, live or dead — so matching the raw body inverted the source
completely: 9 of 10 live postings scored dead, and the genuinely dead ones
scored `unknown`. The same trap caught a page whose *stylesheet* defines
`span.expired-title`.

Both wrong verdicts came from reading markup as prose. The invariant: match
visible text, never the raw body. Everything now passes through a
`visible_text()` that strips `<script>`, `<style>` and `<template>` first.

## `blocked` is not `unknown`

One source 403s every request from this network — a verdict about access, not
about the job. It is recorded as `blocked`, a permanent property of the source,
and 483 of the rows dated 2026-08-18 come from it. Folding it into `unknown`
would imply a retry could resolve it.
