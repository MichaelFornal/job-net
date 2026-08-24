#!/usr/bin/env python3
"""
rippling_discover_boards.py — board-slug discovery for the rippling family.

A thin wrapper so the per-family filename and the canonical last-line contract
survive; the crawl itself lives in slug_discover_boards.py, which covers every
ATS apex from one CDX politeness budget and one response-body validator.

  python3 rippling_discover_boards.py          -> rippling_candidates.csv
  SMOKE=2 python3 rippling_discover_boards.py  -> smoke_rippling_candidates.csv

Covers both Rippling URL shapes:
  ats.rippling.com/{org}/jobs/{uuid}          (and an /en-US/ locale variant)
  {tenant}.rippling-ats.com/job/{id}/{slug}
"""
import sys

import slug_discover_boards as sd

if __name__ == "__main__":
    sys.exit(sd.main(["--apex", "rippling"] + sys.argv[1:]))
