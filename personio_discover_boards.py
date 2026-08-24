#!/usr/bin/env python3
"""
personio_discover_boards.py — board-slug discovery for the personio family.

Thin wrapper over slug_discover_boards.py (see rippling_discover_boards.py for
why the crawl is shared).  Covers both tenant hosts:
  {slug}.jobs.personio.de
  {slug}.jobs.personio.com

  python3 personio_discover_boards.py          -> personio_candidates.csv
  SMOKE=2 python3 personio_discover_boards.py  -> smoke_personio_candidates.csv
"""
import sys

import slug_discover_boards as sd

if __name__ == "__main__":
    sys.exit(sd.main(["--apex", "personio"] + sys.argv[1:]))
