#!/usr/bin/env python3
"""fetch_demo.py — crawl ONE real public ATS board, end to end, in ~10 seconds.

The full pipeline is a multi-hour run across dozens of platforms and tens of
thousands of boards.  This is the same path at a scale you can watch: hit one
unauthenticated endpoint, normalize what comes back through the row contract
every one of the 40+ crawlers writes, and print it.

No API key.  Greenhouse's board API is public and documented; nothing here logs
in, and nothing here reads a page it was not served.

    python3 tools/fetch_demo.py                 # default board
    python3 tools/fetch_demo.py --board figma --limit 5
    python3 tools/fetch_demo.py --out demo.jsonl

The point to notice is `contract_common.row()`.  Every crawler in this repo --
Greenhouse, Lever, Workday, iCIMS, 40-odd others -- emits that same dict, and
that is the only reason a single dedupe pass downstream can merge them.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import contract_common as cc  # noqa: E402

API = "https://boards-api.greenhouse.io/v1/boards/{}/jobs?content=false"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--board", default="gitlab",
                    help="Greenhouse board token, e.g. the name in "
                         "boards.greenhouse.io/<token> (default: gitlab)")
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--out", help="also append normalized rows here as JSONL")
    a = ap.parse_args()

    s = cc.session()
    url = API.format(a.board)
    print(f"GET {url}\n")
    data = cc.get_json(s, url)

    # Terminal statuses come back as a status dict rather than raising -- a dead
    # board must not take down a run that is crawling thousands of them.
    if not isinstance(data, dict) or "__status__" in data:
        code = data.get("__status__") if isinstance(data, dict) else "?"
        print(f"board '{a.board}' returned {code} — it may be retired or renamed")
        return 1

    jobs = data.get("jobs") or []
    print(f"{len(jobs):,} live postings on '{a.board}'; "
          f"showing {min(a.limit, len(jobs))}\n")

    w = cc.Writer(a.out) if a.out else None
    for j in jobs[:a.limit]:
        loc = (j.get("location") or {}).get("name") or ""
        r = cc.row(
            source="greenhouse", source_type="ats",
            company=a.board, title=j.get("title") or "",
            employment_type=cc.classify_any("", j.get("title") or ""),
            location=loc, posted=(j.get("updated_at") or "")[:10],
            apply_url=j.get("absolute_url") or "",
            job_id=str(j.get("id") or ""), board=a.board,
            board_url=f"https://boards.greenhouse.io/{a.board}",
        )
        print(f"  {r['title'][:52]:<52}  {r['location'][:26]:<26}  {r['posted']}")
        if w:
            w.write(r)
    if w:
        w.close()
        print(f"\nwrote normalized rows -> {a.out}")

    print("\nevery crawler in this repo emits exactly this dict; that contract "
          "is what makes\none dedupe pass over 40+ platforms possible.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
