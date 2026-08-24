#!/usr/bin/env python3
"""
state_refresh.py — drop the DONE keys from a fetch state, keep the DEAD ones.

  python3 state_refresh.py [--dry-run] <state.json> [<state.json> ...]

Why this exists.  Most of the tail fetchers (oracle, icims, adp, paycom,
workday) have no refresh flag: `work()` skips any tenant whose state says
"done", so re-running one is nearly a no-op and its rows stay as old as the day
they were fetched.  The only way to refresh those tenants is to forget they
were done.

But deleting the whole state file forgets two different things at once.  The
"done" markers are cheap to rebuild — that is just re-fetching.  The dead
markers are not: they are a census that cost real time to learn, and each one
encodes a platform-specific discrimination that is easy to get wrong
(workday's 403-S22 closed board vs a 403 from the edge; comeet's 302 to the
vendor marketing site; keka's /TenantNotFound.html; zohorecruit's 403
ORG_NOT_FOUND being terminal while its 404 means the org is ALIVE under a
different portal name).  Throwing those away means re-probing every corpse and
re-deriving those distinctions on the next run.

So: keep every key whose value looks dead, drop every key that looks done.
The fetcher then re-fetches exactly the live tenants and skips the corpses.

Duplicate rows are not a concern here.  Every one of these writes through
contract_common.Writer in append mode, and bh_build_combined.py collapses on
(source, board, job_id) keeping max(posted) — so a re-fetched posting merges
with its older copy instead of doubling.  The cost is disk and build time, not
correctness.
"""

import json
import os
import shutil
import sys
from collections import Counter

# Same list ats_fetch/coverage_report use.  A value that starts with any of
# these is a corpse or a permanent refusal and is worth remembering.
DEAD = ("dead", "err", "retired", "not_a_board", "aggregator_drop",
        "blocked", "migrated", "unsupported", "error")


def looks_dead(v):
    s = (v.get("status") if isinstance(v, dict) else v)
    s = str(s or "").lower()
    return any(s.startswith(m) for m in DEAD)


def main_paths(files, dry=False, scopes=None):
    """Programmatic entry point, so run_all can call this in-process.

    Kept separate from main() so the CLI keeps its exact behaviour and the
    caller does not have to fake sys.argv.

    scopes maps a path to a tuple of key prefixes.  A scoped file has ONLY the
    keys under those prefixes considered for re-fetch; every other key is kept
    verbatim, whatever its status.  This exists for atstail_fetch_state.json,
    which is keyed "{family}:{tenant}" across six families: five of them have
    no refresh path at all and sat frozen, while the sixth (zohorecruit) is
    already refreshed by ATSTAIL_REFRESH and must not be touched from here.
    Without scoping the choice is "unfreeze five" or "leave all six" — this
    makes it neither.
    """
    scopes = scopes or {}
    for p in files:
        scope = scopes.get(p)
        if not os.path.exists(p):
            print(f"{os.path.basename(p):36} MISSING — skipped")
            continue
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            # Never rewrite a file we could not read: an unreadable state loads
            # as {} downstream, and "rewriting" it would make that permanent.
            print(f"{os.path.basename(p):36} PARSE FAIL ({e}) — LEFT ALONE")
            continue
        if not isinstance(d, dict):
            print(f"{os.path.basename(p):36} not a dict — LEFT ALONE")
            continue

        if scope:
            keep = {k: v for k, v in d.items()
                    if not k.startswith(scope) or looks_dead(v)}
        else:
            keep = {k: v for k, v in d.items() if looks_dead(v)}
        drop = len(d) - len(keep)
        # Count only the genuinely dead.  Under a scope `keep` also holds
        # every out-of-scope key regardless of status, and folding those in
        # would report live tenants as corpses.
        kinds = Counter(str(v.get("status") if isinstance(v, dict) else v)
                        .split(":")[0] for v in keep.values() if looks_dead(v))
        tag = f"[{','.join(scope)}] " if scope else ""
        print(f"{os.path.basename(p):36} {tag}{len(d):>6} keys -> "
              f"re-fetch {drop:>6}, keep {len(keep):>5} "
              f"dead {dict(kinds) if kinds else ''}")
        if dry:
            continue
        bak = p + ".full"
        if not os.path.exists(bak):
            shutil.copy2(p, bak)          # one pristine copy, never overwritten
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(keep, f)
        os.replace(tmp, p)                # atomic: a torn state reads as {}


def main():
    dry = "--dry-run" in sys.argv
    files = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not files:
        sys.exit(__doc__)
    main_paths(files, dry=dry)


if __name__ == "__main__":
    main()
