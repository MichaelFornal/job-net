#!/usr/bin/env python3
"""
ukg_discover_boards.py — rebuild the UKG board list from the corpus.

UKG Pro board identity is a TRIPLE, and all three parts are printed into every
public posting URL:

    https://{HOST}/{TENANT}/JobBoard/{boardUuid}/OpportunityDetail?opportunityId={GUID}
            ^pod or vanity  ^customer code   ^board

There is no directory endpoint and no slug space to brute-force — the tenant
code (RUB1004RUGH, MOR1018MTHC, DEA1005DEAC) is an opaque customer id and the
board is a GUID, so neither can be guessed.  The corpus is therefore the
discovery surface: all_jobs_master.csv already carries UKG apply/job URLs that
other pipes (aggregators, Getro, job-search portals) collected, and every one
of them hands over a complete triple plus the employer's own name.

Two products share these hosts and only one is in scope:

    UKG Pro Recruiting   recruiting{,2}.ultipro.com, *.rec.pro.ukg.net
                         -> tenants/ukg.csv, fetched by ukg_fetch.py
    UKG Ready            secure*.saashr.com/ta/{cid}.careers?ShowJob={id}
                         -> recorded in ukg_candidates.csv with in_scope=0.
                         Different product, different route: the careers page
                         is a React SPA whose listing call was not found on the
                         public surface (its /ta/rest/ui/* tree answers JSON
                         404 "Unresolvable URL" for every careers path tried,
                         and the bundles contain no listing route).  Recorded
                         so the census is honest, not fetched.

Case: the tenant code is case-INSENSITIVE at the endpoint (verified against
recruiting2/abo1000adobs vs ABO1000ADOBS — byte-identical responses), but the
corpus contains both cases for the same board, so codes are folded to upper
case before the triple is deduped or two spellings become two boards.

    python3 ukg_discover_boards.py            # rebuild both CSVs
    python3 ukg_discover_boards.py --verify   # + probe every board once

Writes: tenants/ukg.csv, ukg_candidates.csv
"""
import argparse
import collections
import csv
import os
import re
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
MASTER = os.path.join(BASE, "all_jobs_master.csv")
TENANTS = os.path.join(BASE, "tenants", "ukg.csv")
CANDIDATES = os.path.join(BASE, "ukg_candidates.csv")

# The host must be matched, not merely contained: "ultipro" also appears inside
# aggregator tracking URLs that wrap a UKG link, and those carry a redirect
# hop, not the employer's own posting.  Anchoring on scheme+host means only a
# first-party UKG URL can produce a board.
RX_PRO = re.compile(
    r"https?://([A-Za-z0-9._-]*(?:ultipro\.com|rec\.pro\.ukg\.net))"
    r"/([^/?#]+)/JobBoard/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", re.I)
RX_READY = re.compile(
    r"https?://([A-Za-z0-9._-]*saashr\.com)/ta/([0-9]+)\.", re.I)


def mine(master=MASTER):
    """-> (pro, ready).  pro[(host, TENANT, uuid)] = Counter(company)."""
    csv.field_size_limit(10 ** 9)
    pro = collections.defaultdict(collections.Counter)
    ready = collections.defaultdict(collections.Counter)
    n = 0
    with open(master, newline="", encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            n += 1
            blob = (r.get("apply_url") or "") + " " + (r.get("job_url") or "")
            if ("ultipro" not in blob and "ukg.net" not in blob
                    and "saashr" not in blob):
                continue
            co = (r.get("company") or "").strip()
            for m in RX_PRO.finditer(blob):
                pro[(m.group(1).lower(), m.group(2).upper(),
                     m.group(3).lower())][co] += 1
            for m in RX_READY.finditer(blob):
                ready[(m.group(1).lower(), m.group(2))][co] += 1
    print("scanned %d master rows" % n, flush=True)
    return pro, ready


def _best(counter):
    """The most-seen company name for a board.  A board can carry several
    spellings (the employer's own, plus whatever an aggregator called it); the
    modal one is the employer's, because the employer-direct pipes outnumber
    any single republisher on a given board."""
    for name, _n in counter.most_common():
        if name:
            return name
    return ""


def write(pro, ready):
    os.makedirs(os.path.join(BASE, "tenants"), exist_ok=True)
    with open(TENANTS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["host", "tenant", "board_uuid", "company", "n_rows"])
        for k in sorted(pro):
            w.writerow([k[0], k[1], k[2], _best(pro[k]), sum(pro[k].values())])
    with open(CANDIDATES, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product", "host", "tenant_or_cid", "board_uuid",
                    "company", "n_rows_in_master", "in_scope"])
        for k in sorted(pro):
            w.writerow(["ukg_pro", k[0], k[1], k[2], _best(pro[k]),
                        sum(pro[k].values()), 1])
        for k in sorted(ready):
            w.writerow(["ukg_ready", k[0], k[1], "", _best(ready[k]),
                        sum(ready[k].values()), 0])
    tenants = {k[1] for k in pro}
    print("tenants/ukg.csv: %d boards / %d tenants / %d companies"
          % (len(pro), len(tenants), len({_best(v) for v in pro.values()})))
    print("ukg_candidates.csv: %d rows (%d ukg_pro in scope, %d ukg_ready out "
          "of scope)" % (len(pro) + len(ready), len(pro), len(ready)))


def verify():
    """Probe every board once through ukg_fetch's own validated request path,
    so 'discovered' and 'reachable' are never confused for each other."""
    import requests
    import ukg_fetch as U
    rows = list(csv.DictReader(open(TENANTS, newline="", encoding="utf-8")))
    s = requests.Session()
    s.headers.update({"User-Agent": U.cc.UA, "Content-Type": "application/json",
                      "Accept": "application/json"})
    ok = dead = err = 0
    for r in rows:
        host, tenant, uuid = r["host"], r["tenant"], r["board_uuid"]
        try:
            d = U.search(s, host, tenant, uuid, 0)
            ok += 1
            print("  LIVE  %-34s %-14s %d" % (host, tenant, d["totalCount"]))
        except U.Dead as e:
            dead += 1
            print("  DEAD  %-34s %-14s %s" % (host, tenant, e.reason))
        except U.Transient as e:
            err += 1
            print("  ERR   %-34s %-14s %s" % (host, tenant, e))
    print("verify: %d live, %d dead, %d transient of %d"
          % (ok, dead, err, len(rows)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=MASTER)
    ap.add_argument("--verify", action="store_true")
    a = ap.parse_args()
    pro, ready = mine(a.master)
    write(pro, ready)
    if a.verify:
        verify()
    return 0


if __name__ == "__main__":
    sys.exit(main())
