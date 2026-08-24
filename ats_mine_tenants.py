#!/usr/bin/env python3
"""
ats_mine_tenants.py — extract per-family tenant/board ids for the ATS families
that have no fetcher yet, by parsing apply/job URLs out of all_jobs_master.csv.

These are boards Getro (and the other piped sources) ALREADY syndicate, so the
list is a floor on each platform's real tenant population, not a census — but
it is a floor that costs one pass over a file we already have, and it is the
only channel that works for platforms whose job URLs carry no discoverable
subdomain.

Writes tenants/<family>.csv with columns: tenant,example_url,n_rows

Ranking note (measured 2026-07-28, see the unpiped-ats-census memory): rank
families by DISTINCT TENANTS, never by URL-occurrence count.  Eightfold shows
53,094 URL hits across just 10 tenants and Oracle 123k across 132 — both are
enterprise-concentrated, and ranking on hits inverts the build queue.
"""
import csv
import os
import re
import sys
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")
OUT_DIR = os.path.join(BASE, "tenants")

# family -> compiled regex whose group(1) is the tenant id.
# Every pattern is anchored on the HOST where possible: matching on a path
# fragment alone picks up unrelated hosts that happen to share a path shape.
PATTERNS = {
    # {tenant}.wd{N}.myworkdayjobs.com/{...}/{site}/...  — the site segment is
    # captured separately because the CXS endpoint needs BOTH.
    # Named groups because the two Workday host shapes put tenant/pod/site in
    # a DIFFERENT order and both feed one tenants/workday.csv.
    "workday": re.compile(
        r"https?://(?P<tenant>[a-z0-9-]+)\.(?P<pod>wd\d+)\.myworkdayjobs\.com/"
        r"(?:[a-z]{2}-[A-Z]{2}/)?(?P<site>[^/?#]+)", re.I),
    # Workday's second host, and the reason it was invisible: here the pod is
    # the leftmost LABEL and the tenant is a PATH segment, so the pattern
    # above -- which reads the leftmost label as the tenant -- matched nothing
    # at all and 3,756 rows across 34 tenants never entered any census.
    "workday_site": re.compile(
        r"https?://(?P<pod>(?:impl-)?wd\d+)\.myworkdaysite\.com/recruiting/"
        r"(?P<tenant>[a-z0-9-]+)/(?P<site>[^/?#]+)", re.I),
    "icims": re.compile(r"https?://([a-z0-9-]+)\.icims\.com", re.I),
    # ADP carries the tenant in a cid= query param, not the host.
    "adp": re.compile(r"workforcenow\.adp\.com/.*?[?&]cid=([0-9a-f-]{8,})", re.I),
    # Paycom's portal key is a 32-hex blob in the path.
    "paycom": re.compile(r"paycomonline\.net/v4/ats/web\.php/jobs?"
                         r"(?:/[^/?#]*)?\?.*?clientkey=([0-9A-F]{16,})", re.I),
    "paycom_path": re.compile(r"paycomonline\.net/.*?/portal/([0-9A-F]{16,})",
                              re.I),
    # comeet.com AND comeet.co both appear in the wild.
    "comeet": re.compile(r"https?://(?:www\.)?comeet\.co(?:m)?/jobs/"
                         r"([a-z0-9._-]+)/", re.I),
    # Oracle: the tenant is the LEFTMOST host label, not the whole host.
    "oracle": re.compile(r"https?://([a-z0-9-]+)\.fa\.[a-z0-9-]+\."
                         r"oraclecloud\.com", re.I),
    "jobvite": re.compile(r"https?://jobs\.jobvite\.com/([a-z0-9-]+)", re.I),
    "pinpoint": re.compile(r"https?://([a-z0-9-]+)\.pinpointhq\.com", re.I),
    "keka": re.compile(r"https?://([a-z0-9-]+)\.keka\.com", re.I),
    "taleo": re.compile(r"https?://([a-z0-9-]+)\.taleo\.net", re.I),
    "join": re.compile(r"https?://join\.com/companies/([a-z0-9-]+)", re.I),
    # Zoho runs eight regional data centres and the tenant slug is NOT unique
    # across them, so the captured id is the FULL HOST — the .com-only pattern
    # this replaced made every .in/.eu/.com.au/.sa board invisible to the
    # miner and held the seed list at 30 against 260 hosts in the corpus.
    # com.au must precede com in the alternation or it truncates to ".com".
    "zohorecruit": re.compile(
        r"https?://([a-z0-9-]+\.zohorecruit\."
        r"(?:com\.au|com|eu|in|jp|ca|sa|uk))", re.I),
    "successfactors": re.compile(
        r"https?://([a-z0-9-]+)\.(?:jobs\.)?successfactors\.(?:com|eu)", re.I),
    "gem": re.compile(r"https?://jobs\.gem\.com/([a-z0-9-]+)", re.I),
    "wttj": re.compile(r"welcometothejungle\.com/[a-z]{2}/companies/"
                       r"([a-z0-9-]+)", re.I),
    "eightfold": re.compile(r"https?://([a-z0-9-]+)\.eightfold\.ai", re.I),
    "phenom_cx": re.compile(r"https?://([a-z0-9-]+)\.phenompeople\.com", re.I),
}

# Hosts that are the VENDOR's own marketing/demo tenant rather than a customer.
SKIP = {"www", "jobs", "careers", "apply", "api", "app", "my", "secure",
        "eu", "us", "demo", "test", "example", "sandbox"}


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else SRC
    if not os.path.exists(src):
        raise SystemExit(f"missing {src}")
    os.makedirs(OUT_DIR, exist_ok=True)
    found = {fam: defaultdict(lambda: [0, ""]) for fam in PATTERNS}
    wd_sites = defaultdict(lambda: defaultdict(int))

    kept_n = {}
    n = 0
    with open(src, newline="", encoding="utf-8", errors="replace") as f:
        rd = csv.reader(f)
        head = next(rd)
        idx = [i for i, c in enumerate(head)
               if c in ("apply_url", "job_url")]
        for row in rd:
            n += 1
            for i in idx:
                if i >= len(row):
                    continue
                u = row[i]
                if not u or "://" not in u:
                    continue
                for fam, rx in PATTERNS.items():
                    m = rx.search(u)
                    if not m:
                        continue
                    gd = m.groupdict()
                    tenant = (gd.get("tenant") or m.group(1)).lower()
                    # SKIP names a leftmost host label.  A family whose id is
                    # a FULL host (zohorecruit, which needs the data centre)
                    # has to be tested on that label rather than on the whole
                    # string, or "www.zohorecruit.com" sails straight through.
                    if tenant.split(".")[0] in SKIP or len(tenant) < 2:
                        continue
                    # Both workday patterns write into ONE family so the
                    # tenant list stays a single file with a single schema.
                    key = "workday" if fam.startswith("workday") else fam
                    rec = found[key][tenant]
                    rec[0] += 1
                    if not rec[1]:
                        rec[1] = u
                    if key == "workday":
                        dom = ("myworkdaysite.com" if fam == "workday_site"
                               else "myworkdayjobs.com")
                        wd_sites[tenant][(gd["pod"].lower(), gd["site"],
                                          dom)] += 1
            if n % 200000 == 0:
                print(f"  scanned {n:,} rows", flush=True)

    print(f"scanned {n:,} rows of {os.path.basename(src)}\n", flush=True)
    summary = []
    for fam, d in found.items():
        if not d:
            continue
        path = os.path.join(OUT_DIR, f"{fam}.csv")
        # PRESERVE ROWS THIS SCRIPT DID NOT PRODUCE.  It is the "sole producer"
        # of tenants/*.csv only for the corpus-mining CHANNEL; workday now also
        # has a Wayback/CXS channel (workday_discover_boards.py), and on
        # 2026-08-20 this function silently deleted 3,293 probed-live Workday
        # boards it had merged 4 minutes earlier -- run_all's prelude runs
        # between discovery and the fetch, so the fetch then ran against 1,869
        # boards instead of 5,176.  A row is kept when its `src` says another
        # channel found it.
        keep = []
        if os.path.exists(path):
            with open(path, newline="", encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    if (r.get("src") or "corpus") != "corpus":
                        keep.append(r)
        with open(path, "w", newline="", encoding="utf-8") as out:
            w = csv.writer(out)
            mine = set()
            if fam == "workday":
                # wd_domain is APPENDED, so every existing reader that
                # asks for tenant/wd_host/site by name keeps working.
                w.writerow(["tenant", "wd_host", "site", "n_rows",
                            "example_url", "wd_domain", "src"])
                for t, (cnt, ex) in sorted(d.items(),
                                           key=lambda kv: -kv[1][0]):
                    # One tenant can host several career SITES (External,
                    # Campus, ...); each needs its own CXS call, so emit them
                    # all rather than collapsing to the most common one.
                    for (wd, site, dom), c in sorted(wd_sites[t].items(),
                                                      key=lambda kv: -kv[1]):
                        mine.add((t, wd, site, dom))
                        w.writerow([t, wd, site, c, ex, dom, "corpus"])
                for r in keep:
                    k = (r.get("tenant", ""), r.get("wd_host", ""),
                         r.get("site", ""),
                         r.get("wd_domain") or "myworkdayjobs.com")
                    if k in mine:
                        continue          # corpus found it too; one row is enough
                    w.writerow([r.get("tenant", ""), r.get("wd_host", ""),
                                r.get("site", ""), r.get("n_rows", ""),
                                r.get("example_url", ""),
                                r.get("wd_domain", ""), r.get("src", "")])
                kept_n[fam] = len(keep)
            else:
                w.writerow(["tenant", "n_rows", "example_url", "src"])
                seen_t = set()
                for t, (cnt, ex) in sorted(d.items(),
                                           key=lambda kv: -kv[1][0]):
                    seen_t.add(t)
                    w.writerow([t, cnt, ex, "corpus"])
                for r in keep:
                    if r.get("tenant") in seen_t:
                        continue
                    w.writerow([r.get("tenant", ""), r.get("n_rows", ""),
                                r.get("example_url", ""), r.get("src", "")])
                kept_n[fam] = len(keep)
        rows = sum(v[0] for v in d.values())
        summary.append((len(d), rows, fam, path))

    if any(kept_n.values()):
        print("  preserved from other discovery channels: "
              + ", ".join(f"{k}={v}" for k, v in sorted(kept_n.items()) if v))
    print(f"{'family':18}{'tenants':>9}{'url hits':>11}")
    for ntenant, rows, fam, path in sorted(summary, reverse=True):
        print(f"{fam:18}{ntenant:9d}{rows:11d}   -> {os.path.relpath(path)}")


if __name__ == "__main__":
    main()
