#!/usr/bin/env python3
"""
host_census.py — open-ended apply-URL census over all_jobs_master.csv.

  python3 host_census.py [master.csv] [--all] [--min N] [--top N]
                         [--out host_census_<date>.csv]

WHY THIS EXISTS.  ats_mine_tenants.py answers "how many tenants does family X
have?" for a CLOSED list of 19 regexes, so a platform nobody has written a
regex for is invisible to it — and therefore invisible to the build queue, to
coverage_report.py and to every ranking either of them feeds.  That is not a
hypothetical: on 2026-08-20 the residue this script prints held UKG (366
boards), Manatal/careers-page.com (562), NEOGOV (156), Dayforce (120) and
Cornerstone (63), none of which had ever appeared in a census, while a month of
"un-piped ATS" ranking work went past them.  This script has no allow-list of
things to look for.  It reports what is THERE and subtracts what is piped, so
the unknown tail is the output rather than the blind spot.

RANK BY BOARDS, NOT BY HITS.  The 2026-07-28 census recorded this the hard way:
Eightfold showed 53,094 URL hits across 10 tenants and Oracle 123k across 132 —
ranking on occurrences inverts the queue and sends you to build the platform
with the fewest employers.  So the default sort is `boards` (see below) and
`rows` is printed beside it as context, never as the ordering.

WHAT "BOARDS" MEANS HERE.  A tenant lives in one of two places and the census
cannot know which in advance:
  host  — {tenant}.bamboohr.com, {tenant}.csod.com, {tenant}.gupy.io
  path  — www.careers-page.com/{tenant}/, governmentjobs.com/careers/{tenant}
Counting only distinct hosts scores every path-tenant platform as "1 board" and
buries it; counting only path segments scores host-tenant platforms as noise.
So both are counted and both are reported -- but `boards` is neither of them.  It is the count of
DISTINCT EMPLOYERS on the apex, taken from the master's own company column,
because both url parses mis-fire in opposite directions and the company column
does not.  Manatal reads 39 hosts / 397 path segments / 397 employers;
unitedhealthgroup.com reads 0 hosts / 1,075 path segments / ONE employer,
because a Radancy url is /job/{city}/{title}/{id} and the segment after the
noise word is a city, not a tenant.
"""
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import date
from urllib.parse import urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")

# Apexes reached by a pipe that exists TODAY.  This is the only hand-maintained
# list in the file and it is a subtraction, not a search: getting it wrong makes
# the report noisier, never blinder, which is the opposite of the failure mode
# ats_mine_tenants.PATTERNS has.  Add an apex here when its fetcher lands.
PIPED = {
    # employer ATS with a fetcher
    "myworkdayjobs.com", "myworkdaysite.com", "oraclecloud.com",
    "smartrecruiters.com", "greenhouse.io", "lever.co", "ashbyhq.com",
    "applytojob.com", "bamboohr.com", "breezy.hr", "recruitee.com",
    "teamtailor.com", "workable.com", "personio.de", "personio.com",
    "icims.com", "rippling.com", "rippling-ats.com", "eightfold.ai",
    "jobvite.com", "gem.com", "comeet.com", "comeet.co", "zohorecruit.com",
    "pinpointhq.com", "keka.com", "join.com", "taleo.net", "adp.com",
    "paycomonline.net", "successfactors.com", "sapsf.eu", "phenompeople.com",
    "notion.site", "dover.com", "bullhornstaffing.com",
    # 2026-08-20: piped as a direct result of this script's own first run.
    # Radancy has no single apex -- it is ~75 employer vanity domains -- so it
    # cannot be subtracted here; its rows stay in the residue and that is
    # correct, because the residue is where the NEXT batch of them will show up.
    "ultipro.com", "ukg.net", "careers-page.com", "governmentjobs.com",
    "dayforcehcm.com", "csod.com",
    # boards / aggregators / niche with a fetcher
    "welcometothejungle.com", "usajobs.gov", "adzuna.com", "jobviewtrack.com",
    "careerjet.com", "himalayas.app", "nofluffjobs.com", "arbeitsagentur.de",
    "wanted.co.kr", "instahyre.com", "torre.ai", "remote.io", "freelancer.com",
    "web3.career", "arbeitnow.com", "powertofly.com", "ycombinator.com",
    "jobgether.com", "themuse.com", "remoteleaf.com", "weworkremotely.com",
    "remoteok.com", "jobicy.com", "workingnomads.co", "justremote.co",
    "dynamitejobs.com", "getonbrd.com", "wantedly.com", "hubstafftalent.com",
    "thehub.io", "braintrust.com", "cryptocurrencyjobs.co", "remotive.com",
    "jobspresso.co", "authenticjobs.com", "larajobs.com", "elixirjobs.net",
    "berlinstartupjobs.com", "landing.jobs", "dribbble.com",
    "mindtheproduct.com", "python.org", "web3career.com",
}

# Leftmost-label noise: a vendor's own www/api host is not a customer board.
SKIP_LABEL = {"www", "jobs", "careers", "career", "apply", "api", "app", "my",
              "secure", "eu", "us", "demo", "test", "sandbox", "static",
              "cdn", "assets", "media", "img", "recruiting", "hire", "talent"}
# First-path-segment noise, for the path-tenant count.
SKIP_SEG = {"", "job", "jobs", "en", "en-us", "en_us", "us", "careers",
            "career", "search", "apply", "view", "detail", "details", "post",
            "posting", "postings", "opportunity", "vacancy", "vacancies",
            "global", "main", "index", "home", "j", "o", "p", "c", "i"}
_NUMERIC = re.compile(r"^[0-9]+$")
# Postings a path segment must carry before it counts as a board.
SEG_MIN = int(os.environ.get("HC_SEG_MIN") or 3)


def apex_of(host):
    """Registrable-ish apex.  Handles the two-label public suffixes that
    actually occur in this corpus (.co.uk, .com.au, .com.br, .co.kr ...);
    anything else collapses to the last two labels."""
    h = host.lower().split(":")[0].strip(".")
    parts = h.split(".")
    if len(parts) >= 3 and len(parts[-1]) == 2 and parts[-2] in (
            "co", "com", "org", "net", "ac", "gov", "edu"):
        return ".".join(parts[-3:])
    return ".".join(parts[-2:]) if len(parts) >= 2 else h


def main():
    # Skip flag VALUES as well as flags: a bare positional scan reads the "40"
    # of "--top 40" as the master path.
    valued = ("--top", "--min", "--out")
    args, skip = [], False
    for a in sys.argv[1:]:
        if skip:
            skip = False
            continue
        if a in valued:
            skip = True
            continue
        if a.startswith("--"):
            continue
        args.append(a)
    src = args[0] if args else SRC
    show_all = "--all" in sys.argv
    top = int(_val("--top") or 60)
    minrows = int(_val("--min") or 300)
    out = _val("--out") or os.path.join(
        BASE, f"host_census_{date.today().isoformat()}.csv")
    if not os.path.exists(src):
        raise SystemExit(f"missing {src}")

    rows = defaultdict(int)
    hosts = defaultdict(set)
    comps = defaultdict(set)
    segs = defaultdict(lambda: defaultdict(int))
    example = {}
    srcs = defaultdict(lambda: defaultdict(int))
    seen_piped = set()
    n = urls = 0

    with open(src, newline="", encoding="utf-8", errors="replace") as f:
        rd = csv.reader(f)
        head = next(rd)
        try:
            cols = [head.index("apply_url"), head.index("job_url")]
        except ValueError:
            raise SystemExit("master has no apply_url/job_url column")
        si = head.index("source") if "source" in head else None
        ci = head.index("company") if "company" in head else None
        for row in rd:
            n += 1
            # apply_url first, job_url only as a fallback: counting both
            # double-counts every row that carries the same board twice.
            u = ""
            for i in cols:
                if i < len(row) and row[i].startswith("http"):
                    u = row[i]
                    break
            if not u:
                continue
            urls += 1
            try:
                pr = urlparse(u)
            except ValueError:
                continue
            host = pr.netloc.lower()
            if not host:
                continue
            a = apex_of(host)
            if a in PIPED:
                seen_piped.add(a)
                if not show_all:
                    continue
            rows[a] += 1
            label = host.split(".")[0]
            if label not in SKIP_LABEL and len(hosts[a]) < 200000:
                hosts[a].add(host)
            # First NON-NOISE segment, not simply the first.  NEOGOV is
            # /careers/{agency} and Dayforce is /en-US/{client}/{site}, so a
            # segment-0 reading scores both as a single board and drops them
            # off the queue entirely.
            for seg in pr.path.lstrip("/").lower().split("/")[:3]:
                if (seg and seg not in SKIP_SEG and not _NUMERIC.match(seg)
                        and 1 < len(seg) < 64):
                    if len(segs[a]) < 200000 or seg in segs[a]:
                        segs[a][seg] += 1
                    break
            if ci is not None and ci < len(row) and row[ci]:
                if len(comps[a]) < 200000:
                    comps[a].add(row[ci].strip().lower()[:80])
            example.setdefault(a, u[:160])
            if si is not None and si < len(row):
                srcs[a][row[si]] += 1

    recs = []
    for a, nr in rows.items():
        nh = len(hosts[a])
        # A path tenant carries MULTIPLE postings; a job slug carries one.
        # Counting every distinct segment scored eiffage.com -- one French
        # employer whose urls end in a per-job slug -- as 1,723 boards, which
        # is precisely the "a domain word must never score as a job title"
        # failure the picks lane hit.  Requiring the segment to repeat is what
        # separates a board from a posting.
        ns = sum(1 for c in segs[a].values() if c >= SEG_MIN)
        nc = len(comps[a])
        by = "host" if nh >= ns else "path"
        # DISTINCT EMPLOYERS is the board proxy, not host or path count.  Both
        # of those are parses of a url and both mis-fire: unitedhealthgroup.com
        # reads as 1,075 "path boards" because a Radancy url is
        # /job/{city}/{title}/{id}, so the segment after the noise word is a
        # CITY.  The company column settles it in one line -- that apex is one
        # employer -- and it needs no per-vendor url knowledge to do so.
        recs.append(dict(apex=a, boards=nc or max(nh, ns), companies=nc,
                         by=by, hosts=nh, path_segs=ns, rows=nr,
                         top_source=max(srcs[a], key=srcs[a].get)
                         if srcs[a] else "",
                         example=example.get(a, "")))
    recs.sort(key=lambda r: (-r["boards"], -r["rows"]))

    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(recs[0]) if recs else
                           ["apex", "boards", "by", "hosts", "path_segs",
                            "rows", "top_source", "example"])
        w.writeheader()
        for r in recs:
            w.writerow(r)

    kept = [r for r in recs if r["rows"] >= minrows]
    print(f"host census over {n:,} rows ({urls:,} with a url)")
    print(f"{'residue' if not show_all else 'ALL apexes'}: "
          f"{len(recs):,} apexes, {sum(r['rows'] for r in recs):,} rows"
          + ("" if show_all else f" not reached by any pipe"))
    print(f"\nranked by BOARDS (>= {minrows} rows), top {top}:\n")
    print(f"{'boards':>7} {'hosts':>7} {'paths':>7} {'rows':>10}  "
          f"{'apex':<28} top_source")
    print("-" * 96)
    for r in kept[:top]:
        print(f"{r['boards']:>7,} {r['hosts']:>7,} "
              f"{r['path_segs']:>7,} {r['rows']:>10,}  {r['apex']:<28} "
              f"{r['top_source']}")

    # A PIPED entry that no longer appears is either a dead pipe or a stale
    # subtraction; either way it should not fail silently.
    missing = sorted(PIPED - seen_piped)
    if missing:
        print(f"\nNOTE {len(missing)} PIPED apex(es) contributed no url this "
              f"pass — dead pipe or stale entry:\n  " + ", ".join(missing))
    print(f"\nwrote {os.path.relpath(out, BASE)}  ({len(recs):,} rows)")


def _val(flag):
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None


if __name__ == "__main__":
    main()
