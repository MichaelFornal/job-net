#!/usr/bin/env python3
"""fixture.py — a synthetic main-feed CSV, so the tests and the demo run on a
clone that has never built the corpus.

WHY THIS EXISTS.  triage/selftest.py used to read row bodies out of the newest
`MA_US_Remote_*.csv`, which is the tail of a multi-hour pipeline run.  On a
fresh checkout that glob is empty and the suite died on an IndexError before
its first assertion — so the 200 checks that document this app's hardest
invariants could only ever be run by someone who already had the 28GB corpus.

The rows below are generated, not sampled, and that is the point: nothing here
came out of anyone's real job search.

WHAT IT HAS TO SPAN.  A flat random fill is not enough.  The suite's facet
tests need every facet populated on BOTH sides or they pass without ever
filtering anything:

  * bucket    -- all three, because the loc facet maps ma / remote / sfl onto
                 them and an empty bucket makes that facet a no-op
  * ai_band   -- banded AND unbanded, since the band drives the AI lanes
  * source    -- employer-direct AND aggregator, because is_agg is derived from
                 it and half the ordering terms key on it
  * us_basis  -- proven AND inferred, which is what sets basis_weak
  * posted    -- dated AND undated, recent AND old, for the freshness sorts

Deterministic on purpose: seeded, so a failure reproduces exactly and a golden
hash means something.  No network, no database, no dependencies.

    python3 triage/fixture.py [--rows N] [--out PATH]
"""
import argparse
import csv
import datetime as dt
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import triage_load as TL  # noqa: E402

HDR = TL.FIELDS + ["job_key"]

BUCKETS = ["MA local", "Remote", "South FL"]
DIRECT = ["greenhouse", "lever", "ashby", "workday", "recruitee", "smartrecruiters"]
AGG = ["himalayas", "jobgether", "careerjet", "adzuna", "jobicy"]

ROLE = ["Platform Engineer", "Backend Engineer", "Solutions Architect",
        "Data Engineer", "Site Reliability Engineer", "Analytics Engineer",
        "Technical Account Manager", "Customer Success Manager",
        "Implementation Consultant", "Automation Engineer",
        "Infrastructure Engineer", "Product Manager", "Support Engineer"]
RANK = ["", "Senior ", "Staff ", "Lead ", "Principal ", "Associate "]
SUFFIX = ["", ", Platform", " - Growth", " (Remote)", " II", " III"]
CITY = {"MA local": ["Boston, MA", "Cambridge, MA", "Somerville, MA",
                     "Newton, MA", "Waltham, MA", "Springfield, MA"],
        "Remote": ["Remote - US", "United States (Remote)", "Anywhere, US"],
        "South FL": ["Miami, FL", "Fort Lauderdale, FL", "Boca Raton, FL"]}
SKILL = ["python, sql, airflow", "kubernetes, terraform, aws",
         "react, typescript", "kafka, spark, scala", "salesforce, zapier"]
INDUSTRY = ["Software", "Fintech", "Healthcare", "Logistics", "Retail"]

# ~1 row in 12 is a staffing desk rather than an employer.  Without these the
# agency lane's builder scores nothing and -- correctly -- refuses to blank the
# lane rather than write an empty one, which would fail `make demo`.  A fixture
# has to carry every population its consumers look for, not just the common one.
AGENCY_CO = ["Beacon Staffing", "Northpoint Talent Partners",
             "Vertex Recruiting Group", "Harbor Technical Staffing",
             "Kestrel Search Partners"]
AGENCY_INDUSTRY = "Staffing and Recruiting"


# ~5,000 distinct company names.  The size matters: decisions carry forward on
# ct_key = company|title, so a small name pool makes synthetic rows collide on
# that key and the carry-forward tests measure the collision instead of the
# behaviour.  Real corpus rows are near-unique on the pair; the fixture has to
# be too.
_HEAD = ["Northwind", "Contoso", "Umbrella", "Initech", "Globex", "Hooli",
         "Acme", "Vandelay", "Soylent", "Cyberdyne", "Tyrell", "Aperture",
         "Blackmesa", "Wonka", "Stark", "Wayne", "Oscorp", "Abstergo",
         "Weyland", "Massive", "Sirius", "Encom", "Gringotts", "Duff",
         "Bluth", "Prestige", "Sterling", "Pierce", "Dunder", "Vehement",
         "Pendant", "Rekall", "Omni", "Nakatomi", "Vault", "Zorg",
         "Multiverse", "Paper", "Anvil", "Ridgeline"]
_MID = ["Labs", "Systems", "Group", "Technologies", "Works", "Dynamics",
        "Analytics", "Robotics", "Networks", "Digital", "Partners", "Studio",
        "Collective", "Industries", "Holdings", "Ventures", "Solutions",
        "Interactive", "Logistics", "Materials", "Automation", "Research",
        "Instruments", "Media", "Capital"]
_TAIL = ["", " Inc", " LLC", " Co", " PBC"]


def _company(i):
    return (f"{_HEAD[i % len(_HEAD)]} "
            f"{_MID[(i // len(_HEAD)) % len(_MID)]}"
            f"{_TAIL[(i // (len(_HEAD) * len(_MID))) % len(_TAIL)]}")


def rows(n=4000, seed=7):
    """n synthetic rows, every facet populated on both sides."""
    random.seed(seed)
    today = dt.date(2026, 8, 24)
    out = []
    for i in range(n):
        # DRAWN, not cycled.  An earlier version assigned facets on i % 3,
        # i % 4, i % 5 -- and the suite samples the file on i % 90, i % 12 and
        # i % 3 strides.  Those alias: every 90th row is also every 3rd row, so
        # the sampled set came back 100% one bucket and the facet tests
        # filtered nothing while reporting success.  Drawing from the seeded
        # RNG keeps the run reproducible and decorrelates the facets from any
        # stride the consumer picks.
        bucket = random.choice(BUCKETS)
        agg = random.random() < 0.34
        source = random.choice(AGG if agg else DIRECT)
        band = "" if random.random() < 0.25 else f"{random.randint(1, 5)} Core AI Building"
        posted = "" if random.random() < 0.14 else str(
            today - dt.timedelta(days=random.randint(0, 119)))
        basis = ((TL.WEAK_BASIS + " (company/USD)") if random.random() < 0.20
                 else "proven (US city)")
        title = f"{random.choice(RANK)}{random.choice(ROLE)}{random.choice(SUFFIX)}"
        agency = random.random() < 0.08
        comp = random.choice(AGENCY_CO) if agency else _company(i)
        smin = random.choice(["", "90000", "120000", "150000"])
        rec = {
            "bucket": bucket, "us_basis": basis,
            "startup_tier": random.choice(["", "1 seed", "2 series A", "3 growth", "4 late"]),
            "startup_evidence": random.choice(["", "crunchbase", "board size"]),
            "ai_band": band, "ai_tier": "Core AI Building" if band else "",
            "company": comp, "title": title,
            "employment_type": random.choice(["Full-time", "Contract"]),
            "seniority": "", "work_mode": "Remote" if bucket == "Remote" else "On-site",
            "remote": "true" if bucket == "Remote" else "false",
            "location": random.choice(CITY[bucket]), "posted": posted,
            "salary_min": smin, "salary_max": str(int(smin) + 30000) if smin else "",
            "salary_currency": "USD" if smin else "", "salary_period": "year" if smin else "",
            "skills": random.choice(SKILL), "company_stage": "",
            "company_size": random.choice(["11-50", "51-200", "201-500"]),
            "company_industry": AGENCY_INDUSTRY if agency else random.choice(INDUSTRY),
            "apply_url": f"https://{source}.example/jobs/{i}",
            "job_url": f"https://{source}.example/jobs/{i}",
            "source": source,
            # Shaped like a real canonical key -- `ats|platform|org|id`, what
            # all_jobs_build.canon_key() emits.  An opaque key looks fine until
            # a test filters on the prefix or splits out the id, and then it
            # silently selects nothing.
            "job_key": f"ats|{source}|{comp.lower().replace(' ', '-')}|{i}",
        }
        out.append([rec[c] for c in HDR])
    return out


def write(path, n=4000, seed=7):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HDR)
        w.writerows(rows(n, seed))
    return path


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rows", type=int, default=4000)
    # Default lives in fixtures/, NOT the repo root, and that placement is
    # load-bearing: triage/selftest.py globs the root for MA_US_Remote_*.csv to
    # decide whether a REAL corpus cut is available, and a generated file
    # sitting there is indistinguishable from one.  Left in the root, the demo
    # silently switched the suite out of fixture mode and six corpus-shaped
    # assertions ran against synthetic data and failed.
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "fixtures", "MA_US_Remote_demo.csv"))
    a = ap.parse_args()
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    p = write(a.out, a.rows)
    print(f"wrote {a.rows:,} synthetic rows -> {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
