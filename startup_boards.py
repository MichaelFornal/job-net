#!/usr/bin/env python3
"""
startup_boards.py — split the Getro board list into VC / ecosystem / junk.

`all_jobs_master.csv` carries a `boards` column whose unprefixed entries are
Getro board names (prefixed ones like `workday:acme` name another source's
board). Those Getro boards are not one population: a16z and Lux Capital sit in
the same column as chambers of commerce, county economic-development
corporations, university career fairs and a data vendor's marketing board. Only
the first kind is evidence that a company is a venture-backed startup.

Board names alone cannot make the call — 8VC, Antler, Atomic, DCVC and Costanoa
carry no "ventures/capital" token, while `Space Capital | Talent` is a talent
network. So each board is classified from how its own companies actually look:

  funded_share  companies with a named funding round, of those with any stage
  small_share   companies at head_count <= 4 (~<=500 staff), of those with any
  big_share     companies at head_count == 6 (1000+ staff), of those with any

A venture board and a chamber of commerce do not resemble each other on those
three numbers. A name prior and a thin allow/blocklist settle the edges.

Every verdict, its inputs and its reason are written to startup_boards.csv so
the call is auditable and can be corrected by hand; startup_build.py reads that
file if it exists and only recomputes when asked.

  python3 startup_boards.py              # write startup_boards.csv
  python3 startup_boards.py --show vc    # print one class and exit
"""

import csv
import os
import re
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")
BOARDS = os.path.join(BASE, "getro_job_boards.csv")
OUT = os.path.join(BASE, "startup_boards.csv")

csv.field_size_limit(10**9)

# Named rounds only. "Pre Seed" is Getro's junk default — 46k of its 62k rows
# are 1000+ headcount employers (Walgreens, AWS, JPMorganChase, Mayo Clinic) —
# so it is not evidence of anything on its own and is excluded here.
FUNDED = {"Seed", "Series A", "Series B", "Series C", "Series D", "Series E",
          "Series F", "Series G", "Series H", "Series I", "Series Unknown",
          "Series C Plus", "Early", "Growth", "Undisclosed", "Ico",
          "Unknown Stage"}
LATE = {"Ipo", "Acquisition", "Private Equity"}

# ---- name priors -----------------------------------------------------------
VC_NAME = re.compile(r"""(?ix)
    \b(ventures?|capital|partners|fund|funds|vc|vcs|accelerator|accelerators|
       incubator|equity|invest|investors?|investments?|holdings|
       venture\s+studio|studio)\b
""")

# Boards whose name alone marks them as a place-based, institutional or
# career-fair roster rather than a venture portfolio.
JUNK_NAME = re.compile(r"""(?ix)
    chamber\s+of\s+commerce | economic\s+development | \bEDC\b | \bEDA\b |
    development\s+(authority|corporation|alliance|foundation|council) |
    \bcounty\b | \bcity\s+of\b | \bregion(al)?\b | \bmunicipal | \btownship\b |
    workforce | career\s+fair | job\s+fair | \bjob\s+board\b | job\s+seekers |
    \bmarketplace\b | \bstaffing\b | \brecruit(ing|ment)\s+agency |
    \buniversity\b | \bcollege\b | \bschools?\b | \balumni\s+association\b |
    community\s+(college|development|foundation) | \bchamber\b |
    destination\s+\w+ | discover\s+\w+ | choose\s+\w+ | this\s+is\s+\w+ |
    \bvisit\s+\w+ | \bgreater\s+\w+\s+(chamber|partnership|council)
""")

# The edges the measured signals and the name priors both get wrong.
# Venture firms, accelerators and studios whose names carry no VC token.
VC_ALLOW = {
    "8VC", "Antler", "Atomic", "DCVC", "Costanoa", "AlleyCorp", "Airtree",
    "Atomico", "Blackbird", "Base10", "Basis Set", "Daphni", "Astanor",
    "AENU", "Emerge", "Archetype", "Amplify", "Ascend", "Concorde",
    "Dyrdek Machine", "Entrepreneurs First", "FORWARD.one", "Colorwave",
    "Techstars", "Y Combinator", "Activate", "2150", "25madison",
    "53 Stations", "AVP", "ARIA", "Blindspot", "Buoyant", "Capmont",
    "Claltech", "Definition", "Echelon", "FUSE", "Arena",
    "Boomtown Accelerators", "Fast Forward", "4.0 Schools", "Praxis",
    "General Catalyst", "Index Ventures", "Insight Partners", "Lux Capital",
    "Khosla Ventures", "Framework Ventures", "Alkeon Capital",
    "Long Journey Ventures", "HighFive Partners", "Alumni Ventures",
    "Israel VC Forum", "Imagine", "Climate Draft", "Onboard Climate",
    "Space Capital | Talent", "Decisive Point", "Arbitrum", "Avalanche",
    "CoinFund", "Dragonfly", "DCG", "Arca", "BNB Chain", "Animoca Brands",
}
# Note: the allowlist is a name hint, not an override. A board whose own
# companies measure enterprise-shaped is reclassified regardless of being
# listed here — `Compa` was allowlisted by name and measures small=12%
# big=66%, which is a comp-benchmarking vendor's board, not a portfolio.

# Boards that are not venture portfolios no matter what their stats say.
# People Data Labs alone carries 181,487 candidate rows — a data vendor's
# marketing board, mostly enterprise employers, and the single largest source
# of the "Pre Seed Walgreens" pollution.
JUNK_ALLOW = {
    "People Data Labs", "Paychex Marketplace", "Getro Community",
    "globalelitecareers", "aogarciaagency", "prolificacademicltd",
    "CareerHub", "Out for Undergrad", "Black Virtual Career Fair",
    "AIgents_network2", "It's Pronounced Data", "Advisors", "All-hands",
    "Business-development", "Economic Development Jobs", "Columbus Job Board",
    "Career Compass of Louisiana", "EDO Marketplace", "Curated jobs in Web3",
    "jobgether", "Communities in Schools", "AnitaB.org", "AdaMarie",
    "Colorintech", "Femtech Insider", "Black Women in Asset Management",
    "Abundance Network", "Equalicert", "Canvas Prime", "CareerHub",
}

MIN_KNOWN = 8       # below this the measured shares are noise
BIG_MAX = 0.35      # more 1000+ employers than this is not a startup board
SMALL_MIN = 0.45    # at least this share small
FUNDED_MIN = 0.30   # at least this share with a real named round


def board_names():
    """Getro board names, so bare `boards` tokens that are really ATS slugs
    (`luminair` on a personio row) are not mistaken for boards."""
    names = {}
    with open(BOARDS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            nm = (r.get("name") or "").strip()
            if nm:
                names[nm] = (r.get("status") or "").strip()
    return names


def collect(known):
    """board -> {company: (stage, head_count)}, one entry per company."""
    per = {}
    seen_rows = 0
    with open(SRC, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            bl = r.get("boards") or ""
            if not bl:
                continue
            co = (r.get("company") or "").strip().lower()
            if not co:
                continue
            stage = (r.get("company_stage") or "").strip()
            hc = (r.get("company_size") or "").strip()
            for b in bl.split(";"):
                b = b.strip()
                if not b or ":" in b or b not in known:
                    continue
                seen_rows += 1
                d = per.setdefault(b, {})
                # keep the richest record seen for a company on this board
                old = d.get(co)
                if old is None or (not old[0] and stage) or (not old[1] and hc):
                    d[co] = (stage or (old[0] if old else ""),
                             hc or (old[1] if old else ""))
    return per, seen_rows


def shares(companies):
    stages = [s for s, _ in companies.values() if s]
    hcs = [h for _, h in companies.values() if h]
    funded = sum(1 for s in stages if s in FUNDED)
    late = sum(1 for s in stages if s in LATE)
    small = sum(1 for h in hcs if h in ("1", "2", "3", "4"))
    big = sum(1 for h in hcs if h == "6")
    return {
        "n_companies": len(companies),
        "n_stage": len(stages),
        "n_hc": len(hcs),
        "funded_share": funded / len(stages) if stages else None,
        "late_share": late / len(stages) if stages else None,
        "small_share": small / len(hcs) if hcs else None,
        "big_share": big / len(hcs) if hcs else None,
    }


def classify(name, s):
    """-> (verdict, why). verdict in {vc, ecosystem, junk}.

    Measured behaviour outranks every name signal. Names — the regexes and both
    hand lists — only decide boards whose own companies are too thinly enriched
    to measure, or break ties among boards that already look startup-shaped.
    """
    named_vc = bool(VC_NAME.search(name)) or name in VC_ALLOW
    enough = (s["n_hc"] or 0) >= MIN_KNOWN and (s["n_stage"] or 0) >= MIN_KNOWN
    big = s["big_share"]
    small = s["small_share"]
    fund = s["funded_share"]

    # Stats first: a board dominated by 1000+ employers, or whose companies are
    # mostly not small, is not a venture portfolio whatever it is called.
    if enough:
        if big is not None and big > BIG_MAX:
            return "junk", f"big_share {big:.0%} > {BIG_MAX:.0%}"
        if small is not None and small < SMALL_MIN:
            return "ecosystem", f"small_share {small:.0%} < {SMALL_MIN:.0%}"

    if name in JUNK_ALLOW:
        return "junk", "blocklist"
    if JUNK_NAME.search(name):
        return "junk", "name: place/institution/fair"

    if enough:
        if fund is not None and fund >= FUNDED_MIN:
            return "vc", (f"measured small {small:.0%} funded {fund:.0%}"
                          + (" + vc name" if named_vc else ""))
        if named_vc:
            # companies are small and not enterprise, but few carry a round —
            # typical of accelerators and nonprofit programmes
            return "vc", (f"vc name; small {small:.0%} but funded only "
                          f"{fund:.0%}")
        return "ecosystem", (f"stats not startup-shaped small {small:.0%} "
                             f"funded {fund:.0%}")

    # too little enrichment to measure: fall back to the name
    if named_vc:
        return "vc", f"vc name; only {s['n_stage']} stages known"
    return "ecosystem", f"unmeasurable ({s['n_companies']} companies)"


FIELDS = ["board", "verdict", "why", "status", "n_companies", "n_stage",
          "n_hc", "funded_share", "late_share", "small_share", "big_share"]


def main():
    known = board_names()
    per, rows = collect(known)
    print(f"{len(known):,} known Getro boards, {len(per):,} seen in master "
          f"({rows:,} board-row pairs)")

    out = []
    for name, companies in per.items():
        s = shares(companies)
        verdict, why = classify(name, s)
        rec = {"board": name, "verdict": verdict, "why": why,
               "status": known.get(name, ""), **s}
        for k in ("funded_share", "late_share", "small_share", "big_share"):
            rec[k] = "" if rec[k] is None else f"{rec[k]:.4f}"
        out.append(rec)

    # boards in the list but with no rows in the master: name-only call
    for name, status in known.items():
        if name in per:
            continue
        s = {"n_companies": 0, "n_stage": 0, "n_hc": 0, "funded_share": None,
             "late_share": None, "small_share": None, "big_share": None}
        verdict, why = classify(name, s)
        out.append({"board": name, "verdict": verdict, "why": why + " (no rows)",
                    "status": status, "n_companies": 0, "n_stage": 0, "n_hc": 0,
                    "funded_share": "", "late_share": "", "small_share": "",
                    "big_share": ""})

    order = {"vc": 0, "ecosystem": 1, "junk": 2}
    out.sort(key=lambda r: (order[r["verdict"]], -r["n_companies"]))

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(out)

    counts = {}
    for r in out:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print("  " + "  ".join(f"{v}={counts.get(v, 0)}"
                           for v in ("vc", "ecosystem", "junk")))
    print(f"wrote {OUT}")

    if "--show" in sys.argv:
        want = sys.argv[sys.argv.index("--show") + 1]
        for r in out:
            if r["verdict"] == want:
                print(f"  {r['n_companies']:>6}  {r['board'][:44]:<46} "
                      f"{r['why']}")


def load(path=OUT):
    """{board: verdict} for startup_build.py."""
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return {r["board"]: r["verdict"] for r in csv.DictReader(f)}


if __name__ == "__main__":
    main()
