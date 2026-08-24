#!/usr/bin/env python3
"""
tokenization_ma_remote.py — the tokenization-space cut: MA + remote.

Reads all_jobs_master.csv, keeps rows whose title / skills / company_industry /
company say the job is in the financial tokenization space (security tokens,
tokenized funds/real estate/private equity/credit, digital securities, RWA,
on-chain capital markets), then slices geography to US-remote or Massachusetts.
Rows about the tokenization of PRIVATE MARKETS are highlighted green and ranked
to the top.

  python3 tokenization_ma_remote.py            last 30 days
  python3 tokenization_ma_remote.py --days 90  last 90 days
  python3 tokenization_ma_remote.py --since 2026-01-01
  python3 tokenization_ma_remote.py --all      no date filter (fullest picture)

Output: Tokenization_MA_Remote_<date>.xlsx  (--all -> ..._all_<date>.xlsx)
  Summary          counts + what each tab is + matching caveat
  Private Markets  the highlighted subset (tokenization of private markets)
  Best Bets        the whole slice ranked (private-markets first, MA before
                   remote inside a band, newest first) — start here
  MA               Massachusetts on-site / hybrid / MA-tagged
  Remote           US-remote
  Remote (no country)  listed only as "Remote", no geography
  US (nationwide)  US-wide postings (no city), typically remote-eligible
  All              everything in the slice, flat
and a twin .csv with the same rows.

NOTE: the master has no job-description/body column, so matching is against
title + skills + company_industry + company only. Postings that mention
tokenization only in the body cannot be caught from this dataset.
"""

import csv
import datetime as dt
import os
import re
import sys

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

import all_jobs_build as ab

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")
SNAP = dt.datetime.now().strftime("%Y-%m-%d")

csv.field_size_limit(10**9)

# Rows with a blank country need a US hint in the location before a "remote" tag
# counts as US-remote — otherwise "Remote, Israel" lands in the slice. The
# two-letter code must be a real US state (uppercase), so ", UK" / ", ON" /
# ", BC" don't masquerade as US states.
US_STATES = (
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO "
    "MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC"
).split()
_US_TEXT = re.compile(r"(?i)\bunited\s+states\b|\b(?:anywhere|worldwide|global)\b")
_US_ABBR = re.compile(r"\bU\.?S\.?A?\.?\b")  # uppercase US/USA only
_US_STATE = re.compile(r",\s*(?:" + "|".join(US_STATES) + r")\b")  # uppercase


def us_hint(s):
    return bool(_US_TEXT.search(s) or _US_ABBR.search(s) or _US_STATE.search(s))

# "Remote" with no geography at all: no country column, no place name. Could
# be US-eligible, so it gets its own bucket instead of being thrown away.
BARE_REMOTE = re.compile(r"(?i)^\s*(remote(\s*[/,-]\s*(online|remote|anywhere))?"
                         r"|work\s*from\s*home|wfh)\s*$")

# US-wide postings whose only location is the country itself (no city) — no
# remote tag, but effectively nationwide / remote-eligible. Kept in their own
# bucket so they don't dilute true remote (this is where "United States"-only
# listings like Securitize's land).
NATIONWIDE = re.compile(r"(?i)^\s*(remote\s*[-,/]?\s*)?(united\s+states"
                        r"(\s+of\s+america)?|u\.?s\.?a?\.?|usa|"
                        r"anywhere\s+in\s+the\s+u\.?s\.?a?\.?)\s*$")


def _argval(flag, default):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


ALL = "--all" in sys.argv
DAYS = int(_argval("--days", "30"))
SINCE = _argval(
    "--since",
    (dt.date.today() - dt.timedelta(days=DAYS - 1)).isoformat(),
)

if ALL:
    stem = f"Tokenization_MA_Remote_all_{SNAP}"
else:
    stem = f"Tokenization_MA_Remote_{SNAP}"
OUT_XLSX = os.path.join(BASE, stem + ".xlsx")
OUT_CSV = os.path.join(BASE, stem + ".csv")

# ---- tokenization vocabulary (tight, financial sense only) -----------------
# Unambiguous financial-tokenization terms — always accepted. Note "digital
# securities" is PLURAL only: the singular "digital security" is the
# cybersecurity sense (PKI/certificates) and must not match.
TOKEN_STRONG = re.compile(r"""(?ix)
    \bsecurity\s+tokens?\b | \bdigital\s+securities\b |
    \breal[-\s]world\s+assets?\b | \bRWAs?\b | \basset[-\s]backed\s+tokens?\b |
    \btokeni[sz](?:ed|es)?\s+
        (?:fund|real\s+estate|asset|securit|treasur|bond|equit|share|
           credit|deposit|money\s+market|debt) |
    \bon[-\s]chain\s+(?:fund|securit|asset|finance|capital\s+markets|treasur)
""")

# Bare "tokeniz*" (US + UK spelling). Because "tokenization" alone is also the
# payment (card-on-file) and data-masking sense, a generic hit is only kept when
# the row ALSO carries a blockchain / digital-asset context signal (see CONTEXT).
TOKEN_GENERIC = re.compile(r"(?i)\btokeni[sz](?:e|es|ed|ing|ation)\b|\bSTOs?\b")

# Blockchain / digital-asset context that qualifies a generic "tokeniz*" hit as
# the financial-tokenization sense we want.
CONTEXT = re.compile(r"""(?ix)
    blockchain | on[-\s]chain | onchain | web3 | digital\s+asset | \bcrypto |
    \bdefi\b | distributed\s+ledger | \bDLT\b | smart\s+contract | ethereum |
    \bsolana\b | stablecoin | \bRWA | real[-\s]world\s+asset |
    security\s+token | tokeni[sz]ed | digital\s+securities
""")

# Non-financial senses of "tokenization" to keep out: NLP, card/payment
# tokenization, and data-security tokenization (masking / encryption / DLP).
EXCLUDE = re.compile(r"""(?ix)
    \bNLP\b | \btokenizer\b | \bsub[-\s]?word\b | \bbyte[-\s]pair\b | \bBPE\b |
    \bPCI\b | \b(?:card|payment|data)\s+tokeni[sz] | \bdetokeni[sz] |
    card\s+on\s+file | \bnetwork\s+tokens?\b | \bEMV\b |
    \bdata\s+security\b | \bdata\s+masking\b | \bdata\s+protection\b |
    \bdata\s+loss\s+prevention\b | \bDLP\b | encryption\s+engineer
""")

# Known pure-play tokenization / RWA firms — catches roles whose title/skills
# don't literally say "tokenization". EDIT THIS LIST to taste. Broad crypto
# names (Coinbase, Fireblocks, Franklin Templeton) are intentionally omitted to
# protect precision; names below are chosen to be distinctive.
_COMPANIES = [
    "securitize", "ondo", "centrifuge", "tokeny", "polymath", "superstate",
    "swarm markets", "addx", "securrency", "provenance blockchain",
    "maple finance", "goldfinch", "backed finance", "plume network", "dinari",
    "taurus", "obligate", "openeden", "hashnote", "archax", "fnality", "nayms",
    "untangled", "credix", "tzero", "realtoken",
]
COMPANY = re.compile(r"(?i)\b(" + "|".join(re.escape(c) for c in _COMPANIES)
                     + r")\b")

# Private-markets signal — drives the green highlight and the top band. Because
# the master has no JD body, private-markets intent rarely appears as a literal
# phrase; it is therefore detected in three layers: (1) the phrase below,
# (2) private-markets company_industry tags, (3) a curated list of firms whose
# business IS the tokenization of private markets.
PRIVATE = re.compile(r"""(?ix)
    private\s+market | private\s+capital | private\s+equit | private\s+credit |
    private\s+debt | private\s+placement | private\s+real\s+estate |
    venture\s+capital | \bVC\s+fund | pre[-\s]?IPO | \bsecondaries\b |
    secondary\s+market | fund\s+finance | \bNAV\s+lending | fund\s+admin |
    limited\s+partner | general\s+partner | \b(?:LP|GP)s?\s*/\s*(?:LP|GP)s?\b |
    alternative\s+(?:asset|investment) | \balternatives\b | \billiquid |
    real\s+estate\s+fund | infrastructure\s+fund
""")

# Private-markets company_industry tags (bare "Asset Management" is too broad —
# it captures public managers — so it is deliberately left out).
PM_INDUSTRY = re.compile(r"""(?ix)
    private\s+equity | venture\s+capital | private\s+credit | private\s+debt |
    alternative\s+invest | hedge\s+fund | fund\s+administration | family\s+office
""")

# Firms whose focus is private-markets / alternatives tokenization & infra.
# EDIT to taste.
_PM_COMPANIES = [
    "securitize", "aumni", "impact capital managers", "addx", "forge global",
    "hamilton lane", "moonfare", "allocate", "republic", "swarm markets",
]
PM_COMPANY = re.compile(r"(?i)\b(" + "|".join(re.escape(c) for c in _PM_COMPANIES)
                        + r")\b")

COLS = [
    "company", "title", "seniority", "employment_type", "work_mode", "remote",
    "location", "state", "country", "posted", "salary_min", "salary_max",
    "salary_currency", "salary_period", "skills", "company_stage",
    "company_size", "company_industry", "apply_url", "job_url", "source",
]
CTX = ["rank", "band", "bucket", "private_markets", "match_field", "matched"]
OUT_COLS = CTX + COLS

WIDTHS = {
    "rank": 6, "band": 34, "bucket": 18, "private_markets": 14,
    "match_field": 11, "matched": 22,
    "company": 26, "title": 48, "seniority": 10, "employment_type": 13,
    "work_mode": 10, "remote": 8, "location": 30, "state": 8, "country": 8,
    "posted": 11, "salary_min": 10, "salary_max": 10, "salary_currency": 8,
    "salary_period": 8, "skills": 50, "company_stage": 12, "company_size": 12,
    "company_industry": 24, "apply_url": 45, "job_url": 45, "source": 14,
}

PM_FILL = PatternFill("solid", fgColor="C6EFCE")  # green — private markets
BANDS = [
    (1, "1 Private-markets tokenization"),
    (2, "2 Tokenization — title"),
    (3, "3 Tokenization — skills/industry/company"),
]


def clean(v):
    return "" if v is None else ILLEGAL_CHARACTERS_RE.sub("", str(v))


def private_reason(title, skills, ind, company):
    """Why this row counts as private-markets tokenization ('' if it doesn't)."""
    m = PRIVATE.search(f"{title} {skills} {ind}") or PRIVATE.search(company)
    if m:
        return re.sub(r"\s+", " ", m.group(0)).strip().lower()
    m = PM_INDUSTRY.search(ind)
    if m:
        return "industry: " + m.group(0).strip().lower()
    m = PM_COMPANY.search(company)
    if m:
        return "firm: " + m.group(1).strip().lower()
    return ""


def classify(row):
    """Return (match_field, matched, private_reason) or None."""
    title = row.get("title", "") or ""
    skills = row.get("skills", "") or ""
    ind = row.get("company_industry", "") or ""
    company = row.get("company", "") or ""
    blob = f"{title} {skills} {ind}"

    company_hit = COMPANY.search(company)
    has_context = bool(CONTEXT.search(blob))
    pm = private_reason(title, skills, ind, company)
    fields = (("title", title), ("skills", skills), ("industry", ind))

    # 1) unambiguous financial-tokenization terms — always accept
    for fname, text in fields:
        m = TOKEN_STRONG.search(text)
        if m:
            return fname, re.sub(r"\s+", " ", m.group(0)).strip(), pm

    # 2) bare "tokeniz*" / STO — needs blockchain/digital-asset context to count,
    #    and must not read as the payment/data/NLP sense
    if has_context and not EXCLUDE.search(blob):
        for fname, text in fields:
            m = TOKEN_GENERIC.search(text)
            if m:
                return fname, re.sub(r"\s+", " ", m.group(0)).strip(), pm

    # 3) known pure-play tokenization company
    if company_hit:
        return "company", company_hit.group(1), pm

    return None


def band_of(match_field, pm):
    if pm:
        return 1, "1 Private-markets tokenization"
    if match_field == "title":
        return 2, "2 Tokenization — title"
    return 3, "3 Tokenization — skills/industry/company"


def _negdate(d):
    """Sort newest-first inside a band."""
    return tuple(-int(x) for x in d.split("-")) if d.count("-") == 2 else (0,)


def main():
    rows = []
    scanned = tok_hits = 0
    with open(SRC, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            scanned += 1
            posted = (r.get("posted") or "")[:10]
            if not ALL and (not posted or posted < SINCE):
                continue
            hit = classify(r)
            if not hit:
                continue
            tok_hits += 1
            match_field, matched, pm = hit

            country = (r.get("country") or "").strip().upper()
            loc = r.get("location") or ""
            state = r.get("state") or ""
            regs = ab.regions_of(loc, state, r.get("country") or "")
            is_ma = "MA" in regs
            remote = (r.get("remote") == "True"
                      or r.get("work_mode") == "Remote"
                      or "remote" in loc.lower())
            us = country in ("US", "USA", "UNITED STATES") or (
                not country and us_hint(loc + " " + state))

            if is_ma:
                bucket = "MA remote" if remote else "MA"
            elif remote and us:
                bucket = "Remote"
            elif remote and not country and BARE_REMOTE.match(loc):
                bucket = "Remote (no country)"
            elif us and NATIONWIDE.match(loc.split(";")[0].strip()):
                bucket = "US (nationwide)"
            else:
                continue

            n, label = band_of(match_field, pm)
            rec = {c: r.get(c, "") for c in COLS}
            rec.update(band=label, bucket=bucket, private_markets=pm,
                       match_field=match_field, matched=matched)
            rec["_n"] = n
            rec["_ma"] = 0 if is_ma else 1
            rec["_posted"] = posted
            rows.append(rec)

    rows.sort(key=lambda r: (r["_n"], r["_ma"], _negdate(r["_posted"]),
                             r["company"]))
    for i, r in enumerate(rows, 1):
        r["rank"] = i

    pm = [r for r in rows if r["private_markets"]]
    ma = [r for r in rows if r["bucket"].startswith("MA")]
    rem = [r for r in rows if r["bucket"] in ("Remote", "MA remote")]
    anyrem = [r for r in rows if r["bucket"] == "Remote (no country)"]
    nation = [r for r in rows if r["bucket"] == "US (nationwide)"]

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    write_xlsx(rows, pm, ma, rem, anyrem, nation, scanned, tok_hits)

    window = "all dates" if ALL else f">= {SINCE} (last {DAYS} days)"
    print(f"scanned {scanned:,} jobs, {tok_hits:,} tokenization roles {window}")
    print(f"  Private markets (highlighted): {len(pm):,}")
    print(f"  MA (incl. MA-remote): {len(ma):,}")
    print(f"  Remote (US, incl. MA-remote): {len(rem):,}")
    print(f"  Remote (no country given): {len(anyrem):,}")
    print(f"  US (nationwide): {len(nation):,}")
    print(f"  slice total: {len(rows):,}")
    for n, label in BANDS:
        c = sum(1 for r in rows if r["_n"] == n)
        print(f"    {label}: {c:,}")
    print(f"wrote {OUT_XLSX}")
    print(f"wrote {OUT_CSV}")


def sheet(wb, name, rows, bold):
    ws = wb.create_sheet(name)
    for i, c in enumerate(OUT_COLS, 1):
        ws.column_dimensions[get_column_letter(i)].width = WIDTHS.get(c, 14)
    ws.freeze_panes = "A2"
    last_col = get_column_letter(len(OUT_COLS))
    ws.auto_filter.ref = f"A1:{last_col}{max(len(rows) + 1, 2)}"
    ws.append(OUT_COLS)
    for r in rows:
        ws.append([clean(r.get(c, "")) for c in OUT_COLS])
    # highlight private-markets rows green
    if rows:
        pmcol = get_column_letter(OUT_COLS.index("private_markets") + 1)
        ws.conditional_formatting.add(
            f"A2:{last_col}{len(rows) + 1}",
            FormulaRule(formula=[f'${pmcol}2<>""'], fill=PM_FILL,
                        stopIfTrue=False))


def write_xlsx(rows, pm, ma, rem, anyrem, nation, scanned, tok_hits):
    wb = Workbook()
    wb.remove(wb.active)
    bold = Font(bold=True)

    ws = wb.create_sheet("Summary")
    for col, w in (("A", 30), ("B", 12), ("C", 92)):
        ws.column_dimensions[col].width = w
    ws.append([f"Tokenization jobs — Massachusetts + remote — {SNAP}"])
    ws["A1"].font = bold
    ws.append(["All dates." if ALL
               else f"Posted on or after {SINCE} (last {DAYS} days)."])
    ws.append([])
    ws.append(["Tab", "Jobs", "What it is"])
    for c in ws[ws.max_row]:
        c.font = bold
    ws.append(["Private Markets", len(pm),
               "Tokenization of private markets: private equity/credit/debt, "
               "funds, venture, secondaries, real estate, LP/GP, alternatives. "
               "These rows are highlighted green on every tab."])
    ws.append(["Best Bets", len(rows),
               "The whole slice ranked: private-markets tokenization first, then "
               "tokenization named in the title, then via skills/industry/"
               "company. MA before remote inside a band, newest first."])
    ws.append(["MA", len(ma),
               "Massachusetts: on-site, hybrid, or MA-tagged (includes MA roles "
               "that are also remote)."])
    ws.append(["Remote", len(rem),
               "US-remote: country is US, or the location names a US state / "
               "'United States'. MA-remote roles appear here and on the MA tab."])
    ws.append(["Remote (no country)", len(anyrem),
               "Listed only as 'Remote' with no country or place at all — may or "
               "may not be US-eligible, kept separate so it doesn't dilute the "
               "Remote tab."])
    ws.append(["US (nationwide)", len(nation),
               "Location is the whole US (e.g. 'United States', no city) with no "
               "explicit remote tag — typically remote-eligible. This is where "
               "US-wide listings such as Securitize's appear."])
    ws.append(["All", len(rows), "Every row in the slice, flat."])
    ws.append([])
    ws.append(["Band", "Jobs", ""])
    for c in ws[ws.max_row]:
        c.font = bold
    for n, label in BANDS:
        ws.append([label, sum(1 for r in rows if r["_n"] == n), ""])
    ws.append([])
    ws.append(["Source", "",
               f"Filtered from {scanned:,} jobs in all_jobs_master.csv; "
               f"{tok_hits:,} tokenization roles matched before the MA/remote "
               "geography cut."])
    ws.append(["Matching", "",
               "The master has no job-description column, so matching is on "
               "title + skills + company_industry + company only. Postings that "
               "mention tokenization only in the body are not captured. Scope is "
               "the financial sense (security tokens, tokenized funds/real "
               "estate/private markets, digital securities, RWA); NLP and "
               "payment/data tokenization are excluded."])
    ws.append(["Columns", "",
               "match_field = where the tokenization match came from (title/"
               "skills/industry/company); matched = the exact keyword; "
               "private_markets = why a row is flagged as private-markets "
               "(the phrase, a private-markets industry tag, or a known firm) — "
               "non-blank rows are highlighted green."])

    sheet(wb, "Private Markets", pm, bold)
    sheet(wb, "Best Bets", rows, bold)
    sheet(wb, "MA", ma, bold)
    sheet(wb, "Remote", rem, bold)
    sheet(wb, "Remote (no country)", anyrem, bold)
    sheet(wb, "US (nationwide)", nation, bold)
    sheet(wb, "All", rows, bold)
    wb.save(OUT_XLSX)


if __name__ == "__main__":
    main()
