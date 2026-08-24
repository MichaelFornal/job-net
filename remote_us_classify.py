#!/usr/bin/env python3
"""US-remote classifier shared by the remote workbook build.

country is blank on ~52% of remote rows and inconsistent where present
('US' / 'USA' / 'United States' / 'us'), so location and state are used as
fallbacks. Country codes in this corpus are ISO-2, which means 'CA' is Canada
(not California), 'DE' is Germany, 'IN' is India -- so two-letter tokens are
only ever read as US states out of the state field or a ', XX' location
segment, never out of country.
"""
import re

US_COUNTRY = {
    "us", "usa", "u.s.", "u.s.a.", "united states", "united states of america",
    "usa.", "united-states", "us.", "america",
}

STATE_NAMES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
    "district of columbia", "puerto rico",
]
STATE_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR",
}

# strong = unambiguous "this is the United States" wording.
# Split case-insensitive (unambiguous words) from case-sensitive (the bare
# 'US' token, so the English pronoun 'us' in e.g. 'come join us' can't fire).
STRONG_US_CI_RE = re.compile(
    r"united states of america|united states|\bu\.?s\.?a\b|\busa\b"
    r"|\bu\.s\.\B|\bus[- ]based\b|\bnationwide\b",
    re.I)
_SEP = r"\s*[-–—,/|()]*\s*"
STRONG_US_CS_RE = re.compile(
    r"\(US\)|\bUS" + _SEP + r"[Rr]emote\b|\b[Rr]emote" + _SEP + r"US\b"
    r"|\bUS\s*only\b|\bUSA\b")
# ', TX' / ', TX ' / ', TX;' style segments -- uppercase only
ABBR_RE = re.compile(r",\s*([A-Z]{2})(?=\b)")
STATE_NAME_RE = re.compile(
    r"\b(" + "|".join(re.escape(s) for s in STATE_NAMES) + r")\b", re.I)
WORLD_RE = re.compile(
    r"\banywhere\b|\bworldwide\b|\bglobal(ly)?\b|\bany location\b"
    r"|\banywhere in the world\b|\bfully distributed\b", re.I)


def _strong(loc):
    return bool(STRONG_US_CI_RE.search(loc) or STRONG_US_CS_RE.search(loc))


def _norm_country(c):
    return c.strip().strip(".").lower()


def us_basis(country, state, location):
    """Return '' (not US), or the reason this row is US-remote-eligible:
    'country' | 'state' | 'location' | 'worldwide-open'."""
    c = _norm_country(country)
    st = (state or "").strip()
    loc = location or ""

    if c in US_COUNTRY:
        return "country"

    foreign = bool(c) and c not in US_COUNTRY
    if foreign:
        # an explicit foreign country only yields to unambiguous US wording
        # (multi-country postings like 'Canada; United States')
        return "location" if _strong(loc) else ""

    # country blank from here on
    if st.upper() in STATE_ABBR or st.strip().lower() in STATE_NAMES:
        return "state"
    if _strong(loc):
        return "location"
    if STATE_NAME_RE.search(loc):
        return "location"
    for m in ABBR_RE.finditer(loc):
        if m.group(1) in STATE_ABBR:
            return "location"
    if WORLD_RE.search(loc):
        return "worldwide-open"
    return ""


BASIS_RANK = {"country": 0, "state": 1, "location": 2, "worldwide-open": 3}
