#!/usr/bin/env python3
"""
successfactors_fetch.py — SAP SuccessFactors Recruiting (career{N} hosts).

STATUS: NO ROWS ARE FETCHED, DELIBERATELY.  Read the finding below before
"fixing" this script — the work of probing the platform is already done and the
conclusion is that there is no route this pipeline should take.

TENANTS.  tenants/successfactors.csv has three rows and the "tenant" in it is
not an employer at all — it is the SuccessFactors DATA CENTRE host:

    career5  https://career5.successfactors.eu/sfcareer/jobreqcareer
             ?company=Sonova&jobId=164771&locale=en_US
    career4  https://career4.successfactors.com/career?...&company=ariat&...
    career8  https://career8.successfactors.com/career?...&company=KomatsuLive

Every SAP customer in a given region shares one career{N} host and is
distinguished only by the `company=` query parameter.  So the three "tenants"
are three shared front doors, and the corpus knows exactly three companies
behind them (Sonova, ariat, KomatsuLive) out of the thousands hosted there —
737 rows total, 0.06% of the 1.17M-row corpus.  There is no enumeration
endpoint: `company=` values can only be harvested from apply URLs we already
have, so this pipe could never be more than a re-fetch of three known boards.

WHAT WAS PROBED (2026-07-29, all three hosts)
  /careers?company=X                200 text/html, 189-270 KB.  A JSF shell.
                                    ZERO job links in it: no `jobId=`, no
                                    ld+json, no JobPosting, no result markup.
                                    Results are loaded afterwards over AJAX.
  /career?company=X&career_ns=job_listing
                                    200, same JSF shell, same zero jobs.
  /careers?company=X&rss=true       200, identical HTML — the rss parameter is
                                    ignored, which is exactly the "200 that
                                    means nothing" this repo keeps paying for.
  /careers/jobRSSFeed?company=X     404
  /services/xml/jobs?company=X      404
  /sitemap.xml                      404
  /sfcareer/jobreqcareer?...        302 (single-posting deep link, no listing)

  The only listing route that exists is DWR remoting.  The page loads
  /ui/rcmjobsearch/js/ajaxservice/careerJobSearchControllerASProxy_*.js, which
  resolves to POST /xi/ajax/remoting -> careerJobSearchControllerProxy.searchJobs,
  guarded by a per-session token the page embeds as
  `var ajaxSecKey="..."` / `_s.crb`.

WHY IT STOPS HERE.  All three hosts serve the same robots.txt:

    User-agent: *
    Disallow: /
    Disallow: /*company
    Allow: /login

That is not a generic blanket rule — `Disallow: /*company` names the exact
query shape every route above needs, and the single Allow is the login page.
SAP is explicitly refusing automated access to these career pages, and the only
technical way through would be to drive the CSRF-guarded internal AJAX endpoint
of a product that just told us not to.  Weighed against 3 boards / 737 corpus
rows / 3 employers who are almost certainly already in the master via their own
careers domains, that is not a trade this pipeline should make.

If that ever changes the fix is small and this script tells you: it re-reads
robots.txt on every run and reports, per host, whether the career path is still
disallowed.  A host that starts allowing it is recorded `allowed-implement-me`
rather than silently doing nothing.

Output: contract_norm/successfactors.jsonl is created (empty) so downstream
globs over contract_norm/ do not have to special-case a missing file.
State:   successfactors_fetch_state.json — per host, the robots verdict.
"""
import os
import sys
import urllib.parse
import urllib.robotparser as robotparser

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "successfactors.csv")
OUT = os.path.join(cc.NORM_DIR, "successfactors.jsonl")
STATE = os.path.join(BASE, "successfactors_fetch_state.json")
TIMEOUT = 30


def hosts_and_companies():
    """[(host, company)] parsed from the example URLs, deduped by host."""
    import csv
    out, seen = [], set()
    if not os.path.exists(TENANTS):
        return out
    with open(TENANTS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            url = (r.get("example_url") or "").strip()
            if not url:
                continue
            sp = urllib.parse.urlsplit(url)
            host = sp.hostname or ""
            comp = (urllib.parse.parse_qs(sp.query).get("company") or [""])[0]
            if host and host not in seen:
                seen.add(host)
                out.append((r.get("tenant") or host, host, comp))
    return out


def robots_verdict(s, host, company):
    """(allowed, detail) for the career listing path on this host.

    Fetched live rather than hard-coded: the whole point of leaving this script
    in the tree is that it notices if SAP ever relaxes the rule.
    """
    url = f"https://{host}/robots.txt"
    try:
        r = s.get(url, timeout=TIMEOUT)
    except requests.RequestException as e:
        return None, f"robots.txt unreachable ({type(e).__name__})"
    if r.status_code != 200:
        return None, f"robots.txt HTTP {r.status_code}"
    rp = robotparser.RobotFileParser()
    rp.parse((r.text or "").splitlines())
    target = f"https://{host}/careers?company={company or 'X'}"
    allowed = rp.can_fetch(cc.UA, target)
    body = " | ".join(x.strip() for x in (r.text or "").splitlines()
                      if x.strip())[:200]
    return allowed, body


def main():
    rows = hosts_and_companies()
    if not rows:
        print(f"successfactors: no tenants in {TENANTS}", flush=True)
        return 0
    # Touch the output so downstream globs see a file, never a stale one.
    open(OUT, "a", encoding="utf-8").close()

    s = cc.session()
    state = {}
    n_allowed = 0
    try:
        for tenant, host, company in rows:
            allowed, detail = robots_verdict(s, host, company)
            if allowed is None:
                state[tenant] = f"unknown: {detail}"
                verdict = "UNKNOWN"
            elif allowed:
                state[tenant] = "allowed-implement-me"
                verdict = "ALLOWED"
                n_allowed += 1
            else:
                state[tenant] = "blocked-by-robots"
                verdict = "DISALLOWED"
            print(f"  {tenant} ({host}, e.g. company={company or '?'}): "
                  f"{verdict} — robots: {detail}", flush=True)
    finally:
        s.close()
    cc.save_state(STATE, state)

    print("DONE successfactors_fetch: 0 rows written, by design.", flush=True)
    print("  No public JSON/RSS/XML listing exists on the career{N} hosts "
          "(see the module docstring for the full probe log); the only "
          "listing route is the CSRF-guarded DWR endpoint, on paths robots.txt "
          "explicitly disallows.", flush=True)
    if n_allowed:
        print(f"  *** {n_allowed} host(s) now ALLOW the career path — robots "
              f"has changed since 2026-07-29. Re-open the DWR route.",
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
