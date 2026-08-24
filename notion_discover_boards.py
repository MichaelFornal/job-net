#!/usr/bin/env python3
"""
notion_discover_boards.py — find public notion.site careers pages.

Two channels, cheapest first (the dover_discover_boards.py ordering):

  corpus  grep all_jobs_master.csv / getro_all_jobs.jsonl for notion.site
          links.  Every notion.site row in the corpus arrives as Getro
          apply-URL residue — 363 distinct hosts in getro_jobs_master.csv.
  cdx     Wayback over the notion.site domain, filtered to job-ish urlkeys.

NEITHER CHANNEL IS SUFFICIENT ALONE (measured 2026-08-13):
  * CDX misses 279 of the corpus's 363 hosts — 77% — because the bare-slug
    grammar (/sdr, /credit, /legal-engineer-us) contains no job keyword and
    cannot match any keyword filter.
  * The corpus misses 2,873 hosts CDX finds.
Dropping either one silently halves the pipe.

TWO URL GRAMMARS, both emitted:
  hex   /Some-Title-<32hex>  — the page id is in the URL, no probe call needed
  slug  /sdr                 — resolved from the HTML by the probe stage
A BARE ROOT IS NOT A CANDIDATE: https://<sub>.notion.site/ serves a ~19KB JS
shell with no page id for every tenant, so a subdomain on its own is useless.

Collapse note: like Dover, the id sits at the END of a variable-length path,
so a prefix collapse of the kind slug_common's apexes use would silently drop
boards.  Full `collapse=urlkey` is used instead.

66% of CDX rows for this domain are /_assets/*.js|css|ico — the asset filter
is load-bearing, not cosmetic.

Output: notion_candidates.csv  host,path,page_id,grammar,sources
"""
import csv
import os
import re
import subprocess
import sys
import time

import slug_common as sc

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

OUT_CSV = os.path.join(BASE, PFX + "notion_candidates.csv")
STATE_JSON = os.path.join(BASE, PFX + "notion_discovery_state.json")
RAW_TXT = os.path.join(BASE, PFX + "notion_wayback_urls.txt")
ALLJOBS_CSV = os.path.join(BASE, "all_jobs_master.csv")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")

CDX_URL = "notion.site"
CDX_FILTER = ("urlkey:.*(career|jobs|hiring|open-role|open-position|"
              "join-us|joinus|were-hiring|vacanc|recruit).*")
CDX_LIMIT = 150000

# ugrep is what `grep` resolves to in this repo and it rejects bounded
# repetitions like .\{0,150\}; keep every pattern unbounded.
GREP_PAT = r"https?://[a-z0-9-]+\.notion\.site/[^,\"' )]+"

URL_RE = re.compile(r"https?://([a-z0-9-]+)\.notion\.site/([^\s,\"')]*)", re.I)
ASSET_RE = re.compile(r"\.(js|css|png|jpe?g|gif|svg|ico|woff2?|ttf|eot|map|"
                      r"json|xml|txt|pdf|webp|mp4)$", re.I)
HEX_TAIL_RE = re.compile(r"(?:^|-)([0-9a-f]{32})$", re.I)

# Hosts that are Notion's own or obviously not an employer board.
JUNK_HOST = {"www", "app", "api", "help", "developers", "notion", "www6",
             "test", "staging", "localhost"}
# Reserved first path segments that are never a board page.
JUNK_PATH = {"login", "signup", "product", "pricing", "templates", "help",
             "usersessionauth", "_assets", "images", "static", "api",
             "robots.txt", "sitemap.xml", "favicon.ico"}


def norm(host, path):
    """-> (host, path, page_id, grammar) or None.

    Strips the query (?pvs=25, ?source=copy_link) and — critically — the
    #fragment.  vet_seen_keys.txt shows one atlaskitchen.notion.site page
    exploded into 30 fabricated job keys via #anchor fragments; keeping
    fragments here would reproduce that at discovery time."""
    host = (host or "").lower()
    if host in JUNK_HOST or len(host) < 2:
        return None
    path = (path or "").split("?")[0].split("#")[0].strip("/")
    if not path:
        return None                      # bare root carries no page id
    first = path.split("/")[0].lower()
    if first in JUNK_PATH or ASSET_RE.search(path):
        return None
    # Notion paths are a single segment; anything deeper is an asset route.
    if "/" in path:
        return None
    m = HEX_TAIL_RE.search(path)
    if m:
        return host, path, m.group(1).lower(), "hex"
    if re.fullmatch(r"[0-9a-f]{32}", path, re.I):
        return host, path, path.lower(), "hex"
    if len(path) > 120:
        return None
    return host, path, "", "slug"


def grep(path, pattern):
    if not os.path.exists(path):
        print("  (%s missing, skipped)" % os.path.basename(path), flush=True)
        return ""
    with open(path, "rb") as fh:
        return subprocess.run(["grep", "-aoE", pattern], stdin=fh,
                              capture_output=True, text=True).stdout


def from_corpus(add):
    for path, tag in ((ALLJOBS_CSV, "corpus-alljobs"),
                      (GETRO_JSONL, "corpus-getro")):
        out = grep(path, GREP_PAT)
        n = 0
        hosts = set()
        for hit in set(out.splitlines()):
            m = URL_RE.match(hit)
            if not m:
                continue
            r = norm(m.group(1), m.group(2))
            if r:
                add(*r, src=tag)
                hosts.add(r[0])
                n += 1
        print("  %s: %d candidate pages, %d hosts"
              % (tag, n, len(hosts)), flush=True)


def from_cdx(add, state):
    """ONE request, not paginated.

    Uses slug_common's VALIDATED client, never a raw requests.get:
    web.archive.org serves 503/504 HTML with HTTP 200, and the legacy
    lever_/greenhouse_/ashby_ discover scripts still have that bug.

    showNumPages reports 3,491 pages for this domain, but that is the
    UNFILTERED size — with the job-keyword filter applied nearly every page
    comes back empty, so walking them costs 3,491 requests to collect ~18k
    rows.  A single high-limit request returns the same set in about three
    minutes (measured).  Pagination survives only as the truncation fallback."""
    s = sc.session()
    total = 0
    try:
        for part in cdx_partitions(state):
            pk = part["key"]
            if pk in state.get("pages_done", {}):
                continue
            params = {"url": CDX_URL, "matchType": "domain", "fl": "original",
                      "collapse": "urlkey", "filter": CDX_FILTER,
                      "limit": str(CDX_LIMIT)}
            params.update(part["params"])
            body = sc.cdx_get(s, params, "urls")
            lines = [x for x in body.splitlines() if x.strip()]
            if len(lines) >= CDX_LIMIT and not part["params"]:
                # Truncated at the cap: fall back to per-year partitions so
                # nothing is silently lost.
                print("    hit the %d-row cap — partitioning by year"
                      % CDX_LIMIT, flush=True)
                state["partition_by_year"] = True
                sc.save_state(STATE_JSON, state)
                return from_cdx(add, state)
            # Durability before bookkeeping: a page must never be recorded
            # done before its rows are on disk (slug_discover_boards.py).
            with open(RAW_TXT, "a", encoding="utf-8") as f:
                for ln in lines:
                    f.write(ln + "\n")
                f.flush()
                os.fsync(f.fileno())
            for ln in lines:
                m = URL_RE.match(ln.strip())
                if not m:
                    continue
                r = norm(m.group(1), m.group(2))
                if r:
                    add(*r, src="cdx")
                    total += 1
            state.setdefault("pages_done", {})[pk] = len(lines)
            sc.save_state(STATE_JSON, state)
            print("    %s: %d rows, %d candidates so far"
                  % (pk, len(lines), total), flush=True)
    finally:
        s.close()
    print("  cdx: %d candidate pages" % total, flush=True)


def cdx_partitions(state):
    if state.get("partition_by_year"):
        return [{"key": "y%d" % y,
                 "params": {"from": str(y), "to": str(y)}}
                for y in range(2018, int(time.strftime("%Y")) + 1)]
    return [{"key": "all", "params": {}}]


def main(argv=None):
    argv = list(argv if argv is not None else sys.argv[1:])
    do_cdx = "--no-cdx" not in argv and not SMOKE
    state = sc.load_state(STATE_JSON)

    cands = {}

    def add(host, path, page_id, grammar, src=""):
        # Key on (host, page_id) when we have one so the same board found by
        # two different title-slugs collapses; else on the literal path.
        k = (host, page_id or "s:" + path.lower())
        d = cands.setdefault(k, {"host": host, "path": path,
                                 "page_id": page_id, "grammar": grammar,
                                 "sources": set()})
        d["sources"].add(src)
        # Prefer a hex path: it needs no HTML round-trip in the probe.
        if grammar == "hex" and d["grammar"] != "hex":
            d.update(path=path, page_id=page_id, grammar="hex")

    from_corpus(add)
    if do_cdx:
        from_cdx(add, state)
    else:
        print("  (cdx skipped)", flush=True)

    rows = sorted(cands.values(), key=lambda d: (d["host"], d["path"]))
    if SMOKE and len(rows) > SMOKE:
        step = max(1, len(rows) // SMOKE)
        rows = rows[::step][:SMOKE]
        print("SMOKE MODE: %d candidates, artifacts prefixed '%s'"
              % (len(rows), PFX), flush=True)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["host", "path", "page_id", "grammar", "sources"])
        for d in rows:
            w.writerow([d["host"], d["path"], d["page_id"], d["grammar"],
                        "|".join(sorted(x for x in d["sources"] if x))])
    sc.save_state(STATE_JSON, state)

    nhex = sum(1 for d in rows if d["grammar"] == "hex")
    print("%d candidates (%d hex, %d slug) across %d hosts -> %s"
          % (len(rows), nhex, len(rows) - nhex,
             len({d["host"] for d in rows}), OUT_CSV), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
