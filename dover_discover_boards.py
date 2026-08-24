#!/usr/bin/env python3
"""
dover_discover_boards.py — find Dover careers-page UUIDs.

Three channels, in increasing cost order:

  corpus-apply   grep all_jobs_master.csv / getro_all_jobs.jsonl for
                 app.dover.com/apply/{careers_uuid}/{job_uuid}.  When the
                 first segment is a UUID it IS the careers-page id, so this
                 yields board ids for free.
  corpus-slug    the same links sometimes carry a company SLUG instead of the
                 UUID (app.dover.com/apply/transfyr/{job_uuid}).  Those are
                 resolved by asking the public job-detail endpoint for the
                 job's client_id.
  cdx            Wayback over app.dover.com AND app.dover.io for
                 /{Company Name}/careers/{uuid}.  Slow and flaky, so it is
                 skipped under SMOKE and can be turned off with --no-cdx.

Collapse note: Dover's UUID sits at the END of a variable-length path
(/{Company Name}/careers/{uuid}), so a prefix collapse of the kind the other
apexes use would silently drop UUIDs.  Full `collapse=urlkey` is used instead
— it still dedupes repeated snapshots of the same URL, which is all we want.

The literal path segment "dover" appears as the company name on many archived
URLs and is not a company; it is treated as junk.

Output: dover_candidates.csv  uuid,name_hint,sources
"""
import csv
import json
import os
import re
import subprocess
import sys
import time

import requests

import slug_common as sc

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

OUT_CSV = os.path.join(BASE, PFX + "dover_candidates.csv")
STATE_JSON = os.path.join(BASE, PFX + "dover_discovery_state.json")
RAW_TXT = os.path.join(BASE, PFX + "dover_wayback_urls.txt")
ALLJOBS_CSV = os.path.join(BASE, "all_jobs_master.csv")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")

DETAIL = "https://app.dover.com/api/v1/inbound/application-portal-job/%s"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
APPLY_RE = re.compile(r"app\.dover\.(?:com|io)/apply/([^/\s\"',)]+)/"
                      r"([0-9a-fA-F-]{36})")
CAREERS_RE = re.compile(r"app\.dover\.(?:com|io)/([^/\s\"',)]+)/careers/"
                        r"([0-9a-fA-F-]{36})")
JUNK_NAME = {"dover", "apply", "careers", "api", "www", ""}


def json_or_none(r):
    """Dover's SPA answers HTTP 200 with text/html on unrecognised paths, so a
    status-code check alone reports success on a completely wrong URL."""
    if r.status_code != 200:
        return None
    if "json" not in (r.headers.get("content-type") or "").lower():
        return None
    body = r.text.lstrip()
    if not body or body[0] == "<":
        return None
    try:
        return json.loads(body)
    except ValueError:
        return None


def grep(path, pattern):
    if not os.path.exists(path):
        print("  (%s missing, skipped)" % os.path.basename(path), flush=True)
        return ""
    with open(path, "rb") as fh:
        return subprocess.run(["grep", "-aoE", pattern], stdin=fh,
                              capture_output=True, text=True).stdout


def from_corpus(add):
    pat = (r"app\.dover\.(com|io)/apply/[^/\"' ,)]+/[0-9a-fA-F-]{36}"
           r"|app\.dover\.(com|io)/[^/\"' ,)]+/careers/[0-9a-fA-F-]{36}")
    slug_jobs = {}
    for path, tag in ((ALLJOBS_CSV, "alljobs"), (GETRO_JSONL, "getro")):
        out = grep(path, pat)
        n_u = n_s = 0
        for hit in set(out.splitlines()):
            m = CAREERS_RE.search(hit)
            if m:
                name, uu = m.group(1), m.group(2).lower()
                if UUID_RE.match(uu):
                    add(uu, "corpus-careers",
                        "" if name.lower() in JUNK_NAME
                        else requests.utils.unquote(name).replace("%20", " "))
                    n_u += 1
                continue
            m = APPLY_RE.search(hit)
            if m:
                first, job = m.group(1), m.group(2).lower()
                if UUID_RE.match(first):
                    add(first.lower(), "corpus-apply", "")
                    n_u += 1
                elif first.lower() not in JUNK_NAME:
                    slug_jobs.setdefault(first.lower(), job)
                    n_s += 1
        print("  %s: %d board uuids, %d slug-form links" % (tag, n_u, n_s),
              flush=True)
    return slug_jobs


def resolve_slugs(slug_jobs, add):
    """slug -> board uuid, via one job's client_id.  Cheap: one call per slug,
    and only for slugs whose board we do not already have."""
    if not slug_jobs:
        return
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    n = 0
    try:
        for slug, job in sorted(slug_jobs.items()):
            try:
                d = json_or_none(s.get(DETAIL % job, timeout=30))
            except requests.RequestException:
                d = None
            time.sleep(0.35)
            if not d:
                continue
            cid = (d.get("client_id") or "").lower()
            if UUID_RE.match(cid or ""):
                add(cid, "corpus-slug", d.get("client_name") or slug)
                n += 1
    finally:
        s.close()
    print("  slug->board resolution: %d of %d" % (n, len(slug_jobs)),
          flush=True)


def from_cdx(add, state):
    s = sc.session()
    total = 0
    try:
        for tag, pattern in (("com", "app.dover.com/*"),
                             ("io", "app.dover.io/*")):
            key = "numpages:" + tag
            if key in state:
                npages = state[key]
            else:
                npages = int(sc.cdx_get(
                    s, {"url": pattern, "showNumPages": "true"},
                    "int").strip() or 0)
                state[key] = npages
            print("  cdx %s: %d pages" % (tag, npages), flush=True)
            for page in range(npages):
                pk = "%s:%d" % (tag, page)
                if pk in state.get("pages_done", {}):
                    continue
                body = sc.cdx_get(s, {"url": pattern, "fl": "original",
                                      "collapse": "urlkey", "page": str(page)},
                                  "urls")
                lines = [x for x in body.splitlines() if x.strip()]
                with open(RAW_TXT, "a", encoding="utf-8") as f:
                    for ln in lines:
                        f.write("%s\t%s\n" % (tag, ln))
                for ln in lines:
                    m = CAREERS_RE.search(ln)
                    if m:
                        name, uu = m.group(1), m.group(2).lower()
                        if UUID_RE.match(uu):
                            add(uu, "cdx",
                                "" if name.lower() in JUNK_NAME else name)
                            total += 1
                state.setdefault("pages_done", {})[pk] = len(lines)
                sc.save_state(STATE_JSON, state)
    finally:
        s.close()
    print("  cdx: %d careers-page hits" % total, flush=True)


def main(argv=None):
    argv = list(argv if argv is not None else sys.argv[1:])
    do_cdx = "--no-cdx" not in argv and not SMOKE
    state = sc.load_state(STATE_JSON)

    cands = {}

    def add(uu, src, name=""):
        d = cands.setdefault(uu, {"sources": set(), "name": ""})
        d["sources"].add(src)
        if name and not d["name"]:
            d["name"] = name.strip()

    slug_jobs = from_corpus(add)
    resolve_slugs(slug_jobs, add)
    if do_cdx:
        from_cdx(add, state)
    else:
        print("  (cdx skipped)", flush=True)

    rows = sorted(cands)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["uuid", "name_hint", "sources"])
        for uu in rows:
            w.writerow([uu, cands[uu]["name"],
                        "|".join(sorted(cands[uu]["sources"]))])
    sc.save_state(STATE_JSON, state)
    raw = sum(state.get("pages_done", {}).values())
    print("%d raw wayback rows -> %d candidates -> %s"
          % (raw, len(rows), OUT_CSV), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
