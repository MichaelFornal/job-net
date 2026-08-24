#!/usr/bin/env python3
"""
slug_discover_boards.py — crawl Wayback CDX for every ATS apex in
slug_common.APEXES and emit per-family board-slug candidate lists.

  python3 slug_discover_boards.py                     all apexes (2.5-4 h)
  python3 slug_discover_boards.py --apex rippling     one family (~1 min)
  python3 slug_discover_boards.py --apex rippling,personio
  python3 slug_discover_boards.py --measure-only      metrics, no full crawl
  python3 slug_discover_boards.py --no-grep           skip the 669 MB greps
  SMOKE=2 python3 slug_discover_boards.py --apex rippling
                                                      2 pages/apex, smoke_* out

Outputs (all prefixed smoke_ under SMOKE):
  slug_wayback_urls.txt      raw dump, "tag\\turlkey" per line, append-only
  slug_discovery_state.json  pages_done / numpages / collapse_probe — this
                             is expensive to rebuild, so run_all.py must NOT
                             rotate it away on an ordinary refresh
  <family>_candidates.csv    slug,sources,name_hint   (one per family)
  wayback_ats_slugs.json     {family: {slug: n}} sidecar merged by ats_fetch.py
  slug_discovery_report.txt  the measured baseline (Metric A/B/C)

Every slug count this repo quotes should come from the report file.  The
published baselines it replaces were not reproducible: one apex claimed 7,107
slugs and actually yields 1,164, because a too-shallow collapse silently drops
slugs that share a prefix.  Metric A now measures that per apex instead of
assuming it.
"""
import concurrent.futures as cf
import csv
import json
import os
import re
import subprocess
import sys
import threading
import time

import slug_common as sc

BASE = sc.BASE
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""
WORKERS = 6

RAW_TXT = os.path.join(BASE, PFX + "slug_wayback_urls.txt")
STATE_JSON = os.path.join(BASE, PFX + "slug_discovery_state.json")
SIDECAR = os.path.join(BASE, PFX + "wayback_ats_slugs.json")
REPORT = os.path.join(BASE, PFX + "slug_discovery_report.txt")
ALLJOBS_CSV = os.path.join(BASE, "all_jobs_master.csv")
GETRO_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")
GETRO_SLUGS = os.path.join(BASE, "getro_ats_slugs.json")

_lock = threading.Lock()
_state = {}


def cand_csv(family):
    return os.path.join(BASE, PFX + family + "_candidates.csv")


# ------------------------------------------------------------------ crawling
def fetch_page(apex, page):
    s = sc.session()
    try:
        # expect_for(): an fl=original apex returns plain URLs, which contain
        # no ')' and would be rejected wholesale by the urlkey shape test.
        body = sc.cdx_get(s, sc.page_params(apex, page),
                          sc.expect_for(apex), apex)
    finally:
        s.close()
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    key = "%s:%d" % (apex.tag, page)
    with _lock:
        # Bytes first, state second: a page must never be recorded done
        # before its rows are durable on disk.
        with open(RAW_TXT, "a", encoding="utf-8") as f:
            for ln in lines:
                f.write("%s\t%s\n" % (apex.tag, ln))
            f.flush()
            os.fsync(f.fileno())
        _state["pages_done"][key] = len(lines)
        if not lines:
            _state["empty_pages"].append(key)
        sc.save_state(STATE_JSON, _state)
    return len(lines)


def year_chunk(apex, year):
    """Fallback for a page that failed every retry: narrow by capture date so
    a different index shard is touched.  A slug archived at all was archived
    in some year, so the union preserves recall even though the partitioning
    differs from `page`."""
    s = sc.session()
    try:
        body = sc.cdx_get(s, {
            "url": apex.pattern, "fl": apex.fl, "collapse": apex.collapse,
            "from": str(year), "to": str(year), "limit": "200000",
        }, sc.expect_for(apex), apex)
    finally:
        s.close()
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    key = "%s:y%d" % (apex.tag, year)
    with _lock:
        with open(RAW_TXT, "a", encoding="utf-8") as f:
            for ln in lines:
                f.write("%s\t%s\n" % (apex.tag, ln))
            f.flush()
            os.fsync(f.fileno())
        _state["pages_done"][key] = len(lines)
        sc.save_state(STATE_JSON, _state)
    return len(lines)


def crawl(apexes):
    s = sc.session()
    jobs = []
    try:
        for a in apexes:
            # A changed collapse depth invalidates everything already crawled
            # for this apex — the old pages were collapsed more aggressively
            # and are missing slugs.  Without this the next run would keep the
            # shallow results and quietly report them as complete.
            used = _state.setdefault("collapse_used", {}).get(a.tag)
            if used and used != a.collapse:
                stale = [k for k in _state["pages_done"]
                         if k.startswith(a.tag + ":")]
                for k in stale:
                    del _state["pages_done"][k]
                print("  %-22s collapse %s -> %s, dropping %d cached pages"
                      % (a.tag, used, a.collapse, len(stale)), flush=True)
            _state["collapse_used"][a.tag] = a.collapse

            if a.pattern in _state["numpages"]:
                n = _state["numpages"][a.pattern]
            else:
                n = sc.numpages(s, a)
                with _lock:
                    _state["numpages"][a.pattern] = n
                    sc.save_state(STATE_JSON, _state)
            if SMOKE:
                n = min(n, SMOKE)
            print("  %-22s %d pages" % (a.tag, n), flush=True)
            for page in range(n):
                if "%s:%d" % (a.tag, page) not in _state["pages_done"]:
                    jobs.append((a, page))
    finally:
        s.close()
    if not jobs:
        return 0
    import random
    random.shuffle(jobs)      # spread apexes across workers
    t0 = time.time()
    done = failed = 0
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_page, a, p): (a, p) for a, p in jobs}
        for fut in cf.as_completed(futs):
            a, p = futs[fut]
            done += 1
            try:
                fut.result()
            except Exception as e:            # noqa: BLE001 — keep going
                failed += 1
                print("  PAGE FAIL %s:%d %s" % (a.tag, p, str(e)[:70]),
                      flush=True)
            if done % 20 == 0:
                el = time.time() - t0
                eta = el / done * (len(jobs) - done) / 60
                print("  %d/%d pages | eta %.0fm" % (done, len(jobs), eta),
                      flush=True)
    return failed


def run_year_fallback(apexes):
    """Second pass for apexes that still have missing pages."""
    todo = []
    for a in apexes:
        n = _state["numpages"].get(a.pattern, 0)
        if SMOKE:
            n = min(n, SMOKE)
        missing = [p for p in range(n)
                   if "%s:%d" % (a.tag, p) not in _state["pages_done"]]
        if not missing:
            continue
        this_year = time.gmtime().tm_year
        for y in range(2015, this_year + 1):
            if "%s:y%d" % (a.tag, y) not in _state["pages_done"]:
                todo.append((a, y))
    if not todo:
        return
    print("year-chunk fallback: %d windows" % len(todo), flush=True)
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(year_chunk, a, y): (a, y) for a, y in todo}
        for fut in cf.as_completed(futs):
            a, y = futs[fut]
            try:
                fut.result()
            except Exception as e:            # noqa: BLE001
                print("  YEAR FAIL %s:%d %s" % (a.tag, y, str(e)[:70]),
                      flush=True)


# ------------------------------------------------------------- other sources
GREP_PAT = {
    "rippling": r"ats\.rippling\.com/[A-Za-z0-9_.-]+|[A-Za-z0-9_-]+\.rippling-ats\.com",
    "personio": r"[A-Za-z0-9_-]+\.jobs\.personio\.(de|com)",
    "breezy": r"[A-Za-z0-9_-]+\.breezy\.hr",
    "recruitee": r"[A-Za-z0-9_-]+\.recruitee\.com",
    "jazzhr": r"[A-Za-z0-9_-]+\.applytojob\.com",
    "bamboohr": r"[A-Za-z0-9_-]+\.bamboohr\.com",
    "teamtailor": r"[A-Za-z0-9_-]+\.teamtailor\.com",
    "smartrecruiters": r"jobs\.smartrecruiters\.com/[A-Za-z0-9_.-]+",
    "workable": r"apply\.workable\.com/[A-Za-z0-9_.-]+|[A-Za-z0-9_-]+\.workable\.com",
}
_HOST_SLUG = re.compile(r"^([A-Za-z0-9_.-]+?)\.(jobs\.personio|breezy|"
                        r"recruitee|applytojob|bamboohr|teamtailor|"
                        r"rippling-ats|workable)\.")
_PATH_SLUG = re.compile(r"^(?:ats\.rippling\.com|jobs\.smartrecruiters\.com|"
                        r"apply\.workable\.com)/([A-Za-z0-9_.-]+)")


def grep_add(path, family, add, tag):
    if not os.path.exists(path):
        print("  (%s: %s missing, skipped)"
              % (tag, os.path.basename(path)), flush=True)
        return
    pat = GREP_PAT.get(family)
    if not pat:
        return
    with open(path, "rb") as fh:
        out = subprocess.run(["grep", "-aoE", pat], stdin=fh,
                             capture_output=True, text=True).stdout
    n = 0
    for hit in set(out.splitlines()):
        hit = hit.strip().lower()
        m = _PATH_SLUG.match(hit)
        seg = m.group(1) if m else None
        if seg is None:
            m = _HOST_SLUG.match(hit)
            seg = m.group(1) if m else None
        # ats.rippling.com/en-us/... -> the locale, not a board
        if seg in ("en-us", "en-gb", "de-de", "fr-fr"):
            continue
        s = sc.clean(seg)
        if s:
            add(s, tag)
            n += 1
    print("  %s/%s: %d slugs" % (family, tag, n), flush=True)


def name_hints_rippling():
    """Real company names for rippling boards, free, from the master CSV.

    The master already holds ~340 distinct ats.rippling.com boards syndicated
    via Getro, each with a proper company name — far better than
    slug.replace('-', ' ').title().
    """
    hints = {}
    if not os.path.exists(ALLJOBS_CSV):
        return hints
    with open(ALLJOBS_CSV, "rb") as fh:
        out = subprocess.run(["grep", "-a", "ats.rippling.com"], stdin=fh,
                             capture_output=True, text=True).stdout
    csv.field_size_limit(10 ** 7)
    for row in csv.reader(out.splitlines()):
        if len(row) < 2:
            continue
        company = (row[0] or "").strip()
        if not company:
            continue
        for cell in row:
            m = re.search(r"ats\.rippling\.com/([A-Za-z0-9_.-]+)", cell or "")
            if m:
                s = sc.clean(m.group(1))
                if s and s not in hints:
                    hints[s] = company
                break
    return hints


# ------------------------------------------------------------------- metrics
def metric_a(apexes):
    """Collapse-loss probe: does the production collapse depth drop slugs?"""
    out = {}
    s = sc.session()
    try:
        for a in apexes:
            n = _state["numpages"].get(a.pattern, 0)
            if not n:
                continue
            if a.raw_only:
                # Metric A measures slug loss by collapse depth, and a
                # raw_only apex has no single slug to lose -- running it here
                # would compare two empty sets and report a clean 0.00%.
                print("  metric-A skip %s (raw_only)" % a.tag, flush=True)
                continue
            pages = sorted({0, n // 2, max(0, n - 1)})
            if SMOKE:
                pages = pages[:1]
            lo_all, hi_all = set(), set()
            for p in pages:
                deep = "urlkey:%d" % (len(a.prefix) + a.budget + 24)
                try:
                    b_lo = sc.cdx_get(s, sc.page_params(a, p), "rows", a)
                    b_hi = sc.cdx_get(s, sc.page_params(a, p, deep), "rows", a)
                except RuntimeError as e:
                    print("  metric-A skip %s:%d (%s)"
                          % (a.tag, p, str(e)[:50]), flush=True)
                    continue
                for body, acc in ((b_lo, lo_all), (b_hi, hi_all)):
                    for ln in body.splitlines():
                        sl = sc.slug_of(a, ln.strip())
                        if sl:
                            acc.add(sl)
            lost = hi_all - lo_all
            rate = (len(lost) / len(hi_all)) if hi_all else 0.0
            out[a.tag] = {"lo": len(lo_all), "hi": len(hi_all),
                          "lost": len(lost), "rate": rate,
                          "sample": sorted(lost)[:5]}
    finally:
        s.close()
    return out


def known_slugs(family):
    known = set()
    if os.path.exists(GETRO_SLUGS):
        try:
            with open(GETRO_SLUGS, encoding="utf-8") as f:
                for k in (json.load(f).get(family) or {}):
                    s = sc.clean(k)
                    if s:
                        known.add(s)
        except (OSError, json.JSONDecodeError):
            pass
    return known


# --------------------------------------------------------------- aggregation
def aggregate(apexes, do_grep):
    fams = sorted({a.family for a in apexes})
    per = {f: {} for f in fams}          # family -> slug -> set(sources)

    def adder(family):
        def add(slug, src):
            per[family].setdefault(slug, set()).add(src)
        return add

    tag_apex = {a.tag: a for a in apexes}
    if os.path.exists(RAW_TXT):
        with open(RAW_TXT, encoding="utf-8") as f:
            for line in f:
                tag, _, key = line.partition("\t")
                a = tag_apex.get(tag)
                if a is None or a.raw_only:
                    # raw_only: the rows are cached for a family-specific
                    # parser (workday_discover_boards.py) and slug_of would
                    # read an fl=original URL as a urlkey and return junk.
                    continue
                sl = sc.slug_of(a, key.strip())
                if sl:
                    per[a.family].setdefault(sl, set()).add("wayback")

    for fam in fams:
        add = adder(fam)
        for k in known_slugs(fam):
            add(k, "getro")
        if do_grep:
            grep_add(ALLJOBS_CSV, fam, add, "alljobs")
            grep_add(GETRO_JSONL, fam, add, "getro-jsonl")

    hints = name_hints_rippling() if ("rippling" in fams and do_grep) else {}

    sidecar = {}
    for fam in fams:
        rows = sorted(per[fam])
        with open(cand_csv(fam), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["slug", "sources", "name_hint"])
            for sl in rows:
                w.writerow([sl, "|".join(sorted(per[fam][sl])),
                            hints.get(sl, "")])
        sidecar[fam] = {sl: 1 for sl in rows}
    tmp = SIDECAR + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sidecar, f)
    os.replace(tmp, SIDECAR)
    return per


def write_report(apexes, per, probe):
    fams = sorted({a.family for a in apexes})
    lines = ["family           pages    raw_rows   slugs   known  recall  "
             "loss%   collapse", "-" * 82]
    for fam in fams:
        taglist = [a for a in apexes if a.family == fam]
        pages_done = sum(1 for a in taglist for k in _state["pages_done"]
                         if k.startswith(a.tag + ":"))
        pages_tot = sum(min(_state["numpages"].get(a.pattern, 0), SMOKE)
                        if SMOKE else _state["numpages"].get(a.pattern, 0)
                        for a in taglist)
        raw = sum(v for k, v in _state["pages_done"].items()
                  if any(k.startswith(a.tag + ":") for a in taglist))
        slugs = set(per.get(fam) or {})
        known = known_slugs(fam)
        recall = (len(slugs & known) / len(known)) if known else float("nan")
        worst = max((probe.get(a.tag, {}).get("rate", 0.0) for a in taglist),
                    default=0.0)
        lines.append("%-16s %3d/%-4d %9d %7d %7d  %5.3f  %5.2f  %s"
                     % (fam, pages_done, pages_tot, raw, len(slugs),
                        len(known), recall, worst * 100,
                        ",".join(a.collapse for a in taglist)))
    body = "\n".join(lines) + "\n"
    with open(REPORT, "w", encoding="utf-8") as f:
        f.write(body)
    print()
    print(body, end="")
    return body


# ---------------------------------------------------------------------- main
def select(spec):
    if not spec:
        return list(sc.APEXES)
    want = {x.strip().lower() for x in spec.split(",") if x.strip()}
    out = [a for a in sc.APEXES if a.tag in want or a.family in want]
    if not out:
        raise SystemExit("unknown --apex %r (families: %s)"
                         % (spec, ", ".join(sc.FAMILIES)))
    return out


def argval(argv, flag, default=None):
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    return default


def main(argv=None):
    argv = list(argv if argv is not None else sys.argv[1:])
    apexes = select(argval(argv, "--apex"))
    do_grep = "--no-grep" not in argv and not SMOKE
    measure_only = "--measure-only" in argv

    global _state
    _state = sc.load_state(STATE_JSON)

    if SMOKE:
        print("SMOKE MODE: %d pages/apex, artifacts prefixed '%s'"
              % (SMOKE, PFX), flush=True)
    print("apexes: %s" % ", ".join(a.tag for a in apexes), flush=True)

    if not measure_only:
        crawl(apexes)
        run_year_fallback(apexes)

    probe = metric_a(apexes)
    _state["collapse_probe"] = {k: {kk: vv for kk, vv in v.items()
                                    if kk != "sample"}
                                for k, v in probe.items()}
    sc.save_state(STATE_JSON, _state)

    per = aggregate(apexes, do_grep)
    write_report(apexes, per, probe)

    raw_rows = sum(v for k, v in _state["pages_done"].items()
                   if any(k.startswith(a.tag + ":") for a in apexes))
    fams = sorted({a.family for a in apexes})
    for fam in fams:
        print("%d raw wayback rows -> %d candidate slugs -> %s"
              % (raw_rows, len(per.get(fam) or {}), cand_csv(fam)), flush=True)
    if len(fams) > 1:
        print("TOTAL: %d raw wayback rows -> %d candidate slugs across "
              "%d families -> %s"
              % (raw_rows, sum(len(per[f]) for f in fams), len(fams), REPORT),
              flush=True)

    # --- exit conditions
    shallow = [t for t, v in probe.items() if v["rate"] > 0.005]
    if shallow:
        for t in shallow:
            v = probe[t]
            print("COLLAPSE TOO SHALLOW: %s loses %d slugs (%.2f%%) e.g. %s"
                  % (t, v["lost"], v["rate"] * 100, v["sample"]), flush=True)
        raise SystemExit("collapse too shallow for %s — raise slug_budget "
                         "and re-crawl" % ",".join(shallow))
    for fam in fams:
        known = known_slugs(fam)
        if known:
            rec = len(set(per.get(fam) or {}) & known) / len(known)
            if rec < 0.85:
                print("WARNING: %s recall %.3f against %d known slugs — the "
                      "crawl may be broken regardless of raw count"
                      % (fam, rec, len(known)), flush=True)
    if not measure_only:
        per_apex = {}
        total_pages = 0
        for a in apexes:
            n = _state["numpages"].get(a.pattern, 0)
            if SMOKE:
                n = min(n, SMOKE)
            total_pages += n
            miss = [p for p in range(n)
                    if "%s:%d" % (a.tag, p) not in _state["pages_done"]]
            if miss:
                per_apex[a.tag] = miss
        missing = sum(len(v) for v in per_apex.values())
        if missing:
            # Whether a missing page is recoverable is decided by ONE thing:
            # has the year-chunk fallback already been spent on its apex?
            # Before that, re-running is how the page gets recovered and
            # exiting non-zero is correct.  After it, the page has been asked
            # for two different ways and web.archive.org refused both, so
            # another identical attempt is not a recovery strategy — it is a
            # loop.  run_all.py relaunches this script up to 40 times, and
            # each cycle re-greps the 669 MB master once per family, so an
            # unrecoverable page used to cost hours and still delivered
            # nothing.  Ceiling below keeps a genuinely broken crawl fatal.
            unspent = [t for t in per_apex
                       if not any(k.startswith(t + ":y")
                                  for k in _state["pages_done"])]
            frac = missing / max(total_pages, 1)
            print("MISSING PAGES: %d/%d (%.2f%%)"
                  % (missing, total_pages, frac * 100), flush=True)
            for tag in sorted(per_apex):
                pages = per_apex[tag]
                print("  %-22s %3d dropped%s  %s"
                      % (tag, len(pages),
                         " (year-fallback NOT yet spent)"
                         if tag in unspent else "",
                         ",".join(str(p) for p in pages[:12])
                         + ("..." if len(pages) > 12 else "")), flush=True)
            if unspent:
                raise SystemExit(
                    "%d CDX pages still missing and the year-chunk fallback "
                    "has not run for %s — re-run"
                    % (missing, ",".join(sorted(unspent))))
            if frac > 0.20:
                raise SystemExit(
                    "%d/%d pages missing (%.1f%%) — past the 20%% ceiling "
                    "this is a broken crawl, not flaky pages"
                    % (missing, total_pages, frac * 100))
            print("ACCEPTED WITH GAPS: the year-chunk fallback was already "
                  "spent on every affected apex, so these %d pages are "
                  "treated as unrecoverable rather than re-looped.  Coverage "
                  "%.2f%%.  Re-crawl them by deleting their keys from %s."
                  % (missing, (1 - frac) * 100,
                     os.path.basename(STATE_JSON)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
