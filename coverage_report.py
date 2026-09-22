#!/usr/bin/env python3
"""
coverage_report.py — per-source census of what this pipeline actually reaches.

  python3 coverage_report.py [--snapshot-dir DIR] [--with-rows] [--with-master]
                             [--out coverage_<date>.csv]

Answers the one question no existing artifact answers: for every source, how
many boards do we KNOW about, how many have we actually FETCHED, and how many
of the rows we pulled survive into the master.

Three things make this harder than it looks, and all three are why the numbers
here are assembled the way they are:

1. BOARDS KNOWN IS A UNION, NEVER ONE FILE.  A family's slugs arrive from up to
   four independent channels — the Wayback slug sweep (wayback_ats_slugs.json),
   corpus mining of Getro rows (getro_ats_slugs.json), per-family CDX discovery
   (<fam>_candidates.csv), and master-mined tenants (tenants/<fam>.csv).  Any
   single one of them understates the denominator.

2. ROWS MUST COME FROM THE JSONLs, NOT THE STATE FILES.  State says which
   boards were visited, not what they yielded, and several families carry no
   count at all.  Worse, a state file can be reset independently of its output:
   on 2026-08-13 ats_all_fetch_state.json held 1,194 keys, every one of them
   "workable:", while the seven other platforms' JSONLs on disk held over
   170,000 smartrecruiters rows.  A report that trusted state alone would have
   published "0 boards reached, 0% coverage" for seven healthy platforms.

3. NOT EVERY *_fetch_state.json IS BOARD-KEYED.  adzuna/jobgether/remoteleaf/
   themuse/usajobs/web3career keep cursors, watermarks and partition lists in
   the same filename shape.  Counting their keys as "boards" is meaningless, so
   they are declared ROWS_ONLY and report boards as n/a rather than as zero —
   a zero here reads as a failure, and these are not failing.

Run it before a crawl and after one; the DIFF is the deliverable.  --with-rows
streams ~4 GB of JSONL and --with-master streams the 1.26 GB master, so the
default run is state-and-boards only, which takes about a second.
"""

import csv
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
NORM = os.path.join(BASE, "contract_norm")
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# A board is "reached" unless its state value says otherwise.  Note "empty" is
# NOT dead: a live board with no open postings answers 200 with [], and roughly
# a third of live rippling boards look like that at any moment (some refill
# later).  "unsupported" is its own thing — provably alive behind a front end
# we cannot read, which is neither a success nor a corpse.
DEAD_MARKERS = ("dead", "err", "retired", "not_a_board", "aggregator_drop",
                "blocked", "migrated", "unsupported", "error")


def is_dead(v):
    s = (v.get("status") if isinstance(v, dict) else v)
    s = str(s or "").lower()
    return any(s.startswith(m) for m in DEAD_MARKERS)


class Fam:
    """One reportable family.

    state       *_fetch_state.json, or None when the family keeps no per-board
                state (the ROWS_ONLY case)
    key_prefix  for the shared ats_all/atstail state files, the "platform:"
                prefix that selects this family's keys out of the shared dict
    slug_key    key inside wayback_ats_slugs.json / getro_ats_slugs.json
    lists       *_candidates.csv / *_job_boards.csv / tenants/*.csv
    sources     `source` column values this family emits (for row counting)
    jsonls      output files, relative to BASE
    """

    def __init__(self, name, state=None, key_prefix=None, slug_key=None,
                 lists=(), sources=None, jsonls=(), note="", probe=None,
                 covered_by=None):
        self.name = name
        self.state = state
        self.key_prefix = key_prefix
        self.slug_key = slug_key
        self.lists = lists
        self.sources = sources or (name,)
        self.jsonls = jsonls
        self.note = note
        # probe: the *_probe_results.csv holding this family's dead census.
        # A candidate that PROBED DEAD is culled, not unreached — without this
        # the report publishes lever as 31% covered when its probe actually
        # settled all 9,126 candidates and only 2,842 were ever alive.
        self.probe = probe
        # covered_by: another family whose pipe already reaches these boards.
        # ashby is fetched twice (the ats_fetch platform AND its own native
        # pipe); counting both double-counts the same 6,257 slugs.
        self.covered_by = covered_by


# The seven platforms ats_fetch.py drives, plus workable which is deliberately
# parked on its own state file (durable per-IP Cloudflare penalty).
_ATS = ["ashby", "smartrecruiters", "recruitee", "breezy", "bamboohr",
        "jazzhr", "teamtailor"]

FAMS = [Fam(p, state="ats_all_fetch_state.json", key_prefix=p + ":",
            slug_key=p, lists=(f"{p}_candidates.csv",),
            jsonls=(f"contract_norm/ats_all_{p}.jsonl",),
            covered_by=("ashby-native" if p == "ashby" else None))
        for p in _ATS]

FAMS += [
    Fam("workable", state="ats_all_workable_state.json", key_prefix="workable:",
        slug_key="workable", lists=("workable_candidates.csv",),
        jsonls=("contract_norm/ats_all_workable.jsonl",),
        note="parked: durable per-IP Cloudflare penalty"),
    Fam("getro", state="getro_fetch_state.json", lists=("getro_job_boards.csv",),
        jsonls=("getro_all_jobs.jsonl",)),
    Fam("lever", probe="lever_probe_results.csv", state="lever_fetch_state.json", slug_key="lever",
        lists=("lever_job_boards.csv", "lever_candidates.csv"),
        jsonls=("lever_all_jobs.jsonl",)),
    Fam("greenhouse", probe="greenhouse_probe_results.csv", state="greenhouse_fetch_state.json", slug_key="greenhouse",
        lists=("greenhouse_job_boards.csv", "greenhouse_candidates.csv"),
        jsonls=("greenhouse_all_jobs.jsonl",)),
    Fam("ashby-native", probe="ashby_fetch_state.json", state="ashby_fetch_state.json", slug_key="ashby",
        lists=("ashby_job_boards.csv", "ashby_candidates.csv"),
        sources=("ashby",), jsonls=("ashby_all_jobs.jsonl",),
        note="ashby's own pipe, separate from the ats_fetch platform"),
    Fam("personio", state="personio_fetch_state.json", slug_key="personio",
        lists=("personio_job_boards.csv", "personio_candidates.csv"),
        jsonls=("personio_all_jobs.jsonl",)),
    Fam("rippling", probe="rippling_probe_results.csv", state="rippling_fetch_state.json", slug_key="rippling",
        lists=("rippling_job_boards.csv", "rippling_candidates.csv"),
        jsonls=("rippling_all_jobs.jsonl",)),
    Fam("yc", state="yc_fetch_state.json",
        lists=("yc_job_boards.csv", "yc_candidates.csv"),
        jsonls=("yc_all_jobs.jsonl",)),
    Fam("dover", probe="dover_probe_results.csv", state="dover_fetch_state.json",
        lists=("dover_job_boards.csv", "dover_candidates.csv"),
        jsonls=("dover_all_jobs.jsonl",)),
    Fam("notion", probe="notion_probe_results.csv", state="notion_fetch_state.json",
        lists=("notion_job_boards.csv", "notion_candidates.csv"),
        jsonls=("notion_all_jobs.jsonl",)),
    Fam("bullhorn", state="bh_all_fetch_state.json", sources=("bullhorn",),
        jsonls=("contract_norm/bullhorn_all.jsonl",)),
    Fam("workday", state="workday_fetch_state.json",
        lists=("tenants/workday.csv",), jsonls=("contract_norm/workday.jsonl",)),
    Fam("icims", state="icims_fetch_state.json", lists=("tenants/icims.csv",),
        jsonls=("contract_norm/icims.jsonl",)),
    Fam("oracle", state="oracle_fetch_state.json", lists=("tenants/oracle.csv",),
        jsonls=("contract_norm/oracle.jsonl",)),
    Fam("adp", state="adp_fetch_state.json", lists=("tenants/adp.csv",),
        jsonls=("contract_norm/adp.jsonl",)),
    Fam("paycom", state="paycom_fetch_state.json",
        lists=("tenants/paycom_path.csv", "tenants/paycom.csv"),
        jsonls=("contract_norm/paycom.jsonl",)),
    Fam("eightfold", state="eightfold_fetch_state.json",
        lists=("tenants/eightfold.csv",),
        jsonls=("contract_norm/eightfold.jsonl",)),
    Fam("jobvite", state="jobvite_fetch_state.json",
        lists=("tenants/jobvite.csv",), jsonls=("contract_norm/jobvite.jsonl",)),
    Fam("gem", state="gem_fetch_state.json", lists=("tenants/gem.csv",),
        jsonls=("contract_norm/gem.jsonl",)),
    Fam("comeet", state="comeet_fetch_state.json", lists=("tenants/comeet.csv",),
        jsonls=("contract_norm/comeet.jsonl",)),
    # --- 2026-08-20: the families host_census.py surfaced --------------------
    # Listing them here is what puts them in the boards-known-vs-reached diff.
    # A family missing from FAMS is not reported as 0% covered -- it is not
    # reported at all, which is how a pipe goes stale unnoticed.
    Fam("ukg", state="ukg_fetch_state.json",
        lists=("tenants/ukg.csv", "ukg_candidates.csv"),
        jsonls=("contract_norm/ukg.jsonl",)),
    Fam("manatal", state="manatal_fetch_state.json",
        lists=("tenants/manatal.csv", "manatal_candidates.csv"),
        jsonls=("contract_norm/manatal.jsonl",)),
    Fam("dayforce", state="dayforce_fetch_state.json",
        lists=("tenants/dayforce.csv", "dayforce_candidates.csv"),
        jsonls=("contract_norm/dayforce.jsonl",)),
    Fam("neogov", state="neogov_fetch_state.json",
        lists=("tenants/neogov.csv", "neogov_candidates.csv"),
        jsonls=("contract_norm/neogov.jsonl",)),
    Fam("radancy", state="radancy_fetch_state.json",
        lists=("tenants/radancy.csv", "radancy_candidates.csv"),
        jsonls=("contract_norm/radancy.jsonl",)),
    Fam("cornerstone", state="cornerstone_fetch_state.json",
        lists=("tenants/cornerstone.csv",),
        jsonls=("contract_norm/cornerstone.jsonl",)),
    Fam("successfactors", state="successfactors_fetch_state.json",
        lists=("tenants/successfactors.csv",),
        jsonls=("contract_norm/successfactors.jsonl",),
        note="DELIBERATE ZERO: robots.txt closes every route"),
    # state+lists added 2026-08-20: phenom is now board-driven off
    # tenants/phenom.csv (127 proved boards out of 3,709 audited candidates)
    # instead of six hard-coded hosts, so it has a real known-vs-reached number
    # for the first time.  Without these it reported blank and was invisible in
    # the coverage diff.
    # phenom_candidates.csv WAS in `lists` and must not be.  It is a candidate
    # POOL -- 4,255 mined hosts awaiting proof -- not a board list.  Counting it
    # as the denominator reported phenom at 131/4,255 = 3.1% with "4,121
    # unreached", which read as the worst coverage hole in the repo and was
    # steering the discovery queue toward a gap that does not exist.  Against
    # the boards that actually proved phenom, coverage is 131/132 = 99.2%.
    # An unproven candidate is not an unreached board.
    Fam("phenom", state="phenom_fetch_state.json",
        lists=("tenants/phenom.csv",),
        sources=("phenom",), jsonls=("contract_norm/phenom_all.jsonl",)),
]

# The six families sharing atstail_fetch_state.json, keyed "{family}:{tenant}".
for f in ("pinpoint", "keka", "join", "zohorecruit", "taleo", "wttj"):
    FAMS.append(Fam(f, state="atstail_fetch_state.json", key_prefix=f + ":",
                    lists=(f"tenants/{f}.csv",),
                    jsonls=("contract_norm/atstail.jsonl",)))

# Row-bearing sources with no per-board state.  Their *_fetch_state.json (where
# one exists) holds cursors and watermarks, so board counts are n/a, not 0.
ROWS_ONLY = {
    "careerjet": ("contract_norm/careerjet.jsonl",),
    "themuse": ("contract_norm/themuse.jsonl",),
    "jobgether": ("contract_norm/jobgether.jsonl",),
    "remoteleaf": ("contract_norm/remoteleaf.jsonl",),
    "web3career": ("contract_norm/web3career.jsonl",),
    "usajobs": ("contract_norm/usajobs.jsonl",),
    "adzuna": ("contract_norm/adzuna.jsonl",),
    "arbeitsagentur": ("contract_norm/arbeitsagentur.jsonl",),
    "smallremote": ("contract_norm/smallremote.jsonl",),
    "nicheboards": ("contract_norm/nicheboards.jsonl",),
    "intlboards": ("contract_norm/intlboards.jsonl",),
    "intlmisc": ("contract_norm/intlmisc.jsonl",),
    "boards": ("contract_norm/boards_all.jsonl",),
}


def _argval(flag, default=None):
    if flag in sys.argv:
        j = sys.argv.index(flag)
        if j + 1 < len(sys.argv):
            return sys.argv[j + 1]
    return default


def read_json(root, name):
    p = os.path.join(root, name)
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # A truncated state file loads as {} through contract_common, which
        # silently reads as "nothing done".  Say so instead of averaging it in.
        print(f"  !! {name} FAILED TO PARSE ({e}) — treated as absent")
        return None


def count_list(root, rel):
    """Rows in a candidate/board/tenant CSV, minus the header."""
    p = os.path.join(root, rel)
    if not os.path.exists(p):
        return 0
    with open(p, encoding="utf-8", errors="replace") as f:
        return max(0, sum(1 for _ in f) - 1)


def boards_known(root, fam, wayback, getro, state_total=0):
    """Union of every channel that can name a board for this family.

    state_total belongs in the union, not outside it: a fetch state accumulates
    across discovery generations, so it routinely holds boards the current
    candidate list no longer mentions.  taleo is the proof — 45 state keys
    against a 42-row tenants file, which reported 107% coverage until the state
    count was folded in here.
    """
    best = state_total
    for rel in fam.lists:
        best = max(best, count_list(root, rel))
    if fam.slug_key:
        for src in (wayback, getro):
            if src and fam.slug_key in src:
                best = max(best, len(src[fam.slug_key]))
    return best


def state_counts(root, fam):
    """(reached, dead, total_keys) or None when the family keeps no state."""
    if not fam.state:
        return None
    d = read_json(root, fam.state)
    if d is None:
        return None
    if fam.key_prefix:
        d = {k: v for k, v in d.items() if k.startswith(fam.key_prefix)}
    if not d:
        return (0, 0, 0)
    dead = sum(1 for v in d.values() if is_dead(v))
    return (len(d) - dead, dead, len(d))


# A family is stale when its NEWEST per-board fetch is older than this.  Days,
# not runs: a family that no longer runs produces no runs to count.
STALE_DAYS = int(os.environ.get("COVERAGE_STALE_DAYS") or 3)


def _age(day):
    """Whole days between an ISO date and today, or "" if unparseable."""
    try:
        y, m, d = (int(x) for x in
                   str(day).rstrip("*").split("-")[:3])
        return (date.today() - date(y, m, d)).days
    except (ValueError, TypeError):
        return ""


def state_freshness(root, fam):
    """(newest 'at' date, boards touched in the last STALE_DAYS) or None.

    Coverage answers "was this board EVER reached" and that is not the same
    question as "is this family still working".  Measured 2026-09-16:
    cornerstone reported 100.0% coverage (63 of 63 boards) while 61 of its 63
    state keys were dated 2026-08-20 -- 27 days untouched -- because its state
    was frozen and every run printed "0 to fetch" and exited 0.  A family can
    be fully covered and completely dead at the same time, and until this
    column existed the report could not say so.
    """
    if not fam.state:
        return None
    d = read_json(root, fam.state)
    if d is None:
        return None
    if fam.key_prefix:
        d = {k: v for k, v in d.items() if k.startswith(fam.key_prefix)}
    ats = [(v or {}).get("at") for v in d.values() if isinstance(v, dict)]
    ats = [a for a in ats if a]
    cut = (date.today() - timedelta(days=STALE_DAYS)).isoformat()
    if ats:
        return (max(ats), sum(1 for a in ats if a >= cut))
    # Most state files do not carry a per-board "at" -- 30 of the 41 families
    # store a bare status string, or a dict with a different shape.  Fall back
    # to the mtime of the family's OUTPUT, never the state file's: a run that
    # fetches nothing still rewrites its state at exit, so state mtime is
    # exactly the false-freshness trap that hid the frozen families.  The
    # output only moves when rows are written.
    newest = ""
    for rel in fam.jsonls:
        try:
            m = os.path.getmtime(os.path.join(root, rel))
        except OSError:
            continue
        iso = datetime.fromtimestamp(m, timezone.utc).strftime("%Y-%m-%d")
        if iso > newest:
            newest = iso
    if not newest:
        return None
    return (newest + "*", "")   # * marks an output-mtime fallback


def _fam_from_path(rel):
    """contract_norm/ats_all_breezy.jsonl -> breezy;  yc_all_jobs.jsonl -> yc."""
    b = os.path.basename(rel)
    for ext in (".jsonl",):
        if b.endswith(ext):
            b = b[:-len(ext)]
    if b.startswith("ats_all_"):
        b = b[len("ats_all_"):]
    for suf in ("_all_jobs", "_all"):
        if b.endswith(suf):
            b = b[:-len(suf)]
    return b


def scan_rows(paths):
    """source -> (rows, distinct boards), one streaming pass per file."""
    rows = Counter()
    boards = defaultdict(set)
    for rel in paths:
        p = os.path.join(BASE, rel)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8", errors="replace") as f:
            for ln in f:
                if not ln.strip():
                    continue
                try:
                    r = json.loads(ln)
                except json.JSONDecodeError:
                    continue
                # The per-family *_all_jobs.jsonl files are RAW fetch output,
                # not contract_common rows, so they carry no "source" key at
                # all.  Deriving it from the filename needs the _all_jobs /
                # ats_all_ decoration stripped, or every one of those families
                # reports zero rows and lands in the zero-row footer — which
                # is exactly the false alarm this report exists to prevent.
                s = r.get("source") or _fam_from_path(rel)
                rows[s] += 1
                b = r.get("board") or r.get("board_url")
                if b:
                    boards[s].add(b)
    return rows, boards


def scan_master():
    """source -> rows surviving into all_jobs_master.csv."""
    p = os.path.join(BASE, "all_jobs_master.csv")
    if not os.path.exists(p):
        return Counter()
    csv.field_size_limit(10 ** 9)
    got = Counter()
    with open(p, encoding="utf-8", errors="replace", newline="") as f:
        rd = csv.reader(f)
        hdr = next(rd, None)
        if not hdr or "sources" not in hdr:
            return got
        si = hdr.index("sources")
        for row in rd:
            if len(row) > si:
                # `sources` is the union across merged duplicates, so a row can
                # credit several pipes.  Count it for each — the column is the
                # only record of which pipes reached a job at all.
                for s in str(row[si]).split(","):
                    s = s.strip()
                    if s:
                        got[s] += 1
    return got


def main():
    root = _argval("--snapshot-dir", BASE)
    with_rows = "--with-rows" in sys.argv
    with_master = "--with-master" in sys.argv
    out = _argval("--out", os.path.join(BASE, f"coverage_{SNAP}.csv"))

    print(f"coverage report  state<-{os.path.relpath(root, BASE) or '.'}  "
          f"rows={'yes' if with_rows else 'skipped'}  "
          f"master={'yes' if with_master else 'skipped'}")

    wayback = read_json(root, "wayback_ats_slugs.json") or {}
    getro = read_json(root, "getro_ats_slugs.json") or {}

    rows = boards_seen = None
    if with_rows:
        allp = []
        for fam in FAMS:
            allp += list(fam.jsonls)
        for v in ROWS_ONLY.values():
            allp += list(v)
        print(f"  streaming {len(set(allp))} JSONLs ...")
        rows, boards_seen = scan_rows(sorted(set(allp)))
    master = scan_master() if with_master else Counter()

    recs = []
    for fam in FAMS:
        sc = state_counts(root, fam)
        fr = state_freshness(root, fam)
        known = boards_known(root, fam, wayback, getro,
                             state_total=sc[2] if sc else 0)
        reached, dead, total = sc if sc else ("", "", "")
        raw = sum(rows[s] for s in fam.sources) if rows else ""
        inmaster = sum(master[s] for s in fam.sources) if master else ""
        # A probed-dead candidate is settled, not unreached.  Take the
        # probe census as the true denominator of "already decided".
        settled = total
        if fam.probe:
            settled = max(settled, count_list(root, fam.probe)
                          if fam.probe.endswith(".csv") else 0)
        if fam.covered_by:
            settled = known          # another pipe reaches these boards
        unreached = (known - settled) if (sc and known) else ""
        pct = (100.0 * min(settled, known) / known) if (sc and known) else ""
        recs.append(dict(
            family=fam.name, boards_known=known or "",
            boards_reached=reached, boards_dead=dead,
            boards_unreached=max(0, unreached) if unreached != "" else "",
            pct_reached=f"{pct:.1f}" if pct != "" else "",
            rows_raw=raw, rows_in_master=inmaster,
            pct_surviving=(f"{100.0 * inmaster / raw:.1f}"
                           if raw and inmaster else ""),
            covered_by=fam.covered_by or "",
            last_fetch=(fr[0] if fr else ""),
            fresh_boards=(fr[1] if fr else ""),
            stale_days=(_age(fr[0]) if fr else ""),
            note=fam.note))

    for name, paths in sorted(ROWS_ONLY.items()):
        raw = rows[name] if rows else ""
        recs.append(dict(
            family=name, boards_known="n/a", boards_reached="n/a",
            boards_dead="", boards_unreached="", pct_reached="",
            rows_raw=raw, rows_in_master=master[name] if master else "",
            pct_surviving="", covered_by="",
            last_fetch="", fresh_boards="", stale_days="",
            note="no per-board state (cursor/partition only)"))

    # covered_by is emitted so a consumer can sum without double-counting.  It
    # existed on Fam but never left the script, so every reader of this CSV --
    # including the site -- counted ashby twice.
    cols = ["family", "boards_known", "boards_reached", "boards_dead",
            "boards_unreached", "pct_reached", "rows_raw", "rows_in_master",
            "pct_surviving", "covered_by", "last_fetch", "fresh_boards",
            "stale_days", "note"]
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in recs:
            w.writerow(r)

    hdr = f"{'family':17}{'known':>8}{'reached':>9}{'dead':>7}{'unreached':>11}{'pct':>7}"
    if with_rows:
        hdr += f"{'rows':>11}"
    print("\n" + hdr)
    print("-" * len(hdr))
    tk = tr = tu = 0
    fams_counted = 0
    for r in recs:
        if r["boards_known"] in ("", "n/a"):
            continue
        # A family whose boards another pipe already reaches must not be added
        # in: ashby and ashby-native carry the same 6,327 slugs, so summing
        # both inflates the total by a whole platform.
        if not r["covered_by"]:
            tk += r["boards_known"]
            tr += r["boards_reached"] or 0
            tu += r["boards_unreached"] or 0
            if r["boards_reached"]:
                fams_counted += 1
        line = (f"{r['family']:17}{r['boards_known']:>8}"
                f"{r['boards_reached']:>9}{r['boards_dead']:>7}"
                f"{r['boards_unreached']:>11}{r['pct_reached']:>7}")
        if with_rows:
            line += f"{r['rows_raw']:>11}"
        print(line)
    print("-" * len(hdr))
    print(f"{'TOTAL':17}{tk:>8}{tr:>9}{'':>7}{tu:>11}"
          f"{100.0 * tr / tk if tk else 0:>6.1f}%")
    covered = [r["family"] for r in recs if r["covered_by"]]
    if covered:
        print(f"  (deduplicated: {', '.join(covered)} excluded, "
              f"already reached by another pipe)")
    print(f"  boards reached {tr} across {fams_counted} families")

    # STALE: covered but not WORKING.  The percentage above answers "was this
    # board ever reached"; it cannot answer "is this family still running", and
    # the difference is not academic -- on 2026-09-16 cornerstone reported
    # 100.0% (63 of 63) while 61 of its 63 state keys were dated 2026-08-20,
    # 27 days untouched, because its state was frozen and every run printed
    # "0 to fetch" and exited 0.  A * marks a date taken from the family's
    # OUTPUT mtime because its state carries no per-board timestamp.
    stale = []
    for r in recs:
        age = r.get("stale_days")
        if isinstance(age, int) and age > STALE_DAYS:
            stale.append((age, r["family"], r["last_fetch"], r["note"]))
    if stale:
        stale.sort(reverse=True)
        print(f"\n  STALE (nothing written in over {STALE_DAYS} days — "
              f"coverage says reached, not recently):")
        for age, name, last, note in stale:
            why = f"  — {note[:58]}" if note else ""
            print(f"    {name:17} {last:13} {age:>4}d{why}")

    # Sources that are listed as inputs but contribute nothing.  A listed-and-
    # empty source is invisible in every other report: the build still prints a
    # healthy total and the missing file never appears in it.
    if with_rows:
        zeros = [r["family"] for r in recs
                 if r["rows_raw"] == 0 or r["rows_raw"] == ""]
        # The phenom exemption was removed 2026-08-20.  It was added when the
        # family contributed nothing; it now ships six figures of rows, and
        # leaving the exemption in would silently excuse a total failure of
        # this pipe -- which is precisely how it sat at 6 boards with 3 of them
        # returning zero for weeks.
        if zeros:
            print("\nZERO-ROW SOURCES (listed as inputs, contributing nothing):")
            for z in zeros:
                note = next((r["note"] for r in recs if r["family"] == z), "")
                print(f"  {z:20} {note}")

    print(f"\nwrote {os.path.relpath(out, BASE)}  ({len(recs)} rows)")


if __name__ == "__main__":
    main()
