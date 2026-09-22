#!/usr/bin/env python3
"""
bh_build_combined.py — merge the FT+contract portal crawls into one deduplicated
master + workbook.

Explicit source list (NOT a glob) so the FT-inclusive *_all files are used and
the older contract-only duplicates (ats_*, phenom.jsonl, bullhorn.jsonl) are
skipped:

  bullhorn_all.jsonl        Bullhorn OSCP staffing agencies   (FT + contract)
  ats_all_*.jsonl (x5)      Ashby/SmartRecruiters/Recruitee/Breezy/BambooHR
                            first-party ATS boards            (FT + contract)
  phenom_all.jsonl          Allegis staffing brands (Phenom)  (FT + contract)
  boards_all.jsonl          freelance marketplaces + remote boards (FT +
                            contract; boards_fetch.py under BOARDS_KEEP_ALL=1,
                            which run_all.py always passes).  The older
                            contract-only boards.jsonl is NOT included:
                            keep-all keeps a strict superset of the same rows,
                            so listing both would add nothing but a file that
                            no longer gets refreshed and therefore only ages.
  getro.jsonl / lever.jsonl contract slice of the VC/Lever board crawls

Dedupe is the same two-pass scheme as contract_build.py.

Outputs:
  portals_combined_master.csv
  FT_Contract_Portals_<date>.xlsx   README / All Jobs / Summary / Boards
"""
import csv
import glob
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.utils import get_column_letter

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_CSV = os.path.join(BASE, "portals_combined_master.csv")
OUT_XLSX = os.path.join(BASE, f"FT_Contract_Portals_{SNAP}.xlsx")
XLSX_CAP = 1_048_000
COLS = cc.COLUMNS + ["sources", "n_sources"]

INCLUDE = [
    "bullhorn_all.jsonl",
    "ats_all_ashby.jsonl", "ats_all_smartrecruiters.jsonl",
    "ats_all_recruitee.jsonl", "ats_all_breezy.jsonl", "ats_all_bamboohr.jsonl",
    "ats_all_workable.jsonl", "ats_all_jazzhr.jsonl",
    "ats_all_teamtailor.jsonl",
    "phenom_all.jsonl", "boards_all.jsonl", "getro.jsonl", "lever.jsonl",

    # --- added 2026-07-29: the remote-source and un-piped-ATS expansion -------
    # A missing file here is NOT an error — main() prints "(missing X)" and
    # continues — so a source whose fetcher has not been run yet, or which was
    # verified as genuinely closed, simply contributes nothing.  That is what
    # makes it safe to list the whole intended set rather than editing this
    # list every time one more pipe lands.
    #
    # ATS families mined out of the corpus (see ats_mine_tenants.py):
    "workday.jsonl", "adp.jsonl", "paycom.jsonl", "icims.jsonl",
    "oracle.jsonl", "successfactors.jsonl", "eightfold.jsonl",
    "comeet.jsonl", "jobvite.jsonl", "gem.jsonl", "atstail.jsonl",
    # Remote boards / aggregators:
    "themuse.jsonl", "jobgether.jsonl", "remoteleaf.jsonl", "web3career.jsonl",
    "smallremote.jsonl", "nicheboards.jsonl", "careerjet.jsonl",
    # International:
    "intlboards.jsonl", "intlboards_torre.jsonl", "intlmisc.jsonl",
    "arbeitsagentur.jsonl",
    # wttj_fetch.py, added 2026-09-02: the whole Welcome to the Jungle jobs
    # index, 88,432 postings at 100% coverage, of which 18,132 are US.  It
    # supersedes the wttj rows inside intlmisc.jsonl (that source is
    # unregistered there now) but NOT the ones inside atstail.jsonl, which
    # still walks tenants/wttj.csv against the same index -- those overlap and
    # collapse on job_id here.
    "wttj.jsonl",
    # Key-gated (produce rows only once the user's free keys are in the env):
    "usajobs.jsonl", "adzuna.jsonl",

    # --- added 2026-08-20: families host_census.py surfaced ------------------
    # None of these had ever appeared in a census, because every census in this
    # repo ran off a closed regex list and could only report on families
    # somebody had already written a rule for.  Ranked by distinct EMPLOYERS in
    # the master at the time they were built:
    #   manatal 606 (careers-page.com) · ukg 327 · neogov 159 (governmentjobs,
    #   US state/municipal, zero overlap with the federal usajobs pipe) ·
    #   dayforce 129 · cornerstone 75 · radancy 75 hosts / 62k rows.
    "ukg.jsonl", "manatal.jsonl", "dayforce.jsonl", "neogov.jsonl",
    "radancy.jsonl", "cornerstone.jsonl",

    # --- added 2026-09-05: the npo pipe, 8,590 rows warn_unlisted() had been
    # printing every run since the feed was built -----------------------------
    # These reached triage.db through npo_build.py's own feed lane but never
    # the master, so they were invisible to scout, the corpus and every
    # non-npo lane.  Exactly the failure warn_unlisted() exists to surface.
    #
    # SEVEN OF THE EIGHT ARE AGGREGATORS and could not simply be listed here.
    # Measured 2026-09-05, distinct apply_url hosts per file: idealist 1,
    # impactpool 1, chronicle 1, insidehighered 1, philanthropy 1, workforgood
    # 1, foundationlist 1 -- every row points back at the board itself.  That
    # is the self-redirect shape all_jobs_build documents on Adzuna: tier 2
    # keys on apply_url, so a self-minted id makes every row a permanent
    # non-duplicate that never merges with the copy held from the real ATS.
    # Their hosts went into all_jobs_build.AGGREGATOR_URL_HOSTS in the same
    # change, which drops them to tier 3 -- (company, title, location) -- so
    # they collapse against the existing copy instead of inflating the master.
    #
    # npo_80000hours is the exception and needs no host entry: 274 distinct
    # apply hosts, led by anthropic.com, job-boards.greenhouse.io and
    # openai.com.  It is employer-direct.
    "npo_80000hours.jsonl", "npo_chronicle_hied.jsonl",
    "npo_foundationlist.jsonl", "npo_idealist.jsonl", "npo_impactpool.jsonl",
    "npo_insidehighered.jsonl", "npo_philanthropy.jsonl",
    "npo_workforgood.jsonl",

    # --- added 2026-09-16: Workable's own US job directory -------------------
    # workable_dir_fetch.py walks jobs.workable.com (69k US postings) and keeps
    # only employers the ats-workable chain does NOT already fetch by board --
    # its jobs.workable.com/view/ URLs get a tier-1 key that can never equal
    # the apply.workable.com key, so covered employers would double-count.
    # Measured on the first pages: ~78% of postings were already covered.
    "workable_dir.jsonl",
]
# which sources carry full-time as well as contingent roles
FT_SOURCES = {"bullhorn", "ashby", "smartrecruiters", "recruitee", "breezy",
              "bamboohr", "phenom", "workable", "jazzhr", "teamtailor",
              # 2026-08-20 additions: all six run KEEP_ALL, so they carry
              # full-time as well as contingent roles.
              "ukg", "manatal", "dayforce", "neogov", "radancy",
              "cornerstone",
              # the remote boards + marketplaces, once boards_fetch.py is run
              # with BOARDS_KEEP_ALL=1 (run_all.py passes it)
              "remotive", "remoteok", "himalayas", "jobicy", "workingnomads",
              "arbeitnow", "weworkremotely", "problogger",
              # the npo feed, 2026-09-05.  Sampled titles are salaried roles
              # (General Counsel, Clinical Research Program Manager, Chief
              # Medical Officer), so these carry full-time as well as
              # contingent and must not be booked contingent-only.
              "npo-80000hours", "npo-chronicle", "npo-foundationlist",
              "npo-idealist", "npo-impactpool", "npo-insidehighered",
              "npo-philanthropy", "npo-workforgood",
              # Workable's directory carries every employment type.
              "workable-dir"}


def safe(v):
    return ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v


# ---- compact rows -----------------------------------------------------------
# A row is a LIST over cc.COLUMNS holding the raw r.get(c) values, plus one
# trailing slot for its source set.  It used to be the parsed JSON dict itself
# plus two sets, and at ~7M rows that was the whole problem: a 24-key dict costs
# ~1.2 KB before its values, each row carried a `_sources` AND a `_boards` set,
# and bh_build_combined reached a 34 GB footprint on a 16 GB box.  Measured
# 2026-09-14: swap 19.5 -> 35 GB, free disk 19 -> 3 GB (the swapfiles share the
# data volume), 9,779s for the step against ~712s on a day it fit in RAM.
#
# Output is BYTE-IDENTICAL to the dict version, which is the contract the A/B
# check in the 2026-09-15 plan verifies with cmp:
#   * raw values, never coerced -- 0, False and None stay distinct, and csv
#     writes None as "" exactly as r.get(c, "") did for a missing key;
#   * _merge() keeps the old `not a.get(k) and b.get(k)` fill rule;
#   * one dict for keyed and unkeyed rows, in the same insertion order;
#   * the same sort key with reverse=True (stable, same tie order).
# `_boards` is gone: it was accumulated on every row and merge and never
# written anywhere.
_COLN = len(cc.COLUMNS)
_I = {c: i for i, c in enumerate(cc.COLUMNS)}
_SRC, _BOARD, _JOB_ID = _I["source"], _I["board"], _I["job_id"]
_TITLE, _POSTED, _COMPANY = _I["title"], _I["posted"], _I["company"]
_APPLY, _LOC = _I["apply_url"], _I["location"]
_SRCS = _COLN  # None = just {row[_SRC]}; a set once a merge has happened
_ROW_KEYS = tuple(cc.COLUMNS) + ("\x00sources",)  # never a JSON key -> None
_MERGE_I = tuple(_I[k] for k in (
    "apply_url", "location", "city", "state", "country",
    "salary_min", "salary_max", "salary_currency", "work_mode"))
# Low-cardinality columns whose string values repeat across millions of rows.
# sys.intern is applied ONLY to str values: salary_* arrive as int, float and
# str, remote as bool, job_id often as int.
_INTERN_I = tuple(_I[c] for c in (
    "source", "source_type", "company", "employment_type", "work_mode",
    "location", "city", "state", "country", "remote", "salary_currency",
    "salary_period", "posted", "board", "board_url", "delisted_on")
    if c in _I)


def _row(r):
    row = list(tuple(map(r.get, _ROW_KEYS)))   # exact-size list, no slack
    intern = sys.intern
    for i in _INTERN_I:
        v = row[i]
        if type(v) is str:
            row[i] = intern(v)
    return row


def _merge(a, b):
    sb = b[_SRCS] if b[_SRCS] is not None else (b[_SRC],)
    sa = a[_SRCS]
    if sa is None:
        a[_SRCS] = {a[_SRC], *sb}
    else:
        sa.update(sb)
    if (b[_POSTED] or "") > (a[_POSTED] or ""):
        a[_POSTED] = b[_POSTED]
    for i in _MERGE_I:
        if not a[i] and b[i]:
            a[i] = b[i]


def _cross_key(row):
    """The (company, title, apply-url-or-location) identity, as a 16-byte
    blake2b digest of its repr.  The tuple itself held three freshly lowered
    strings per row -- ~3 GB at 7M rows -- for a key whose only use is dict
    membership.  128 bits makes a collision ~1e-25 at this size, and repr() of
    a tuple of str is unambiguous, so two different keys cannot hash alike."""
    comp = (row[_COMPANY] or "").strip().lower()
    title = (row[_TITLE] or "").strip().lower()
    au = (row[_APPLY] or "").strip().lower()
    k = (comp, title, au or "loc:" + (row[_LOC] or "").lower())
    return hashlib.blake2b(repr(k).encode("utf-8", "surrogatepass"),
                           digest_size=16).digest()


def warn_unlisted():
    """Name every contract_norm/*.jsonl that INCLUDE does not read.

    INCLUDE is deliberately an explicit list, not a glob, so that superseded
    contract-only files stay out.  The cost of that choice is that a NEW pipe
    whose output filename nobody added here contributes silently nothing —
    and the build still reports a healthy-looking total, so the omission is
    invisible.  This turns that failure mode into a printed line.
    """
    listed = set(INCLUDE)
    # Deliberately superseded by their *_all.jsonl counterparts (see the module
    # docstring) — skipped on purpose, so they must not be reported as strays.
    known_skip = {"boards.jsonl", "phenom.jsonl", "bullhorn.jsonl"}
    stray = []
    for path in sorted(glob.glob(os.path.join(cc.NORM_DIR, "*.jsonl"))):
        name = os.path.basename(path)
        if (name in listed or name in known_skip or ".prev." in name
                or name.startswith("ats_")):
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                n = sum(1 for _ in f)
        except OSError:
            n = -1
        stray.append((name, n))
    for name, n in stray:
        print(f"  !! NOT IN INCLUDE: {name} ({n} rows) — add it or it is "
              f"silently dropped", flush=True)
    return stray


def main():
    by_id = {}
    raw = dup_id = 0
    seq = 0   # key for rows with no job_id: unique, never a tuple
    per_file = {}
    warn_unlisted()
    for name in INCLUDE:
        path = os.path.join(cc.NORM_DIR, name)
        if not os.path.exists(path):
            print(f"  (missing {name})", flush=True)
            continue
        n = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                raw += 1
                n += 1
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not (r.get("title") and str(r.get("title")).strip()):
                    continue
                row = _row(r)
                jid = row[_JOB_ID]
                # board-scoped: bamboohr/bullhorn reuse per-board integer ids,
                # so (source, job_id) alone over-merges across agencies.
                k = (row[_SRC], row[_BOARD], str(jid)) if jid else None
                if k and k in by_id:
                    _merge(by_id[k], row)
                    dup_id += 1
                elif k:
                    by_id[k] = row
                else:
                    # A counter, not id(r): the parsed dict is discarded now,
                    # so CPython would hand its address to the next one and
                    # two unkeyed rows would silently overwrite each other.
                    seq += 1
                    by_id[seq] = row
        per_file[name] = n
    after_id = len(by_id)

    by_key = {}
    for row in by_id.values():
        k = _cross_key(row)
        if k in by_key:
            _merge(by_key[k], row)
        else:
            by_key[k] = row
    del by_id
    rows = list(by_key.values())
    del by_key
    for row in rows:
        s = row[_SRCS]
        srcs = sorted(x for x in (s if s is not None else (row[_SRC],)) if x)
        row[_SRCS] = (", ".join(srcs), len(srcs))
    rows.sort(key=lambda r: (r[_POSTED] or "", r[_SRCS][1]), reverse=True)
    print(f"raw {raw} -> id-dedupe {after_id} ({dup_id} dups) -> "
          f"deduped {len(rows)}", flush=True)

    # ATOMIC, like all_jobs_build.py:1073-1079.  This opened OUT_CSV with "w"
    # directly, so a disk that filled mid-write (it reached 3 GB free on
    # 2026-09-14) left a TRUNCATED master for all_jobs_build to merge.  Now a
    # failed write leaves the previous master exactly as it was.
    tmp = OUT_CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLS)
        for row in rows:
            w.writerow(row[:_COLN] + list(row[_SRCS]))
    os.replace(tmp, OUT_CSV)
    print(f"wrote {OUT_CSV}", flush=True)

    # THE WORKBOOK IS OFF BY DEFAULT as of 2026-09-04.  Nothing in this repo
    # opens FT_Contract_Portals_<date>.xlsx -- the only two load_workbook call
    # sites are triage_export.py:380 and vet_build.py:195, both on other files
    # -- and writing it cost ~26 min of a strictly serial build tail plus a
    # multi-GB TMPDIR spill on a volume that was 98% full.
    #
    # RETURN HERE, NOT AFTER THE STATS LOOP.  The first version of this guard
    # sat just above `wb = Workbook(...)`, which left the by_source/by_type/
    # by_board accumulation below it still running -- a full pass over every
    # row whose only consumers are the README/Summary/Boards sheets we are
    # about to skip.  Measured 2026-09-05 with 6,023,872 rows: the step still
    # spent ~19 min after the CSV was already on disk, and saved only 393s of
    # the ~26 min it should have.  Everything that matters downstream is
    # OUT_CSV, and it is written immediately above.
    #
    # PORTALS_XLSX=1 brings the workbook back for one run.
    if os.environ.get("PORTALS_XLSX") != "1":
        print(f"skipping {os.path.basename(OUT_XLSX)} "
              f"(PORTALS_XLSX=1 to write it) — "
              f"{len(rows):,} rows are in {os.path.basename(OUT_CSV)}",
              flush=True)
        return

    # Rows are lists now (see _row); index them by column name from here on.
    _ET, _BURL, _REMOTE = (_I["employment_type"], _I["board_url"],
                           _I["remote"])
    by_source, by_type, by_board = {}, {}, {}
    ft = contingent = 0
    for r in rows:
        s = r[_SRC] or ""
        by_source[s] = by_source.get(s, 0) + 1
        et = r[_ET] or "(none)"
        by_type[et] = by_type.get(et, 0) + 1
        if et in ("Full-time", "Part-time", "Internship", "Volunteer"):
            ft += 1
        else:
            contingent += 1
        bk = (s, r[_BOARD] or "")
        st = by_board.setdefault(bk, [0, r[_BURL] or "", ""])
        st[0] += 1
        if (r[_POSTED] or "") > st[2]:
            st[2] = r[_POSTED] or ""

    wb = Workbook(write_only=True)
    bold = Font(bold=True)

    def hdr(ws, names):
        out = []
        for nm in names:
            c = WriteOnlyCell(ws, value=nm)
            c.font = bold
            out.append(c)
        return out

    ws = wb.create_sheet("README")
    ws.column_dimensions["A"].width = 26
    ws.column_dimensions["B"].width = 118
    n_remote = sum(1 for r in rows if r[_REMOTE])
    lines = [
        ("Full-time & contract jobs — portal crawl master", ""),
        ("Snapshot (UTC)", SNAP),
        ("", ""),
        ("Unique jobs (deduped)", len(rows)),
        ("Raw rows harvested", raw),
        ("Full-time / part-time / intern", ft),
        ("Contract / temp / freelance / etc.", contingent),
        ("Remote", n_remote),
        ("Distinct sources", len(by_source)),
        ("", ""),
        ("What this is", "Every full-time AND contingent role reachable through "
         "open, unauthenticated feeds across staffing ATSs (Bullhorn OSCP), "
         "first-party company ATS boards (Ashby/SmartRecruiters/Recruitee/"
         "Breezy/BambooHR), Allegis staffing brands (Phenom), freelance "
         "marketplaces + remote boards, and the contract slice of the Getro "
         "(VC portfolio) and Lever board crawls."),
        ("Full-time coverage", "Bullhorn, the five ATS platforms, and Phenom "
         "were crawled keeping ALL employment types. Marketplaces (Freelancer, "
         "Braintrust, Himalayas...) are contract/freelance by nature. The "
         "Getro and Lever rows here are the contract slice only; their full "
         "full-time universes are the separate multi-million-row Getro and "
         "Lever masters."),
        ("employment_type", "Full-time / Part-time / Internship, or the "
         "contingent label: Contract, Temp, Freelance, Contract-to-Hire, "
         "Interim, Seasonal, Per Diem, Fractional."),
        ("Dedupe", "Same posting collapses across sources on (company, title, "
         "apply URL) or (company, title, location); 'sources'/'n_sources' show "
         "where it was seen."),
        ("Bullhorn note", "Bullhorn agencies expose jobs only through their "
         "agency portal, so those rows have no public apply URL; the company "
         "column is the portal domain."),
        ("Master CSV", "portals_combined_master.csv holds the complete table."),
    ]
    for a, b in lines:
        ca = WriteOnlyCell(ws, value=a)
        ca.font = bold
        cb = WriteOnlyCell(ws, value=b)
        cb.alignment = Alignment(wrap_text=True, vertical="top")
        ws.append([ca, cb])

    ws = wb.create_sheet("All Jobs")
    widths = [12, 14, 28, 44, 16, 9, 30, 16, 12, 9, 7, 10, 10, 8, 8, 11, 46,
              20, 18, 34, 22, 9]
    for i, wd in enumerate(widths[:len(COLS)], 1):
        ws.column_dimensions[get_column_letter(i)].width = wd
    ws.freeze_panes = "A2"
    n_x = min(len(rows), XLSX_CAP)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(COLS))}{n_x + 1}"
    ws.append(hdr(ws, COLS))
    for r in rows[:XLSX_CAP]:
        ws.append([safe(v) for v in r[:_COLN] + list(r[_SRCS])])
    if len(rows) > XLSX_CAP:
        print(f"NOTE: {len(rows) - XLSX_CAP} rows beyond Excel cap (CSV only)",
              flush=True)

    ws = wb.create_sheet("Summary")
    for col, wd in zip("ABCD", (20, 16, 3, 20)):
        ws.column_dimensions[col].width = wd
    ws.append(hdr(ws, ["by source", "jobs", "", ""]))
    for s, n in sorted(by_source.items(), key=lambda kv: -kv[1]):
        ws.append([s, n])
    ws.append(["", "", "", ""])
    ws.append(hdr(ws, ["by employment_type", "jobs", "", ""]))
    for t, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
        ws.append([t, n])
    ws.append(["", "", "", ""])
    ws.append(hdr(ws, ["rows per source file", "raw", "", ""]))
    for nm, n in per_file.items():
        ws.append([nm, n])
    ws.append(["", "", "", ""])
    ws.append([WriteOnlyCell(ws, value="TOTAL unique"), len(rows)])

    ws = wb.create_sheet("Boards")
    for col, wd in zip("ABCD", (16, 40, 46, 9)):
        ws.column_dimensions[col].width = wd
    ws.freeze_panes = "A2"
    top = sorted(by_board.items(), key=lambda kv: -kv[1][0])[:20000]
    ws.auto_filter.ref = f"A1:D{len(top) + 1}"
    ws.append(hdr(ws, ["source", "board", "board_url", "jobs"]))
    for (s, b), st in top:
        ws.append([s, b, st[1], st[0]])

    wb.save(OUT_XLSX)
    print(f"wrote {OUT_XLSX}", flush=True)


if __name__ == "__main__":
    main()
