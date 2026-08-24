#!/usr/bin/env python3
"""triage_load.py — MA_US_Remote_<date>.csv -> triage.db, without ever losing a
decision you already made.

    python3 triage_load.py [--csv PATH] [--db PATH] [--feed NAME] [--stats]

Run it after every rebuild of the corpus.  It is idempotent: re-running on the
same CSV changes nothing but `last_seen`.

`--feed` names which corpus is being loaded and defaults to `main`.  The other
feed is `contract` (contract_pool.py -> Contract_Pool_<date>.csv), which fills
the contract lane:

    python3 triage_load.py --feed contract

Feeds share one `jobs` table on purpose — decisions, company kills, ct_key
carry-forward and the apply queue then work identically for every feed with no
second code path.  What the feed DOES change is the scope of retire(); see the
note on that function, because an unscoped sweep lets each feed silently kill
the other.

The whole point of this file is carry-forward.  `vet_build.py` tried the same
idea with a CSV ledger keyed on `job_key` and its ledger is still empty, so the
carry-forward path here is the part that actually has to be right:

  * `decisions` is never deleted or rewritten by the loader.  Only the server
    (an explicit undo) and an unblock remove rows from it.
  * Rows that vanish from the corpus go `live=0`.  They leave the queue and keep
    their verdict, so a job you approved can't silently come back as new.
  * **job_key is not stable.**  ma_remote_build.py collapses duplicates
    on company+title and keeps whichever source copy wins the sort, so the
    surviving job_key for one job changes between rebuilds when a different
    source wins.  Every decision therefore also stores `ct_key`
    (company|title), and a new job_key that matches a decided ct_key inherits
    that decision.  Without this the queue quietly refills with jobs you already
    judged, which is exactly how a triage tool stops getting used.
  * `applications` (what you have actually applied to) rides the same ct_key
    path via carry_forward_applications().  The failure it prevents is worse
    than a refilled queue: a reset 'applied' flag means applying to the same
    job twice.

`triage_blocklist.txt` is the hand-editable mirror of the killed-company list.
The loader reconciles the file INTO the database both ways: add a line and that
company is dead, delete a line and it comes back.  The server rewrites the file
on every kill, so the two never drift.

SIX BUILDERS MUST RUN AFTER EVERY `--feed main` LOAD.  This loader upserts the
derived ranking columns from the CSV, and MA_US_Remote_<date>.csv carries none
of them, so a main load blanks every lane's ranking.  The full order is:

    python3 ma_remote_build.py
    python3 date_basis_index.py   # BEFORE the load — see date_basis.py
    python3 triage_load.py --feed main
    python3 lanes_build.py        # restores lane_tags/score — picks + 11 swaths
    python3 ai_agency_build.py    # restores desk_score     — the staffing lane
    python3 geo_lane.py           # restores local_score    — the commute-ring lane
    python3 level.py              # restores level_score    — the right-level lane
    python3 ai_startup_build.py   # restores startup_score  — the AI startup lane,
                                  # and LAST because it carries the ANALYZE

Skip any of them and the lane it owns goes silently empty or unranked, which is
not a failure you would notice from the UI.
"""
import csv
import datetime as dt
import glob
import json
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import date_basis                                   # noqa: E402
from triage.lanes import AGGREGATORS, counts        # noqa: E402

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "triage.db")
BLOCKLIST = os.path.join(BASE, "triage_blocklist.txt")
# Built by date_basis_index.py: the apply_urls whose `posted` is our crawl date
# rather than the employer's.  Absent is not an error -- the eight mixed
# sources then fall back to their source-level basis, which is the same answer
# for every source except workday/icims/atstail.
BASIS_INDEX = os.path.join(BASE, "date_basis_index.json")


def stamp():
    """A fresh load stamp, to microseconds.

    `retire()` finds vanished rows by asking which ones this load did NOT
    touch, so the stamp has to be unique per load.  A module-level constant
    frozen at import time silently retires nothing when two loads share a
    process, and two runs inside the same second would collide at
    second precision.
    """
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

# CSV column -> jobs column.  Everything else in the 36-column CSV is dropped;
# triage needs what fits on a card, not the whole row (triage_export.py goes
# back to the CSV for the full record).
FIELDS = [
    "bucket", "us_basis", "startup_tier", "startup_evidence", "ai_band",
    "ai_tier", "company", "title", "employment_type", "seniority",
    "work_mode", "remote", "location", "posted", "salary_min", "salary_max",
    "salary_currency", "salary_period", "skills", "company_stage",
    "company_size", "company_industry", "apply_url", "job_url", "source",
]

# Prefix of ma_remote_build.INFERRED_BASIS — matched as a PREFIX on purpose,
# so the parenthetical part of that label can change without silently zeroing
# every basis_weak here.  Not imported from ma_remote_build: that module pulls
# in openpyxl and the whole sheet-writing stack for one string constant.
WEAK_BASIS = "inferred"

DDL = """
CREATE TABLE IF NOT EXISTS jobs (
  job_key TEXT PRIMARY KEY,
  ct_key TEXT, company_norm TEXT,
  is_agg INTEGER DEFAULT 0,       -- source republishes other boards
  rank_tier INTEGER DEFAULT 9,    -- ai_tier as int, 9 = not an AI role
  basis_weak INTEGER DEFAULT 0,   -- 1 = us_basis was INFERRED, not proven
                                  -- (ma_remote_build.INFERRED_BASIS).  Kept
                                  -- as an int for the same reason as is_agg:
                                  -- lanes.ORDER_BY sorts every queue on it and
                                  -- LIKE 'inferred%' over 200k rows is not
                                  -- something to do per keystroke.
  date_basis TEXT DEFAULT '',     -- who produced `posted`: employer / network /
                                  -- aggregator / first_seen.  See date_basis.py
                                  -- -- 57% of the rows dated 2026-08-18 were
                                  -- not posted that day, they were INGESTED
                                  -- that day, which is why fresh-looking rows
                                  -- open as expired.
  date_weak INTEGER DEFAULT 0,    -- 1 = date_basis is not 'employer'.  An int
                                  -- beside the text for exactly the reason
                                  -- basis_weak is one: every lane's ORDER BY
                                  -- sorts on it, per keystroke, over 200k rows.
  feed TEXT DEFAULT 'main',       -- which corpus this row came from
  score REAL DEFAULT 0,           -- feed-specific rank (contract: 0-100 blend)
  score_why TEXT DEFAULT '',      -- plain-English reason for that score
  money_score REAL DEFAULT 0,     -- quick-hire + pay rank (fast-money lane)
  money_why TEXT DEFAULT '',
  geo_score REAL DEFAULT 0,       -- recency+fit rank for a metro lane, written
  geo_why TEXT DEFAULT '',        -- by a per-metro builder.  Its own column, and
                                  -- not `score`, because lanes_build.py owns
                                  -- score/score_why on the main feed and
                                  -- rewrites them for every row it assigns to
                                  -- one of its twelve lanes — a South FL row
                                  -- that also matched a swath would have its
                                  -- ranking silently overwritten by whichever
                                  -- builder ran last.
  desk_score REAL DEFAULT 0,      -- staffing-desk rank for the `aiagency` lane,
  desk_why TEXT DEFAULT '',       -- written by ai_agency_build.py.  Its own
                                  -- column for the same reason geo_score is
                                  -- one, and the collision is real here: these
                                  -- rows are AI reqs, so lanes_build.py assigns
                                  -- many of them to a swath and would overwrite
                                  -- score/score_why depending on run order.
  lane_tags TEXT DEFAULT '',      -- ',contract,fastmoney,' — which slices
                                  -- this row was selected into.  One row can
                                  -- be in both, and is decided once.
  bucket TEXT, us_basis TEXT, startup_tier TEXT, startup_evidence TEXT,
  ai_band TEXT, ai_tier TEXT,
  company TEXT, title TEXT, employment_type TEXT, seniority TEXT,
  work_mode TEXT, remote TEXT, location TEXT, posted TEXT,
  salary_min TEXT, salary_max TEXT, salary_currency TEXT, salary_period TEXT,
  skills TEXT, company_stage TEXT, company_size TEXT, company_industry TEXT,
  apply_url TEXT, job_url TEXT, source TEXT,
  first_seen TEXT, last_seen TEXT, live INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS decisions (
  job_key TEXT PRIMARY KEY,
  ct_key TEXT,
  verdict TEXT,        -- approve | deny | maybe
  reason TEXT,         -- 'manual' | 'company_kill:<company>'
  lane TEXT,
  decided_at TEXT
);
CREATE TABLE IF NOT EXISTS blocked_companies (
  company_norm TEXT PRIMARY KEY,
  company TEXT, blocked_at TEXT, n_killed INTEGER DEFAULT 0
);
-- Only jobs you have ACTED on.  'to_apply' is the absence of a row, exactly
-- as "undecided" is the absence of a `decisions` row (triage/lanes.py), so
-- approving more jobs in triage puts them in the apply queue with no backfill
-- step to remember to run.  See triage/apply_queue.py.
CREATE TABLE IF NOT EXISTS applications (
  job_key TEXT PRIMARY KEY,
  ct_key TEXT,
  status TEXT NOT NULL,   -- applying | applied | skipped | dead
  applied_at TEXT,        -- date only, stamped on the move into 'applied';
                          -- NULL while 'applying' — started is not submitted
  note TEXT DEFAULT '',
  -- What came BACK.  `status` records what our side did; these record what the employer
  -- did, and the two are independent: an application stays 'applied' forever while
  -- its outcome moves acknowledged -> interview.  Without this the funnel is
  -- write-only — 161 applications were sent across ten days with no way to tell
  -- which lane converted.  Written by the /apply page (and by any mail-sync
  -- integration that reads recruiter replies back in).
  outcome TEXT DEFAULT '',    -- '' | acknowledged | rejected | interview | offer | ghost
  outcome_at TEXT,
  updated_at TEXT
);
-- The reference number you can say out loud.  `job_key` is the only other
-- identifier a row has and it averages 95 characters, so quoting a job in
-- conversation has always meant pasting a URL; `J4821` is the same job in five
-- keystrokes.
--
-- KEYED ON IDENTITY, NOT ON ROW, and that is the whole design.  job_key churns
-- 6-8% per rebuild (ma_remote_build collapses duplicates on company+title and
-- keeps whichever source copy wins the sort), so a ref anchored to it would go
-- dead for exactly the population carry_forward() exists to rescue.  `ref_key`
-- is therefore the same ct_key the decisions ride, which means a ref survives a
-- rebuild precisely as well as a verdict does and needs no separate machinery.
--
-- Two consequences worth knowing before reading a ref as "one row": ~1k rows
-- have a blank company and so key on 'jk:'||job_key instead (single-rebuild
-- lived, same as their carry-forward), and 1.6k live ct_keys carry more than
-- one live row, which share a ref because they ARE one job.
--
-- `company`/`title` are frozen at minting so a ref whose row has since been
-- retired still reads as something rather than as a dead number.
CREATE TABLE IF NOT EXISTS refs (
  ref       INTEGER PRIMARY KEY,   -- the number: J4821
  ref_key   TEXT UNIQUE NOT NULL,  -- ct_key, or 'jk:'||job_key when company is blank
  company   TEXT DEFAULT '',
  title     TEXT DEFAULT '',
  minted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(live, is_agg, rank_tier,
                                                  posted);
CREATE INDEX IF NOT EXISTS idx_jobs_ct    ON jobs(ct_key);
CREATE INDEX IF NOT EXISTS idx_jobs_co    ON jobs(company_norm);
CREATE INDEX IF NOT EXISTS idx_jobs_seen  ON jobs(last_seen);
CREATE INDEX IF NOT EXISTS idx_dec_ct     ON decisions(ct_key);
CREATE INDEX IF NOT EXISTS idx_dec_reason ON decisions(reason);
-- The apply queue's driving index.  Without it the planner starts every
-- apply-queue read at `jobs` (183k live rows) and probes `decisions` for each
-- one, when the approved-manual set it wants is a few hundred rows of a 1.3k
-- table.  With it the join runs small-to-large and the page reads in ~2ms
-- rather than ~180 — which only started to matter once the filter bar made it
-- eleven reads per keystroke instead of two.
CREATE INDEX IF NOT EXISTS idx_dec_verdict ON decisions(verdict, reason,
                                                        job_key);
CREATE INDEX IF NOT EXISTS idx_app_ct     ON applications(ct_key);
CREATE INDEX IF NOT EXISTS idx_app_status ON applications(status);
"""

_WS = re.compile(r"\s+")


def norm_co(s):
    """Casefolded, whitespace-collapsed company name — the blocklist key."""
    return _WS.sub(" ", (s or "").strip()).casefold()


def ct_key(company, title):
    """company|title, blank when the company is blank.

    962 rows in the corpus have no company at all; keying those on title alone
    would let two unrelated employers' "Software Engineer" share a verdict.
    Blank company means no ct_key and therefore no carry-forward — the job_key
    path still covers them.
    """
    c = norm_co(company)
    if not c:
        return ""
    return c + "|" + _WS.sub(" ", (title or "").strip()).casefold()


def connect(path=DB):
    con = sqlite3.connect(path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


# Indexes over columns migrate() may have to add.  They cannot live in DDL:
# CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so on a
# pre-`feed` triage.db the CREATE INDEX would run against a column that does
# not exist yet and take the whole connection down with it.
POST_MIGRATE_DDL = """
-- ref -> row, for triage/ref.py.  Partial: a ref of 0 means "not minted yet"
-- and nobody ever looks one up, so the index covers only rows that can answer.
CREATE INDEX IF NOT EXISTS idx_jobs_ref ON jobs(ref) WHERE ref > 0;
CREATE INDEX IF NOT EXISTS idx_jobs_feed ON jobs(feed, live, score);
-- Mirrors lanes.ORDER_BY once basis_weak joined it.  idx_jobs_queue is left in
-- place: it still serves reads that do not order (the lane COUNT(*)s), and
-- rebuilding it would rewrite the index on every existing row for no gain.
-- ...and once more when date_weak joined it (date_basis.py).  Every index
-- below gained the same column in the same position its lane's ORDER BY did;
-- leaving them as they were would have taken the ordering off the index and
-- back to a sort over 288k rows on every keystroke.  DROP + a NEW NAME rather
-- than editing in place, because CREATE INDEX IF NOT EXISTS will not alter an
-- index that already exists -- it would silently keep the old column list.
-- Both statements are idempotent, so this costs one rebuild, not one per load.
DROP INDEX IF EXISTS idx_jobs_order;
CREATE INDEX IF NOT EXISTS idx_jobs_order2 ON jobs(live, is_agg, basis_weak,
                                                   rank_tier, date_weak,
                                                   posted);
-- PARTIAL, and it has to be.  The `aistartup` lane's whole predicate is
-- `startup_score > 0`, with no bucket or tier term to narrow it the way `sfl`
-- is narrowed by bucket='South FL' — so without an index its COUNT(*) scans all
-- 288k rows on every menu render.  Partial because ~350 rows are non-zero: the
-- index is the lane, and it costs nothing on the other 99.9% of the table.
DROP INDEX IF EXISTS idx_jobs_aistartup;
CREATE INDEX IF NOT EXISTS idx_jobs_aistartup2 ON jobs(startup_score DESC,
                                                      is_agg, basis_weak,
                                                      date_weak, posted)
                                              WHERE startup_score > 0;
-- Partial for the same reason, and note what is NOT in it: `is_agg`.  The
-- `aiagency` lane deliberately does not sort on it (lanes.AIAGENCY_TERMS), so
-- carrying it here would add a column the ORDER BY never reads.
DROP INDEX IF EXISTS idx_jobs_aiagency;
CREATE INDEX IF NOT EXISTS idx_jobs_aiagency2 ON jobs(desk_score DESC,
                                                     basis_weak, date_weak,
                                                     posted)
                                             WHERE desk_score > 0;
-- Partial for the third time, and this lane is the largest of the three: ~3.6k
-- rows out of 250k live, predicate `local_score > 0` with no bucket term to
-- narrow it either.  `is_agg` IS carried here, unlike idx_jobs_aiagency --
-- lanes.LOCAL_TERMS sorts on it, and half that lane is aggregator republish, so
-- it is doing real work rather than riding along.
DROP INDEX IF EXISTS idx_jobs_local;
CREATE INDEX IF NOT EXISTS idx_jobs_local2 ON jobs(local_score DESC,
                                                   is_agg, date_weak, posted)
                                           WHERE local_score > 0;
-- Partial for the fourth time, and the largest of the four: `level` admits a
-- rung band across the whole corpus rather than a geography or a tier, so its
-- predicate `level_score > 0` has nothing else to narrow it either.  Carries
-- `is_agg` for the same reason idx_jobs_local2 does -- lanes.LEVEL_TERMS sorts
-- on it, and most of the wide remote surface this lane reaches is republish --
-- and `basis_weak`, which LOCAL_TERMS omits: this lane admits inferred-US rows
-- where that one cannot, so the column is doing real work rather than riding
-- along.  Column list and position both mirror LEVEL_TERMS; take either out and
-- the ordering comes off the index and back to a sort over 250k rows.
CREATE INDEX IF NOT EXISTS idx_jobs_level ON jobs(level_score DESC, is_agg,
                                                  basis_weak, date_weak,
                                                  posted)
                                          WHERE level_score > 0;

-- Partial for the fifth time, and the SMALLEST of the five: `fundfinance`
-- needs both a domain and a function match, so its predicate `ff_score > 0`
-- selects a few hundred rows out of 250k and the index is what keeps the lane
-- menu's COUNT(*) off a full scan.  Column list and position mirror
-- lanes.FF_TERMS exactly; take either out and the ordering comes off the index
-- and back to a sort.  `basis_weak` is carried because the lane admits
-- inferred-US rows -- most of the private-markets software surface it reaches
-- is remote-tagged with no country -- and demoting them is the point.
CREATE INDEX IF NOT EXISTS idx_jobs_ff ON jobs(ff_score DESC, is_agg,
                                               basis_weak, date_weak, posted)
                                       WHERE ff_score > 0;

-- Partial for the sixth time, and the same shape for the same reason: the
-- `legalai` predicate selects ~1.3k rows of 250k, so the partial is what keeps
-- the lane menu's COUNT(*) off a full scan.  Column list and position mirror
-- lanes.LEGALAI_TERMS exactly.  `basis_weak` is carried because this lane is
-- overwhelmingly remote -- 89% of it -- and a remote row whose US basis was
-- inferred rather than stated is worth less than one that said so.
--
-- NOTE these partial indexes are inert without sqlite_stat1: triage.db shipped
-- for weeks with no ANALYZE and the lane menu took 12.8s because SQLite would
-- not use them.  The last builder in the chain carries the ANALYZE, which is
-- why the chain's order is fixed rather than incidental.
CREATE INDEX IF NOT EXISTS idx_jobs_legalai ON jobs(legalai_score DESC, is_agg,
                                                    basis_weak, date_weak,
                                                    posted)
                                            WHERE legalai_score > 0;
"""


def migrate(con):
    """Add columns a database written by an older version is missing.

    CREATE TABLE IF NOT EXISTS does nothing to a table that already exists, so
    a triage.db built before `feed` would 500 every read the moment the server
    started selecting it.  Existing rows are the main corpus by definition.
    """
    have = {r["name"] for r in con.execute("PRAGMA table_info(jobs)")}
    for col, ddl in (("feed", "feed TEXT DEFAULT 'main'"),
                     ("score", "score REAL DEFAULT 0"),
                     ("score_why", "score_why TEXT DEFAULT ''"),
                     ("money_score", "money_score REAL DEFAULT 0"),
                     ("money_why", "money_why TEXT DEFAULT ''"),
                     ("geo_score", "geo_score REAL DEFAULT 0"),
                     ("geo_why", "geo_why TEXT DEFAULT ''"),
                     ("startup_score", "startup_score REAL DEFAULT 0"),
                     ("startup_why", "startup_why TEXT DEFAULT ''"),
                     ("desk_score", "desk_score REAL DEFAULT 0"),
                     ("desk_why", "desk_why TEXT DEFAULT ''"),
                     ("local_score", "local_score REAL DEFAULT 0"),
                     ("local_why", "local_why TEXT DEFAULT ''"),
                     ("level_score", "level_score REAL DEFAULT 0"),
                     ("level_why", "level_why TEXT DEFAULT ''"),
                     ("ff_score", "ff_score REAL DEFAULT 0"),
                     ("ff_why", "ff_why TEXT DEFAULT ''"),
                     ("legalai_score", "legalai_score REAL DEFAULT 0"),
                     ("legalai_why", "legalai_why TEXT DEFAULT ''"),
                     ("lane_tags", "lane_tags TEXT DEFAULT ''"),
                     ("date_basis", "date_basis TEXT DEFAULT ''"),
                     ("date_weak", "date_weak INTEGER DEFAULT 0"),
                     ("basis_weak", "basis_weak INTEGER DEFAULT 0"),
                     # Denormalised from `refs` by mint_refs() rather than
                     # joined, because lanes.queue_sql() is `SELECT ... FROM
                     # jobs` with no join and every one of the thirty lanes
                     # goes through it.  A constant DEFAULT is a schema-only
                     # ALTER in SQLite, so this costs nothing on 372k rows.
                     ("ref", "ref INTEGER DEFAULT 0")):
        if col not in have:
            con.execute(f"ALTER TABLE jobs ADD COLUMN {ddl}")
    con.execute("UPDATE jobs SET feed = 'main' WHERE feed IS NULL OR feed = ''")

    # `applications` needs the same treatment, and for the same reason: the outcome
    # columns were added after 188 applications had already been recorded, and
    # CREATE TABLE IF NOT EXISTS will not add them to a table that exists.
    have_app = {r["name"] for r in con.execute("PRAGMA table_info(applications)")}
    for col, ddl in (("outcome", "outcome TEXT DEFAULT ''"),
                     ("outcome_at", "outcome_at TEXT")):
        if col not in have_app:
            con.execute(f"ALTER TABLE applications ADD COLUMN {ddl}")
    con.commit()
    con.executescript(POST_MIGRATE_DDL)
    con.commit()


def init(path=DB):
    con = connect(path)
    con.executescript(DDL)
    con.commit()
    migrate(con)
    return con


def newest_csv():
    hits = sorted(glob.glob(os.path.join(BASE, "MA_US_Remote_*.csv")))
    if not hits:
        sys.exit("no MA_US_Remote_*.csv found — run ma_remote_build.py first")
    return hits[-1]


def newest_contract_csv():
    hits = sorted(glob.glob(os.path.join(BASE, "Contract_Pool_*.csv")))
    if not hits:
        sys.exit("no Contract_Pool_*.csv found — run contract_pool.py first")
    return hits[-1]


# --------------------------------------------------------------------- blocks
def write_blocklist(con):
    """Mirror blocked_companies out to the hand-editable text file."""
    rows = con.execute("SELECT company, company_norm FROM blocked_companies "
                       "ORDER BY company COLLATE NOCASE").fetchall()
    with open(BLOCKLIST, "w", encoding="utf-8") as f:
        f.write("# Companies killed in triage. One per line; '#' comments.\n"
                "# Delete a line and triage_load.py brings that company back.\n"
                "# Matching is case- and whitespace-insensitive.\n")
        for r in rows:
            f.write(r["company"] + "\n")
    return len(rows)


def sync_blocklist(con):
    """Reconcile the text file into the table, in both directions.

    The file wins: a line you deleted by hand unblocks that company and drops
    the company_kill denials it caused.  Verdicts you gave that company by hand
    are `reason='manual'` and are left alone.
    """
    if not os.path.exists(BLOCKLIST):
        return write_blocklist(con), 0, 0
    want = {}
    with open(BLOCKLIST, encoding="utf-8") as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if line:
                want[norm_co(line)] = line
    have = {r["company_norm"]: r["company"] for r in
            con.execute("SELECT company_norm, company FROM blocked_companies")}

    added = [k for k in want if k not in have]
    removed = [k for k in have if k not in want]
    if added:
        con.executemany(
            "INSERT OR IGNORE INTO blocked_companies"
            "(company_norm, company, blocked_at) VALUES (?,?,?)",
            [(k, want[k], stamp()) for k in added])
    if removed:
        con.executemany("DELETE FROM blocked_companies WHERE company_norm=?",
                        [(k,) for k in removed])
        # Undo only the denials the block itself created.
        con.executemany(
            "DELETE FROM decisions WHERE reason = 'company_kill:' || ? "
            "AND job_key IN (SELECT job_key FROM jobs WHERE company_norm=?)",
            [(have[k], k) for k in removed])
    con.commit()
    return len(want), len(added), len(removed)


# ---------------------------------------------------------------------- load
def load_basis_index():
    """-> set of apply_urls whose `posted` is our crawl date, or an empty set.

    Missing file is not fatal: the eight mixed sources then resolve to their
    source-level basis (EMPLOYER), which is right for five of them and
    optimistic for workday/icims/atstail.  The count is printed either way so
    a silently-absent index cannot look like a clean run.
    """
    try:
        with open(BASIS_INDEX) as f:
            urls = json.load(f).get("fabricated_apply_urls") or []
    except FileNotFoundError:
        print(f"  ! {os.path.basename(BASIS_INDEX)} missing — workday/icims/"
              f"atstail dates will read as employer-published. "
              f"Run date_basis_index.py.")
        return set()
    except (ValueError, AttributeError) as e:
        print(f"  ! {os.path.basename(BASIS_INDEX)} unreadable ({e}) — ignored")
        return set()
    return set(urls)


def load(con, path, mark, feed="main"):
    csv.field_size_limit(10 ** 9)
    cols = (["job_key", "ct_key", "company_norm", "is_agg", "rank_tier",
             "basis_weak", "date_basis", "date_weak", "feed", "score",
             "score_why", "money_score",
             "money_why", "geo_score", "geo_why", "startup_score",
             "startup_why", "desk_score", "desk_why", "local_score",
             "local_why", "level_score", "level_why", "lane_tags"] + FIELDS)
    ins = (f"INSERT INTO jobs ({','.join(cols)}, first_seen, last_seen, live) "
           f"VALUES ({','.join('?' * len(cols))},?,?,1) "
           f"ON CONFLICT(job_key) DO UPDATE SET "
           + ", ".join(f"{c}=excluded.{c}" for c in cols[1:])
           + ", last_seen=excluded.last_seen, live=1")

    print(f"loading {os.path.basename(path)} "
          f"({os.path.getsize(path) / 1e6:.0f} MB) ...", flush=True)
    fabricated = load_basis_index()
    print(f"  date-basis index: {len(fabricated):,} apply_urls with a "
          f"fabricated posted date", flush=True)
    n = 0
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        hdr = next(r)
        idx = {c: i for i, c in enumerate(hdr)}
        missing = [c for c in FIELDS + ["job_key"] if c not in idx]
        if missing:
            sys.exit(f"CSV is missing expected columns: {missing}")
        pos = [idx[c] for c in FIELDS]
        # Optional: only the scored feeds carry these.
        i_score, i_why = idx.get("score"), idx.get("score_why")
        i_mscore, i_mwhy = idx.get("money_score"), idx.get("money_why")
        i_gscore, i_gwhy = idx.get("geo_score"), idx.get("geo_why")
        i_sscore, i_swhy = idx.get("startup_score"), idx.get("startup_why")
        i_dscore, i_dwhy = idx.get("desk_score"), idx.get("desk_why")
        i_lscore, i_lwhy = idx.get("local_score"), idx.get("local_why")
        i_vscore, i_vwhy = idx.get("level_score"), idx.get("level_why")
        i_tags = idx.get("lane_tags")

        def opt_num(row, i):
            if i is None:
                return 0.0
            try:
                return float(row[i])
            except ValueError:
                return 0.0

        def opt_txt(row, i):
            return row[i] if i is not None else ""

        def batches():
            nonlocal n
            buf = []
            for row in r:
                if len(row) < len(hdr):
                    continue
                vals = [row[p] for p in pos]
                co, ti = row[idx["company"]], row[idx["title"]]
                # The numeric AI strength is the first character of `ai_band`
                # ("1 Claude / agentic / FDE").  `ai_tier` is a text label
                # ("Core AI Building"), NOT a number — reading it here silently
                # ranks every AI role as 9 and empties the AI lane.
                band = row[idx["ai_band"]].strip()[:1]
                # `posted` provenance.  is_agg above is NOT a substitute: it
                # comes from startup_build.EXCL_SRC, which calls workday and
                # icims aggregators and does not mention getro at all.  See the
                # warning at the top of date_basis.py.
                dbasis = date_basis.basis_of(
                    row[idx["source"]],
                    posted=row[idx["posted"]],
                    fabricated=row[idx["apply_url"]].strip() in fabricated)
                buf.append(tuple(
                    [row[idx["job_key"]], ct_key(co, ti), norm_co(co),
                     1 if row[idx["source"]] in AGGREGATORS else 0,
                     int(band) if band.isdigit() else 9,
                     1 if row[idx["us_basis"]].startswith(WEAK_BASIS) else 0,
                     dbasis, date_basis.is_weak(dbasis),
                     feed, opt_num(row, i_score), opt_txt(row, i_why),
                     opt_num(row, i_mscore), opt_txt(row, i_mwhy),
                     opt_num(row, i_gscore), opt_txt(row, i_gwhy),
                     opt_num(row, i_sscore), opt_txt(row, i_swhy),
                     opt_num(row, i_dscore), opt_txt(row, i_dwhy),
                     opt_num(row, i_lscore), opt_txt(row, i_lwhy),
                     opt_num(row, i_vscore), opt_txt(row, i_vwhy),
                     opt_txt(row, i_tags)]
                    + vals + [mark, mark]))
                n += 1
                if len(buf) >= 5000:
                    yield from buf
                    buf = []
            yield from buf

        con.execute("BEGIN")
        con.executemany(ins, batches())
        con.commit()
    return n


def retire(con, mark, feed="main"):
    """Rows of THIS feed that this load didn't touch are no longer live.

    The feed scope is load-bearing, not tidiness.  Retirement is defined as
    "not touched by the current load", so an unscoped sweep would let each
    corpus kill the other: loading the contract pool would set live=0 on all
    178k main rows, and the next `python3 triage_load.py` would empty the
    contract lane right back.  Both would keep their verdicts and silently
    vanish from every queue — the same failure carry_forward() exists to
    prevent, arriving from the opposite direction.
    """
    cur = con.execute("UPDATE jobs SET live=0 "
                      "WHERE last_seen <> ? AND live=1 AND feed = ?",
                      (mark, feed))
    con.commit()
    return cur.rowcount


def carry_forward(con):
    """Give a decided ct_key's verdict to a new job_key for the same job.

    SQLite's documented bare-column rule applies to the inner query: with a
    MAX() aggregate, the other selected columns come from the row that MAX
    matched, so a ct_key decided more than once carries its newest verdict.
    """
    cur = con.execute("""
        INSERT OR IGNORE INTO decisions
            (job_key, ct_key, verdict, reason, lane, decided_at)
        SELECT j.job_key, j.ct_key, d.verdict, d.reason, d.lane, d.decided_at
        FROM jobs j
        JOIN (SELECT ct_key, verdict, reason, lane, MAX(decided_at) decided_at
              FROM decisions WHERE ct_key <> '' GROUP BY ct_key) d
          ON d.ct_key = j.ct_key
        WHERE j.live = 1 AND j.ct_key <> ''
          AND NOT EXISTS (SELECT 1 FROM decisions x WHERE x.job_key = j.job_key)
    """)
    con.commit()
    return cur.rowcount


def carry_forward_applications(con):
    """Give a ct_key's application status to a new job_key for the same job.

    The exact counterpart of carry_forward() and just as load-bearing: a
    rebuild that moves a job to a new job_key would otherwise reset every
    'applied' flag to 'to apply' and you would apply to the same job twice.
    Same bare-column-with-MAX() idiom, so a ct_key worked more than once
    carries its newest status.

    The predecessor row is left in place rather than deleted — it is what
    proves the old key was worked.  That does mean one ct_key can hold two
    rows, which is why every read in triage/apply_queue.py joins `jobs` and
    filters live = 1.
    """
    cur = con.execute("""
        INSERT OR IGNORE INTO applications
            (job_key, ct_key, status, applied_at, note, outcome, outcome_at, updated_at)
        SELECT j.job_key, j.ct_key, a.status, a.applied_at, a.note,
               a.outcome, a.outcome_at, a.updated_at
        FROM jobs j
        JOIN (SELECT ct_key, status, applied_at, note, outcome, outcome_at,
                     MAX(updated_at) updated_at
              FROM applications WHERE ct_key <> '' GROUP BY ct_key) a
          ON a.ct_key = j.ct_key
        WHERE j.live = 1 AND j.ct_key <> ''
          AND NOT EXISTS (SELECT 1 FROM applications x
                          WHERE x.job_key = j.job_key)
    """)
    con.commit()
    return cur.rowcount


# The corpus behind the scout lane.  Optional: a fresh clone has no corpus.db
# and must still load, so every use of this is guarded by os.path.exists.
CORPUS_DB = os.path.join(BASE, "corpus.db")

# The one definition of a ref's key, used against BOTH databases.
#
# Note it calls tl_ct_key(company, title) rather than reading the stored ct_key
# column, and that is not redundancy.  corpus_load.py builds corpus.ct_key
# as norm_co(co)|title.strip().casefold() and skips the internal-whitespace
# collapse that ct_key() applies, so the two disagree on 7,839 of 978,443 rows
# -- titles carrying a double space, a tab or a newline.  Keying the scout half
# on the stored column and the triage half on ours would hand those 7,839 jobs
# two different refs for one job, which is the exact failure this whole table
# exists to prevent.  Computing both sides through one function costs a UDF call
# per row, once per load, and cannot drift.
def _ref_key_sql(alias=""):
    p = (alias + ".") if alias else ""
    ct = f"tl_ct_key({p}company, {p}title)"
    return f"CASE WHEN {ct} = '' THEN 'jk:' || {p}job_key ELSE {ct} END"


def mint_refs(con, corpus=None):
    """Give every job a short number you can say out loud.  Idempotent.

    Runs after the two carry_forwards because it wants the post-retire, post-
    upsert view of `jobs`, and NOT feed-scoped the way retire() is: a ref is an
    identity and identities do not belong to a feed, so the --feed main pass
    mints for the contract rows too and the --feed contract pass is a near
    no-op.  Cheaper than scoping it, and it means a contract row is never
    briefly unnumbered between the two halves of triage_chain.sh.

    Mints the UNION of jobs and corpus.db.  Measured 2026-08-24: 322,017
    distinct keys in jobs, 569,292 in corpus, 142,724 shared -- so minting from
    jobs alone would leave three quarters of the scout lane unbadged, and
    minting both costs nothing in ref length because 322,017 and 748,585 are
    both six digits.  The ledger lives here and never in corpus.db, which
    corpus_load.py deletes and recreates on every build (its rowids renumber
    completely -- 0 of the first 120,000 job_keys survived one rebuild).

    Returns (minted, repointed).
    """
    corpus = CORPUS_DB if corpus is None else corpus
    con.create_function("tl_ct_key", 2, ct_key, deterministic=True)
    today = dt.date.today().isoformat()

    before = con.execute("SELECT COUNT(*) FROM refs").fetchone()[0]

    # ORDER BY first_seen, so refs minted from here on climb with age.  INSERT
    # OR IGNORE plus the UNIQUE on ref_key does the deduping, so the first row
    # a key appears on is the one that names it.
    #
    # Do NOT read this as "a low ref means an old job" across the whole table:
    # 48% of live rows share the single first_seen of the initial bulk load, and
    # within that block the tie breaks on job_key, i.e. source-prefix order,
    # which is arbitrary.  Nothing depends on the ordering either way -- SQLite
    # assigns implicit rowids in the order the SELECT yields rows, which is
    # dependable in practice and not promised by the docs, so it is a nicety and
    # must never be asserted in a test.
    con.execute(f"""
        INSERT OR IGNORE INTO refs (ref_key, company, title, minted_at)
        SELECT {_ref_key_sql()}, company, title, ?
          FROM jobs ORDER BY first_seen, job_key
    """, (today,))

    if os.path.exists(corpus):
        con.execute("ATTACH DATABASE ? AS cdb",
                    (f"file:{corpus}?mode=ro",))
        try:
            con.execute(f"""
                INSERT OR IGNORE INTO refs (ref_key, company, title, minted_at)
                SELECT {_ref_key_sql()}, company, title, ?
                  FROM cdb.corpus ORDER BY rowid
            """, (today,))
        finally:
            con.commit()
            con.execute("DETACH DATABASE cdb")
    con.commit()
    minted = con.execute("SELECT COUNT(*) FROM refs").fetchone()[0] - before

    # Denormalise onto jobs.  Two things about this UPDATE:
    #
    # It carries NO `ref = 0` guard.  job_key is URL-derived so it survives an
    # upstream title edit, but ct_key does not, and load()'s upsert writes the
    # new ct_key onto the same row -- guarding on 0 would leave that row wearing
    # the ref of an identity it no longer has, which is worse than wearing none.
    #
    # And it goes through a temp table rather than a correlated subquery so the
    # UDF is called once per row instead of four times.  `jobs.ref <> t.ref`
    # keeps the write down to rows that actually moved.
    con.execute("DROP TABLE IF EXISTS temp._jr")
    con.execute(f"""
        CREATE TEMP TABLE _jr AS
        SELECT j.job_key AS job_key, r.ref AS ref
          FROM jobs j JOIN refs r ON r.ref_key = {_ref_key_sql("j")}
    """)
    con.execute("CREATE INDEX temp.idx_jr ON _jr(job_key)")
    cur = con.execute("""
        UPDATE jobs SET ref = t.ref FROM _jr t
         WHERE t.job_key = jobs.job_key AND jobs.ref <> t.ref
    """)
    repointed = cur.rowcount
    con.execute("DROP TABLE temp._jr")
    con.commit()
    return minted, repointed


def apply_blocks(con, mark=None):
    """Auto-deny live rows from killed companies.

    Runs after carry_forward, so an explicit verdict you gave this exact job
    outranks the blanket block.
    """
    mark = mark or stamp()
    cur = con.execute("""
        INSERT OR IGNORE INTO decisions
            (job_key, ct_key, verdict, reason, lane, decided_at)
        SELECT j.job_key, j.ct_key, 'deny', 'company_kill:' || b.company,
               'load', ?
        FROM jobs j
        JOIN blocked_companies b ON b.company_norm = j.company_norm
        WHERE j.live = 1
          AND NOT EXISTS (SELECT 1 FROM decisions x WHERE x.job_key = j.job_key)
    """, (mark,))
    con.commit()
    return cur.rowcount


def report(con):
    from triage.apply_queue import counts as app_counts

    d = con.execute("SELECT verdict, COUNT(*) n FROM decisions "
                    "GROUP BY verdict").fetchall()
    tot = con.execute("SELECT COUNT(*) FROM jobs WHERE live=1").fetchone()[0]
    by_feed = con.execute("SELECT feed, COUNT(*) n FROM jobs WHERE live=1 "
                          "GROUP BY feed ORDER BY n DESC").fetchall()
    print(f"\n  {tot:,} live rows"
          + (("  (" + " · ".join(f"{r['n']:,} {r['feed']}" for r in by_feed)
              + ")") if len(by_feed) > 1 else ""))
    if d:
        print("  decisions: " + " · ".join(f"{r['n']:,} {r['verdict']}"
                                           for r in d))
    ac = app_counts(con)
    if ac["total"]:
        print("  apply queue: " + " · ".join(
            f"{ac[s]:,} {s.replace('_', ' ')}" for s in
            ("to_apply", "applying", "applied", "skipped", "dead") if ac[s]))
    # How much of the live corpus carries a date the employer actually set.
    # Printed every load because it is the number that decides whether a
    # date-sorted lane means anything, and it moves when a source is added.
    db_rows = con.execute(
        "SELECT date_basis, COUNT(*) n FROM jobs WHERE live=1 AND date_basis<>''"
        " GROUP BY date_basis ORDER BY n DESC").fetchall()
    if db_rows:
        n_all = sum(r["n"] for r in db_rows)
        emp = sum(r["n"] for r in db_rows if r["date_basis"] == "employer")
        print(f"  posted dates: {emp:,} of {n_all:,} ({100 * emp // max(n_all, 1)}%)"
              f" are the employer's — "
              + " · ".join(f"{r['n']:,} {r['date_basis']}" for r in db_rows))
    unknown = date_basis.audit(
        r[0] for r in con.execute("SELECT DISTINCT source FROM jobs"))
    if unknown:
        print(f"  ! {len(unknown)} unclassified source(s) defaulting to "
              f"'{date_basis.DEFAULT}': {', '.join(unknown[:8])}"
              f"{' ...' if len(unknown) > 8 else ''}  (fix in date_basis.py)")
    print("\n  LANES                          left")
    for L in counts(con):
        print(f"    {L['label']:<28} {L['remaining']:>7,}")


def main():
    args = sys.argv[1:]

    def val(flag, default=None):
        return args[args.index(flag) + 1] if flag in args else default

    db = val("--db", DB)
    con = init(db)
    if "--stats" in args:
        report(con)
        return

    feed = val("--feed", "main")
    path = val("--csv") or (newest_contract_csv() if feed == "contract"
                            else newest_csv())
    n_block, added, removed = sync_blocklist(con)
    if added or removed:
        print(f"blocklist: {added} added, {removed} removed "
              f"({n_block} companies blocked)")

    mark = stamp()
    n = load(con, path, mark, feed)
    gone = retire(con, mark, feed)
    carried = carry_forward(con)
    carried_app = carry_forward_applications(con)
    minted, repointed = mint_refs(con)
    blocked = apply_blocks(con, mark)
    write_blocklist(con)

    print(f"\n{n:,} rows loaded into feed '{feed}'"
          + (f" · {gone:,} retired (live=0)" if gone else "")
          + (f" · {carried:,} decisions carried forward by company+title"
             if carried else "")
          + (f" · {carried_app:,} application statuses carried forward"
             if carried_app else "")
          + (f" · {blocked:,} auto-denied by blocklist" if blocked else "")
          + (f" · {minted:,} new refs minted" if minted else "")
          + (f" · {repointed:,} refs repointed" if repointed else ""))
    report(con)
    # Without sqlite_stat1 the planner treats the partial indexes as unusable
    # and the lane menu goes back to a full scan per lane (measured 12.8s ->
    # 2.7s the first time this was run by hand).  The index set just changed
    # for date_weak, so the old stats describe indexes that no longer exist.
    # Here and not in POST_MIGRATE_DDL: that runs on every server start too.
    print("analyzing ...", flush=True)
    con.execute("ANALYZE")
    con.commit()
    print(f"\ntriage.db ready — python3 triage/server.py")
    con.close()


if __name__ == "__main__":
    main()
