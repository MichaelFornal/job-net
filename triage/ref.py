#!/usr/bin/env python3
"""ref.py — turn `J4821` back into a job.

WHY THIS EXISTS.  `job_key` is the only other identifier a row has and it
averages 95 characters, so quoting a job in conversation has always meant
pasting a URL -- which means leaving the keyboard, which is the one thing the
triage page is built to avoid.  Every card now wears a short number
(triage_load.mint_refs) and this reads it back.

    python3 triage/ref.py J4821                 one ref, in full
    python3 triage/ref.py J4821 J5013 J900      several, as a table
    python3 triage/ref.py --find anthropic      text -> refs, the other way
    python3 triage/ref.py --json J4821          for a program
    python3 triage/ref.py --mint                fill in refs without a full load

A REF NAMES AN IDENTITY, NOT A ROW, and the output says so rather than hiding
it.  `refs.ref_key` is the same ct_key (company|title) that decisions carry
forward on, which is exactly why a ref survives a rebuild -- but it means one
ref legitimately covers every live copy of a posting: 1.6k live ct_keys hold
more than one row in `jobs`, some spanning both feeds, and corpus.db keeps
every duplicate board copy.  So this prints ALL of them and counts them,
instead of picking one and looking authoritative.

Live rows lead.  Nothing ever deletes from `jobs` -- retire() only sets
live=0 -- so a ref that has been around a while resolves to a live row plus
its dead predecessors, and printing those in any other order would answer a
question about a current job with a row that is months stale.

Read-only.  Safe to run against a database the server has open (WAL), which is
the normal case.
"""
import argparse
import json
import os
import re
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import lanes as L                     # noqa: E402
import triage_load as TL              # noqa: E402

_REF = re.compile(r"^[#Jj]?(\d+)$")


def parse_ref(s):
    """'J4821' / 'j4821' / '#4821' / '4821' -> 4821.  None if it isn't one."""
    m = _REF.match((s or "").strip())
    return int(m.group(1)) if m else None


def fmt(n):
    return "J%d" % n


# ------------------------------------------------------------------ lookup

def ledger(con, ref):
    return con.execute(
        "SELECT ref, ref_key, company, title, minted_at FROM refs WHERE ref = ?",
        (ref,)).fetchone()


def job_rows(con, ref):
    """Every `jobs` row wearing this ref, live first, newest-seen first.

    `AND ref > 0` is not a filter, it is the INDEX HINT, and without it this SCANS all
    293k rows.  `idx_jobs_ref` is deliberately partial (`WHERE ref > 0`, since ref 0 means
    "not minted yet" and nobody looks one up), and SQLite will only use a partial index
    when the query's own WHERE terms imply its predicate -- `ref = ?` is a bound
    parameter, so at prepare time nothing proves it is positive, and the index sat unused
    from the day it was written.  Measured 2026-08-26: 2.128s -> 0.001s.  It is also
    correct on its own terms: J0 is not a reference number.
    """
    return con.execute(
        "SELECT * FROM jobs WHERE ref = ? AND ref > 0 "
        "ORDER BY live DESC, last_seen DESC", (ref,)).fetchall()


def corpus_rows(con, ref_key, corpus=None):
    """Every corpus.db row whose computed ref_key matches.

    Separate from job_rows because ~57% of refs exist ONLY here: corpus.db
    carries 569k distinct keys against jobs' 322k, and the scout lane serves
    straight off it.  A ref that resolves to nothing in `jobs` is normal, not
    a bug, and saying which database answered is the difference between the two.
    """
    corpus = TL.CORPUS_DB if corpus is None else corpus
    if not corpus or not os.path.exists(corpus):
        return []
    c = sqlite3.connect("file:%s?mode=ro" % corpus, uri=True)
    c.row_factory = sqlite3.Row
    c.create_function("tl_ct_key", 2, TL.ct_key, deterministic=True)
    try:
        return c.execute(
            "SELECT * FROM corpus WHERE %s = ? ORDER BY dup_rank"
            % TL._ref_key_sql(), (ref_key,)).fetchall()
    finally:
        c.close()


def lanes_holding(con, job_key):
    """Which lanes currently contain this row.

    Tests each lane's raw predicate, NOT queue_sql() -- that folds UNDECIDED in,
    so a decided row would report as being in no lane at all when what you want
    to know is where it sits and, separately, that it has been judged.
    """
    out = []
    for key, (label, pred, params) in L.lane_map().items():
        try:
            hit = con.execute(
                "SELECT 1 FROM jobs WHERE job_key = ? AND (%s) LIMIT 1" % pred,
                [job_key] + list(params)).fetchone()
        except sqlite3.Error:
            continue
        if hit:
            out.append(key)
    return out


def verdict_of(con, job_key):
    return con.execute(
        "SELECT verdict, reason, lane, decided_at FROM decisions WHERE job_key = ?",
        (job_key,)).fetchone()


def application_of(con, job_key):
    return con.execute(
        "SELECT status, applied_at, outcome, outcome_at, note "
        "FROM applications WHERE job_key = ?", (job_key,)).fetchone()


def resolve(con, ref, corpus=None):
    """Everything known about one ref."""
    led = ledger(con, ref)
    if led is None:
        return {"ref": ref, "known": False}
    jrows = job_rows(con, ref)
    crows = corpus_rows(con, led["ref_key"], corpus)
    out = {"ref": ref, "known": True, "ref_key": led["ref_key"],
           "minted_at": led["minted_at"],
           "minted_company": led["company"], "minted_title": led["title"],
           "jobs": [], "corpus": []}
    for r in jrows:
        d = dict(r)
        d["lanes"] = lanes_holding(con, r["job_key"]) if r["live"] else []
        v = verdict_of(con, r["job_key"])
        a = application_of(con, r["job_key"])
        d["decision"] = dict(v) if v else None
        d["application"] = dict(a) if a else None
        out["jobs"].append(d)
    for r in crows:
        out["corpus"].append(dict(r))
    return out


# ------------------------------------------------------------------ output

def money(r):
    lo, hi = (r.get("salary_min") or "").strip(), (r.get("salary_max") or "").strip()
    if not lo and not hi:
        return ""
    cur = (r.get("salary_currency") or "").strip() or "USD"
    per = (r.get("salary_period") or "").strip()
    return " ".join(x for x in ["-".join(x for x in (lo, hi) if x), cur, per] if x)


def show(res, verbose=True):
    ref = fmt(res["ref"])
    if not res["known"]:
        print("%s  -- no such ref.  Refs are minted by triage_load.py; if this "
              "one is newer than\n%s  your last load, run: python3 triage/ref.py --mint"
              % (ref, " " * len(ref)))
        return
    jobs, corp = res["jobs"], res["corpus"]
    if not jobs and not corp:
        print("%s  %s -- %s" % (ref, res["minted_company"] or "(no company)",
                                res["minted_title"] or "(no title)"))
        print("     known but no longer present in either database "
              "(minted %s).  Company and title above are frozen from that day."
              % res["minted_at"])
        return

    live = [j for j in jobs if j["live"]]
    dead = [j for j in jobs if not j["live"]]
    head = live[0] if live else (jobs[0] if jobs else corp[0])
    print("%s  %s -- %s" % (ref, head.get("company") or "(no company)",
                            head.get("title") or "(no title)"))

    def field(k, v):
        if v:
            print("      %-11s %s" % (k, v))

    for j in live:
        print("    triage.db  live  feed=%s" % (j.get("feed") or ""))
        field("location", j.get("location"))
        field("mode", " · ".join(x for x in (j.get("work_mode"),
                                             j.get("employment_type"),
                                             j.get("seniority")) if x))
        field("salary", money(j))
        posted = j.get("posted") or "(undated)"
        if j.get("date_weak"):
            posted += "   ~ %s, not the employer's date" % (j.get("date_basis") or "inferred")
        field("posted", posted)
        field("source", (j.get("source") or "")
              + ("  (aggregator republish)" if j.get("is_agg") else ""))
        field("lanes", ", ".join(j["lanes"]) or "(in no lane)")
        d = j.get("decision")
        field("decision", "%s  (%s, lane %s, %s)" % (d["verdict"], d["reason"],
                                                     d["lane"] or "-", d["decided_at"])
              if d else "undecided")
        a = j.get("application")
        if a:
            field("applied", "%s  %s%s" % (a["status"], a["applied_at"] or "",
                                           ("  outcome: " + a["outcome"]) if a["outcome"] else ""))
        field("apply", j.get("apply_url") or j.get("job_url"))
        if verbose:
            field("job_key", j.get("job_key"))
    if dead:
        print("    triage.db  %d retired row(s) (live=0) also carry this ref"
              % len(dead))
    if corp:
        c0 = corp[0]
        print("    corpus.db  %d row(s)%s" % (
            len(corp), "" if live else "  -- scout only, no live triage row"))
        if not live:
            field("location", c0.get("location"))
            field("posted", c0.get("posted"))
            field("source", c0.get("source"))
            field("apply", c0.get("apply_url") or c0.get("job_url"))
    if len(live) > 1:
        print("    NOTE  %d live rows share this ref -- same company+title, "
              "different source copies." % len(live))


def show_table(results):
    print("%-9s %-26s %-40s %-8s %s" % ("ref", "company", "title", "where", "state"))
    for res in results:
        if not res["known"]:
            print("%-9s %s" % (fmt(res["ref"]), "-- no such ref"))
            continue
        live = [j for j in res["jobs"] if j["live"]]
        head = live[0] if live else (res["jobs"][0] if res["jobs"] else
                                     (res["corpus"][0] if res["corpus"] else {}))
        where = ("triage" if live else "retired" if res["jobs"] else
                 "scout" if res["corpus"] else "gone")
        d = live[0].get("decision") if live else None
        a = live[0].get("application") if live else None
        state = (a["status"] if a else d["verdict"] if d else "undecided")
        print("%-9s %-26.26s %-40.40s %-8s %s" % (
            fmt(res["ref"]), head.get("company") or "-",
            head.get("title") or "-", where, state))


def find(con, text, limit=25):
    """Text -> refs.  The reverse direction, for when you have the job not the number."""
    like = "%%%s%%" % text
    rows = con.execute(
        "SELECT ref, company, title, location, live, feed FROM jobs "
        "WHERE (company LIKE ? OR title LIKE ?) AND ref > 0 "
        "ORDER BY live DESC, last_seen DESC LIMIT ?",
        (like, like, limit)).fetchall()
    if not rows:
        print("nothing in jobs matches %r" % text)
        return
    seen = set()
    for r in rows:
        if r["ref"] in seen:
            continue
        seen.add(r["ref"])
        print("%-9s %-26.26s %-40.40s %-18.18s %s" % (
            fmt(r["ref"]), r["company"] or "-", r["title"] or "-",
            r["location"] or "-", "" if r["live"] else "(retired)"))


def main():
    ap = argparse.ArgumentParser(description="Resolve a triage reference number.")
    ap.add_argument("refs", nargs="*", help="J4821, 4821, #4821 ...")
    ap.add_argument("--find", metavar="TEXT", help="search company/title for refs")
    ap.add_argument("--json", action="store_true", help="machine-readable")
    ap.add_argument("--mint", action="store_true",
                    help="mint refs now instead of waiting for the next load")
    ap.add_argument("--limit", type=int, default=25, help="--find result cap")
    args = ap.parse_args()

    if args.mint:
        # triage_load.py --stats returns before the loader ever reaches
        # mint_refs, and a full load means the whole triage_chain.  This is the
        # repair path: same function, nothing else touched.
        con = TL.init()
        minted, repointed = TL.mint_refs(con)
        con.commit()
        con.close()
        print("minted {:,} new refs, repointed {:,}".format(minted, repointed))
        return 0

    con = TL.connect()
    con.row_factory = sqlite3.Row
    try:
        if args.find:
            find(con, args.find, args.limit)
            return 0
        if not args.refs:
            ap.error("give a ref (J4821) or --find TEXT or --mint")
        bad = [r for r in args.refs if parse_ref(r) is None]
        if bad:
            ap.error("not a reference number: %s" % ", ".join(bad))
        results = [resolve(con, parse_ref(r)) for r in args.refs]
        if args.json:
            print(json.dumps(results, indent=1, default=str))
        elif len(results) == 1:
            show(results[0])
        else:
            show_table(results)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
