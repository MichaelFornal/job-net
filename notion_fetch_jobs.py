#!/usr/bin/env python3
"""
notion_fetch_jobs.py — pull postings from the boards the probe kept.

TWO EXTRACTION TIERS, both structural.  Prose careers pages are deliberately
NOT parsed: on 100daysofcode a sub_header is the job title while its sibling
bulleted_list items are responsibilities, and on 6037 the bulleted_list items
ARE the titles.  No structural signal separates them, so parsing prose would
fabricate rows into the master corpus and the triage queue.

  Tier A (kind=db)     queryCollection rows.  Every field comes from a typed
                       Notion property, mapped by SCHEMA NAME — the keys are
                       per-tenant gibberish ("MIn~", "stOP", "\\TU?") and
                       sometimes UUIDs; only "title" is stable.
  Tier B (kind=pages)  child sub-pages of a careers page, each one a job
                       (the a11team shape).  Title + URL + date only; there
                       is no schema to mine and inventing fields would be
                       fabrication.

POSTED DATES ARE REAL HERE, which is rare in this repo: every row block
carries created_time in epoch ms.  They are still FROZEN in
notion_posted_ledger.json on first sight and reused verbatim, because
all_jobs_build.py keeps blank-posted rows forever and a date that moves
between runs marches rows backwards past POSTED_SINCE while they are still
live.  run_all.py must never rotate the ledger.

Rate limits: measured 4 workers at 2.0 rps -> 6x HTTP 429 in 518s, so the
defaults sit below that and 429 takes a permanent interval penalty instead of
failing a board.  NOTION_BUDGET_MIN caps wall clock and exits 0 so the build
tail still runs.
"""
import csv
import json
import os
import re
import shutil
import sys
import threading
import time
import concurrent.futures as cf

import contract_common as cc
import notion_common as nc

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""
TODAY = time.strftime("%Y-%m-%d")

BOARDS_CSV = os.path.join(BASE, PFX + "notion_job_boards.csv")
OUT_JSONL = os.path.join(BASE, PFX + "notion_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, PFX + "notion_fetch_state.json")
LEDGER_JSON = os.path.join(BASE, PFX + "notion_posted_ledger.json")

WORKERS = 4
RPS = 1.5
PAGE_LIMIT = 300           # rows per queryCollection call
MAX_ROWS = 3000            # per board; beyond this it is not a careers page
MIN_FREE_GB = 5
BUDGET_MIN = float(os.environ.get("NOTION_BUDGET_MIN") or 0)

_lock = threading.Lock()
_ledger_lock = threading.Lock()
_counts = {"boards": 0, "jobs": 0, "dead": 0, "error": 0}
_t0 = time.time()

URLISH = re.compile(r"^(https?://|www\.)\S+$", re.I)


def over_budget():
    return BUDGET_MIN and (time.time() - _t0) / 60.0 > BUDGET_MIN


def page_url(host, block_id):
    return "https://%s.notion.site/%s" % (host, nc.undash(block_id))


def ledger_posted(ledger, jid, created_ms):
    """Freeze on first sight.  A recomputed date would move between runs."""
    with _ledger_lock:
        e = ledger.get(jid)
        if e:
            return e.get("posted", ""), e.get("precision", "")
        d = cc.ymd(ts=created_ms) if created_ms else ""
        posted = d or TODAY
        prec = "created" if d else "first_seen"
        ledger[jid] = {"posted": posted, "precision": prec,
                       "first_seen": TODAY}
        return posted, prec


def query_rows(s, cid, vid, sid):
    """All rows of a collection.  The reducer takes a flat limit, so grow it
    until hasMore clears rather than threading a cursor."""
    limit = PAGE_LIMIT
    while True:
        q = nc.api(s, "queryCollection?src=initial_load", {
            "source": {"type": "collection", "id": nc.dash(cid),
                       "spaceId": sid},
            "collectionView": {"id": nc.dash(vid), "spaceId": sid},
            "loader": {"reducers": {"collection_group_results":
                                    {"type": "results", "limit": limit}},
                       "sort": [], "searchQuery": "",
                       "userTimeZone": "America/New_York", "type": "reducer"}})
        res = ((q.get("result") or {}).get("reducerResults") or {}) \
            .get("collection_group_results") or {}
        ids = res.get("blockIds") or []
        if not res.get("hasMore") or limit >= MAX_ROWS or len(ids) < limit:
            return ids, (q.get("recordMap") or {})
        limit = min(limit * 3, MAX_ROWS)


def extract_db(s, b, ledger):
    cid, vid, sid = b["collection_id"], b["view_id"], b["space_id"]
    ids, rm = query_rows(s, cid, vid, sid)
    _, cname, schema = nc.collection_schema(rm)
    if not schema:
        coll = nc.get_record_values(s, "collection", cid)
        schema = coll.get("schema") or {}
    fmap = nc.map_schema(schema)
    comp_key = b.get("company_prop") or ""
    host = b["host"]
    out = []
    for i in ids:
        v = nc.unwrap((rm.get("block") or {}).get(i, {}))
        if v.get("alive") is False:
            continue
        props = v.get("properties") or {}
        title = nc.rich_text(props.get("title"))
        if not title or nc.is_junk_title(title):
            continue

        def prop(field):
            k = fmap.get(field)
            return nc.rich_text(props.get(k)) if k else ""

        company = nc.rich_text(props.get(comp_key)) if comp_key else ""
        company = company or b.get("company") or ""
        if not company:
            continue          # never emit an unattributed row

        url = prop("apply_url")
        if not URLISH.match(url or ""):
            # altcarbon puts free text in its url column
            # ("Write to us at hi@altcarbon.com") — that is not an apply link.
            url = ""
        jid = nc.undash(i)
        posted, prec = ledger_posted(ledger, jid, v.get("created_time"))
        loc = prop("location")
        out.append({
            "id": jid, "title": title, "company": company,
            "location": loc, "department": prop("department"),
            "employment_type": prop("employment_type"),
            "seniority": prop("seniority"), "salary": prop("salary"),
            "remote_txt": prop("remote"), "posted": posted,
            "posted_precision": prec,
            "apply_url": url or page_url(host, i),
            "job_url": page_url(host, i), "tier": "db",
        })
    return out, cname


def extract_pages(s, b, ledger):
    """Tier B: each child sub-page is one job."""
    host, pid = b["host"], b["host_page_id"]
    d = nc.load_page_chunk(s, pid, limit=200)
    rm = d.get("recordMap") or {}
    kids = nc.child_pages(rm, pid)
    company = b.get("company") or ""
    if not company:
        return [], ""
    out = []
    for bid, title in kids:
        if not nc.looks_like_role(title):
            continue
        v = nc.unwrap((rm.get("block") or {}).get(nc.dash(bid), {}))
        jid = nc.undash(bid)
        posted, prec = ledger_posted(ledger, jid, v.get("created_time"))
        out.append({
            "id": jid, "title": title, "company": company,
            "location": "", "department": "", "employment_type": "",
            "seniority": "", "salary": "", "remote_txt": "",
            "posted": posted, "posted_precision": prec,
            "apply_url": page_url(host, bid),
            "job_url": page_url(host, bid), "tier": "pages",
        })
    return out, b.get("board_name") or ""


def do_board(b, ledger):
    s = nc.session()
    try:
        if b.get("kind") == "pages":
            return extract_pages(s, b, ledger)
        return extract_db(s, b, ledger)
    finally:
        s.close()


def main():
    if shutil.disk_usage(BASE).free < MIN_FREE_GB * (1 << 30):
        raise SystemExit("less than %d GB free" % MIN_FREE_GB)
    if not os.path.exists(BOARDS_CSV):
        raise SystemExit("missing %s — run notion_probe_boards.py"
                         % BOARDS_CSV)
    nc.set_rps(RPS)

    with open(BOARDS_CSV, newline="", encoding="utf-8") as f:
        boards = list(csv.DictReader(f))
    state = cc.load_state(STATE_JSON)
    ledger = cc.load_state(LEDGER_JSON)

    def key(b):
        return b["collection_id"] or "p:" + b["host_page_id"]

    todo = [b for b in boards if state.get(key(b), {}).get("status") != "done"]
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d boards, artifacts prefixed '%s'"
              % (len(todo), len(boards), PFX), flush=True)
    print("%d boards, %d already done, %d to fetch"
          % (len(boards), len(boards) - len(todo), len(todo)), flush=True)

    w = cc.Writer(OUT_JSONL)
    ndone = 0
    # NOT a `with` block: on a budget stop, __exit__ would call
    # shutdown(wait=True) and sit through every already-queued board, which
    # is exactly the wall clock the budget exists to bound.
    ex = cf.ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futs = {ex.submit(do_board, b, ledger): b for b in todo}
        try:
            for fut in cf.as_completed(futs):
                b = futs[fut]
                try:
                    jobs, cname = fut.result()
                    st = "done"
                except nc.Gone as e:
                    jobs, cname, st = [], "", "dead"
                    with _lock:
                        _counts["dead"] += 1
                except Exception as e:                  # noqa: BLE001
                    jobs, cname, st = [], "", "error"
                    with _lock:
                        _counts["error"] += 1
                        if _counts["error"] <= 5:
                            print("  ! %s: %s: %s" % (b["host"],
                                                      type(e).__name__, e),
                                  flush=True)
                with _lock:
                    # Every row is written BEFORE the board is marked done, so
                    # a retry cannot append duplicates.
                    for j in jobs:
                        w.write({"host": b["host"], "company": j["company"],
                                 "board": cname or b.get("board_name") or "",
                                 "board_url": b.get("board_url") or "",
                                 "kind": b.get("kind") or "db", "job": j})
                    _counts["jobs"] += len(jobs)
                    _counts["boards"] += 1 if st == "done" else 0
                    state[key(b)] = {"status": st, "n": len(jobs),
                                     "at": TODAY}
                    ndone += 1
                    if ndone % 50 == 0:
                        cc.save_state(STATE_JSON, state)
                        cc.save_state(LEDGER_JSON, ledger)
                        print("  %d/%d boards, %d jobs (dead %d err %d)"
                              % (ndone, len(todo), _counts["jobs"],
                                 _counts["dead"], _counts["error"]),
                              flush=True)
                if over_budget():
                    print("  budget %g min reached — stopping cleanly "
                          "(%d boards unstarted, resume on the next run)"
                          % (BUDGET_MIN, len(todo) - ndone), flush=True)
                    break
        finally:
            # Drop everything still queued instead of draining it.
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except TypeError:                    # py<3.9
                ex.shutdown(wait=False)
    finally:
        w.close()
        cc.save_state(STATE_JSON, state)
        cc.save_state(LEDGER_JSON, ledger)

    real = sum(1 for e in ledger.values() if e.get("precision") == "created")
    print("\n%d boards -> %d jobs this run -> %s"
          % (_counts["boards"], _counts["jobs"], OUT_JSONL), flush=True)
    print("ledger: %d jobs total, %d (%.0f%%) with a real created date"
          % (len(ledger), real, 100.0 * real / max(1, len(ledger))),
          flush=True)
    print("dead %d, error %d" % (_counts["dead"], _counts["error"]),
          flush=True)
    # A budget stop is not a failure — the supervisor must proceed to build.
    raise SystemExit(0 if (over_budget() or not _counts["error"]) else 1)


if __name__ == "__main__":
    sys.exit(main())
