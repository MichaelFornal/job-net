#!/usr/bin/env python3
"""
notion_probe_boards.py — resolve each candidate page to a job board, or reject it.

The probe earns a stage of its own (unlike ashby/yc, where fetch doubles as
probe) because it does three things the fetcher cannot:

 1. RESOLVES THE BARE-SLUG GRAMMAR.  /sdr carries no page id; it is recovered
    from the HTML.  A ~19KB body with no "pageId" means no published page.

 2. WALKS A ROW UP TO ITS BOARD.  Almost every corpus candidate is an
    individual JOB page, not a board — Getro's apply URLs point at the
    posting.  A row page has parent_table == "collection", so:
        row -> parent_id (collection) -> getRecordValues -> collection's
        parent_id (the block hosting the view) -> loadPageChunk -> view_ids
    Without this walk the pipe would fetch one job per candidate instead of a
    whole board.  (loadPageChunk on a collection id returns HTTP 400; the hop
    through getRecordValues is required, not stylistic.)

 3. OWNS THE JOB-BOARD GATE.  reachcapital.notion.site — 340 rows in
    all_jobs_master.csv, the largest notion host in the corpus — is a VC
    READING LIST ("Our Reading Syllabus", title property "Publication", rows
    "Hechinger Report" / "Stratechery").  Getro labelled it jobs.  Any pipe
    that trusts the host imports thousands more like it.

Statuses: live · empty · not_a_board · aggregator_drop · dead · retired ·
error.  Everything but `error` is settled and carried forward; errors are
re-probed.  RETIRE_AFTER=4 consecutive zero-row probes -> retired, so a third
of every run is not spent re-probing archived boards.
"""
import csv
import os
import sys
import threading
import time
import concurrent.futures as cf

import notion_common as nc

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

CAND_CSV = os.path.join(BASE, PFX + "notion_candidates.csv")
RESULTS_CSV = os.path.join(BASE, PFX + "notion_probe_results.csv")
BOARDS_CSV = os.path.join(BASE, PFX + "notion_job_boards.csv")

# MEASURED, do not raise on a hunch.  4 workers at 1.5 rps sustained 39
# candidates/min over 750 probes with 4 errors.  Trying 6 workers at 3.0 rps
# to go faster HALVED it to 21/min: the extra 429s ratcheted the interval to
# its 1.0s floor and it stayed there.  notion_common now decays the penalty,
# but the right starting point is still the rate the host tolerates cleanly.
WORKERS = 4
RPS = 1.5
RETIRE_AFTER = 4
SAMPLE = 40          # rows pulled to judge a board; not the fetch limit

RES_COLS = ["host", "path", "page_id", "host_page_id", "kind", "collection_id",
            "view_id", "space_id", "status", "company", "company_src",
            "board_name", "multi_employer", "company_prop", "n_rows",
            "zero_streak", "sources", "detail", "probed_at"]

TODAY = time.strftime("%Y-%m-%d")
# Statuses that are settled forever and never re-probed.
SETTLED = ("dead", "retired", "not_a_board", "aggregator_drop")

_lock = threading.Lock()
_counts = {k: 0 for k in ("live", "empty", "not_a_board", "aggregator_drop",
                          "dead", "retired", "error")}
_space_cache = {}
_space_lock = threading.Lock()


def space_name(s, host):
    """-> (company, source).  source is 'space' when Notion told us the name
    and 'host' when it was derived from the subdomain; the builder reports the
    split so attribution precision stays visible."""
    with _space_lock:
        if host in _space_cache:
            return _space_cache[host]
    d = nc.get_public_page_data(s, host)
    space = "" if d.get("isDeleted") else (d.get("spaceName") or "")
    name, src = nc.pick_company(space, host)
    out = (name, src if name else "")
    with _space_lock:
        _space_cache[host] = out
    return out


def find_board(s, page_id):
    """-> (collection_id, view_id, space_id, coll_name, schema, page_title).

    Handles both shapes: the candidate IS a board page, or the candidate is a
    row inside one and we walk up."""
    d = nc.load_page_chunk(s, page_id)
    rm = d.get("recordMap") or {}
    if not (rm.get("block") or {}):
        raise nc.Gone("empty recordMap")
    self_v = nc.unwrap((rm.get("block") or {}).get(nc.dash(page_id), {}))
    ptitle = nc.rich_text((self_v.get("properties") or {}).get("title"))

    views = nc.collection_views(rm)
    if views:
        cid, vid, sid = views[0]
        sid = sid or nc.space_id_of(rm)
        _, cname, schema = nc.collection_schema(rm)
        if not schema:
            c = nc.get_record_values(s, "collection", cid)
            cname, schema = nc.rich_text(c.get("name")), (c.get("schema") or {})
        return dict(kind="db", collection_id=cid, view_id=vid, space_id=sid,
                    board_name=cname, schema=schema, page_title=ptitle,
                    host_page_id=nc.dash(page_id))

    # Not a board page. Is it a row inside one?
    if self_v.get("parent_table") != "collection":
        # TIER B: no database, but child sub-pages may each be a job — the
        # a11team shape ("Open positions (2)" with two child pages).  This is
        # the only reason 46% of live boards are not simply dead.
        kids = nc.child_pages(rm, page_id)
        if kids:
            return dict(kind="pages", collection_id="", view_id="",
                        space_id=nc.space_id_of(rm), board_name=ptitle,
                        schema={}, page_title=ptitle,
                        host_page_id=nc.dash(page_id),
                        child_titles=[t for _, t in kids])
        # Still nothing — but the candidate may BE one Tier B job rather than
        # the board holding it (Getro links the posting, not the careers
        # page).  Walk up once, symmetrically with the row->collection walk
        # above.  The gate still runs on the siblings, so landing on a wiki
        # root is rejected rather than imported.
        parent = self_v.get("parent_id") or ""
        if self_v.get("parent_table") == "block" and parent:
            d3 = nc.load_page_chunk(s, parent, limit=200)
            rm3 = d3.get("recordMap") or {}
            sibs = nc.child_pages(rm3, parent)
            if len(sibs) > 1:
                pv = nc.unwrap((rm3.get("block") or {}).get(nc.dash(parent),
                                                            {}))
                pt = nc.rich_text((pv.get("properties") or {}).get("title"))
                return dict(kind="pages", collection_id="", view_id="",
                            space_id=nc.space_id_of(rm3), board_name=pt,
                            schema={}, page_title=pt,
                            host_page_id=nc.dash(parent),
                            child_titles=[t for _, t in sibs])
        raise nc.Gone("page has no database")
    cid = self_v.get("parent_id") or ""
    if not cid:
        raise nc.Gone("row without parent collection")

    coll = nc.get_record_values(s, "collection", cid)
    cname = nc.rich_text(coll.get("name"))
    schema = coll.get("schema") or {}
    host_block = coll.get("parent_id") or ""
    sid = coll.get("space_id") or ""
    if not host_block:
        raise nc.Gone("collection without host block")

    d2 = nc.load_page_chunk(s, host_block)
    rm2 = d2.get("recordMap") or {}
    host_v = nc.unwrap((rm2.get("block") or {}).get(nc.dash(host_block), {}))
    ptitle = nc.rich_text((host_v.get("properties") or {}).get("title")) \
        or ptitle
    for c2, v2, s2 in nc.collection_views(rm2):
        if nc.undash(c2) == nc.undash(cid):
            return dict(kind="db", collection_id=cid, view_id=v2,
                        space_id=(s2 or sid or nc.space_id_of(rm2)),
                        board_name=cname, schema=schema, page_title=ptitle,
                        host_page_id=nc.dash(host_block))
    raise nc.Gone("view for collection not found")


def sample_rows(s, cid, vid, sid, limit=SAMPLE):
    if not vid:
        return [], {}
    q = nc.api(s, "queryCollection?src=initial_load", {
        "source": {"type": "collection", "id": nc.dash(cid), "spaceId": sid},
        "collectionView": {"id": nc.dash(vid), "spaceId": sid},
        "loader": {"reducers": {"collection_group_results":
                                {"type": "results", "limit": limit}},
                   "sort": [], "searchQuery": "",
                   "userTimeZone": "America/New_York", "type": "reducer"}})
    qrm = q.get("recordMap") or {}
    res = ((q.get("result") or {}).get("reducerResults") or {}) \
        .get("collection_group_results") or {}
    ids = res.get("blockIds") or []
    titles = []
    for i in ids:
        v = nc.unwrap((qrm.get("block") or {}).get(i, {}))
        titles.append(nc.rich_text((v.get("properties") or {}).get("title")))
    _, _, schema = nc.collection_schema(qrm)
    return titles, schema


def probe(cand, prev_streak):
    host, path = cand["host"], cand["path"]
    s = nc.session()
    try:
        page_id = cand.get("page_id") or ""
        if not page_id:
            page_id = nc.resolve_slug(s, host, path)
            if not page_id:
                return dict(status="dead", detail="no published page")
        b = find_board(s, page_id)
        cid, vid, sid = b["collection_id"], b["view_id"], b["space_id"]
        cname, schema, ptitle = b["board_name"], b["schema"], b["page_title"]
        ident = dict(collection_id=cid, view_id=vid, space_id=sid,
                     board_name=cname, page_id=page_id, kind=b["kind"],
                     host_page_id=b["host_page_id"])

        if b["kind"] == "pages":
            titles = b["child_titles"]
            # Tier B has no schema to lean on, so the titles must carry the
            # evidence themselves.  A wiki of child pages is not a job board.
            roles = [t for t in titles if nc.looks_like_role(t)]
            if len(roles) < max(1, nc.MIN_ROLE_RATE * len(titles)):
                return dict(ident, status="not_a_board",
                            detail="%d/%d child pages read as roles"
                                   % (len(roles), len(titles)))
        else:
            titles, schema2 = sample_rows(s, cid, vid, sid)
            schema = schema or schema2
            ok, why = nc.board_gate(cname, schema, titles)
            if not ok:
                return dict(ident, status="not_a_board", detail=why)

        multi, name_key = nc.employer_props(schema)
        company, company_src = "", ""
        if name_key == "title":
            # The title column IS the company, so each row is a COMPANY, not
            # a job — elevenvc's portfolio list.  There is no job title to
            # extract and inventing one from the company name is exactly the
            # reachcapital failure in a different costume.
            return dict(ident, status="not_a_board",
                        detail="title column is the company, rows are firms")
        # A board whose NAME advertises many employers is multi-employer even
        # when its schema has no company-ish column — that is precisely how
        # "100+ AI Startup Roles" labelled 101 other firms' jobs with a
        # recruiter's own name.
        if nc.AGGREGATOR_BOARD_RX.search(cname or "") or \
                nc.AGGREGATOR_BOARD_RX.search(ptitle or ""):
            multi = True
        if multi and not name_key:
            return dict(ident, status="aggregator_drop", multi_employer=1,
                        detail="multi-employer board, no employer column")
        if not multi:
            company, company_src = space_name(s, host)
            if not company:
                company = nc.company_from_title(ptitle) \
                    or nc.company_from_title(cname)
                company_src = "title" if company else ""

        n = len([t for t in titles if t and not nc.is_junk_title(t)])
        base = dict(ident, company=company, company_src=company_src,
                    multi_employer=1 if multi else 0, company_prop=name_key,
                    n_rows=n)
        if n:
            return dict(base, status="live", zero_streak=0)
        streak = prev_streak + 1
        return dict(base, status="retired" if streak >= RETIRE_AFTER
                    else "empty", zero_streak=streak)
    finally:
        s.close()


def main():
    if not os.path.exists(CAND_CSV):
        raise SystemExit("missing %s — run notion_discover_boards.py"
                         % CAND_CSV)
    nc.set_rps(RPS)
    with open(CAND_CSV, newline="", encoding="utf-8") as f:
        cands = list(csv.DictReader(f))

    prev, keep = {}, []
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                prev[(row["host"], row["path"])] = row
                st = row.get("status") or ""
                # Settled statuses carry forward forever.  live/empty carry
                # forward only for TODAY, which is what makes a killed run
                # resumable: this probe takes hours over 6k candidates, and
                # without it a kill at 90% would re-probe every live board.
                # A fresh run tomorrow still re-checks them, because a live
                # board can go empty or disappear.
                if st in SETTLED or (st in ("live", "empty")
                                     and row.get("probed_at") == TODAY):
                    keep.append([row.get(c, "") for c in RES_COLS])
                    _counts[st] += 1
    settled = {(r[0], r[1]) for r in keep}
    todo = [c for c in cands if (c["host"], c["path"]) not in settled]
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d candidates, artifacts prefixed '%s'"
              % (len(todo), len(cands), PFX), flush=True)
    print("%d candidates, %d settled, %d to probe"
          % (len(cands), len(settled), len(todo)), flush=True)

    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(RES_COLS)
        for row in keep:
            w.writerow(row)
        f.flush()
        ndone = 0
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {}
            for c in todo:
                st = int((prev.get((c["host"], c["path"])) or {})
                         .get("zero_streak") or 0)
                futs[ex.submit(probe, c, st)] = c
            for fut in cf.as_completed(futs):
                c = futs[fut]
                try:
                    r = fut.result()
                except nc.Gone as e:
                    r = dict(status="dead", detail=str(e)[:60])
                except Exception as e:               # noqa: BLE001
                    r = dict(status="error",
                             detail="%s: %s" % (type(e).__name__, e)[:60])
                with _lock:
                    _counts[r["status"]] += 1
                    w.writerow([c["host"], c["path"],
                                r.get("page_id", c.get("page_id", "")),
                                r.get("host_page_id", ""), r.get("kind", ""),
                                r.get("collection_id", ""),
                                r.get("view_id", ""), r.get("space_id", ""),
                                r["status"], r.get("company", ""),
                                r.get("company_src", ""),
                                r.get("board_name", ""),
                                r.get("multi_employer", ""),
                                r.get("company_prop", ""),
                                r.get("n_rows", 0), r.get("zero_streak", 0),
                                c.get("sources", ""), r.get("detail", ""),
                                TODAY])
                    f.flush()
                    ndone += 1
                    if ndone % 50 == 0:
                        print("  %d/%d  " % (ndone, len(todo)) +
                              " ".join("%s %d" % (k, v) for k, v
                                       in _counts.items() if v), flush=True)

    # One row per BOARD, not per candidate: many candidates are different
    # jobs on the same board and all resolve to one collection.
    boards = {}
    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] not in ("live", "empty"):
                continue
            # Tier B boards have no collection, so they key on the host page.
            k = nc.undash(row["collection_id"]) or \
                "p:" + nc.undash(row["host_page_id"])
            if k.strip("p:") and k not in boards:
                boards[k] = row
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["host", "page_id", "host_page_id", "kind",
                    "collection_id", "view_id", "space_id",
                    "company", "company_src", "board_name", "multi_employer",
                    "company_prop", "n_rows", "board_url", "sources"])
        for r in sorted(boards.values(), key=lambda r: r["host"]):
            w.writerow([r["host"], r["page_id"], r["host_page_id"],
                        r["kind"], r["collection_id"],
                        r["view_id"], r["space_id"], r["company"],
                        r["company_src"], r["board_name"],
                        r["multi_employer"],
                        r["company_prop"], r["n_rows"],
                        "https://%s.notion.site/%s" % (r["host"], r["path"]),
                        r["sources"]])
    print("\n" + " ".join("%s %d" % (k, v) for k, v in _counts.items()))
    print("%d distinct boards -> %s (%d errors — re-run to retry)"
          % (len(boards), BOARDS_CSV, _counts["error"]), flush=True)
    raise SystemExit(1 if _counts["error"] else 0)


if __name__ == "__main__":
    sys.exit(main())
