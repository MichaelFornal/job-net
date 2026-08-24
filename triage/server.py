#!/usr/bin/env python3
"""The triage control surface — stdlib HTTP server + a JSON API over triage.db.

Same shape as apply/web/server.py: no framework (only `requests` and `openpyxl`
are installed and this adds nothing), an `API` class kept free of HTTP so it
stays testable, and a bind to 127.0.0.1 only.  Port 8766 — 8765 is the apply
dashboard, and both should be able to run at once.

    python3 triage/server.py            # http://127.0.0.1:8766

Two surfaces on the one port, because they are two halves of one job:

    /        index.html  — judge the corpus, one card at a time  (approve/deny)
    /apply   apply.html  — work what you approved, as a checklist (applied/skip)

Every write here is a single small SQLite statement.  The speed of the UI comes
from the client deciding optimistically and flushing in batches (see
index.html), not from anything clever on this side.
"""
import argparse
import json
import os
import re
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import triage_load as TL                              # noqa: E402
from triage import apply_queue as AQ                  # noqa: E402
from triage import lanes as L                         # noqa: E402

# The scout half is imported lazily and never at module scope: it needs numpy,
# corpus.db and scout_index.npz, none of which exist on a fresh clone, and the
# existing lanes must keep working when they are missing.  _scout() returns None
# rather than raising, and every scout route degrades to a message.
_SCOUT = None
_SCOUT_ERR = ""


def _scout(con=None):
    global _SCOUT, _SCOUT_ERR
    if _SCOUT is None and not _SCOUT_ERR:
        try:
            from triage import scout as SC
            _SCOUT = SC.Scout(triage_con=con or TL.connect())
        except Exception as e:                        # noqa: BLE001
            _SCOUT_ERR = f"{type(e).__name__}: {e}"
    return _SCOUT


INDEX = os.path.join(HERE, "index.html")
APPLY = os.path.join(HERE, "apply.html")
VERDICTS = ("approve", "deny", "maybe")
SC_LABEL = "Scout · full corpus"

# Everything a card needs that is NOT one of the per-lane score pairs.
_BASE_COLS = (
    # `ref` is the number on the badge (triage_load.mint_refs).  Denormalised
    # onto `jobs` precisely so it can ride here: lanes.queue_sql() has no join
    # and every one of the thirty lanes goes through it.
    "ref",
    "job_key", "company", "company_norm", "title", "location", "work_mode",
    "posted", "salary_min", "salary_max", "salary_currency", "salary_period",
    "ai_band", "startup_tier", "startup_evidence", "skills", "source",
    "is_agg", "date_basis", "date_weak",
    "bucket", "employment_type", "seniority", "company_size",
    "company_stage", "company_industry", "apply_url", "job_url",
    "feed", "lane_tags",
)

# The score pairs are DERIVED from lanes.LANE_SCORE rather than written out,
# because the hand-kept version silently went stale: `legalai` was added to
# LANE_SCORE and never here, so every card in that lane came back with
# rank_score 0 and rank_why '' -- `r.get(s_col) or 0` below reads a missing
# column as a zero rather than raising, and a lane whose whole premise is
# "found through the employer, not the words" rendered neither its score pill
# nor its reason.  lanes.py and cards() below both promise that adding a
# pair to LANE_SCORE is all you have to do; this is what makes that true.
CARD_COLS = ", ".join(_BASE_COLS + tuple(
    c for pair in list(L.LANE_SCORE.values()) + [L.DEFAULT_SCORE]
    for c in pair if c not in _BASE_COLS))


class API:
    """Request handlers, kept free of HTTP so they stay testable."""

    @staticmethod
    def lanes(con):
        # `groups` is the rail's headings, in draw order, straight from
        # lanes.GROUPS.  Sent rather than hardcoded in index.html for the same
        # reason the apply page's facets are server-owned: one definition of
        # what the menu contains, so a group added to lanes.py appears without
        # a matching edit to the template — and a lane whose group the client
        # doesn't recognise still has somewhere to land (see renderLanes).
        lanes = L.counts(con)
        # Appended here rather than in lanes.py because it is not a predicate
        # over `jobs` and has no COUNT(*) to run: `remaining` is how many rows
        # of corpus.db are still eligible, which only the in-memory index knows.
        sc = _scout(con)
        if sc is not None:
            lanes.append({"key": L.SCOUT_LANE, "label": SC_LABEL,
                          "group": "scout",
                          "remaining": int(sc.eligible(False).sum())})
        return {"lanes": lanes,
                "groups": [{"key": k, "label": v} for k, v in L.GROUPS],
                "cutoff": L.recent_cutoff(),
                "blocked": con.execute(
                    "SELECT COUNT(*) FROM blocked_companies").fetchone()[0],
                "decided": con.execute(
                    "SELECT COUNT(*) FROM decisions").fetchone()[0]}

    @staticmethod
    def cards(con, lane, n=50, q=None, sort=None, offset=0):
        lm = L.lane_map()
        if lane not in lm:
            return {"error": f"no such lane: {lane}"}
        try:
            order = L.order_for(lane, sort)
        except ValueError as e:
            return {"error": str(e)}
        label, pred, params = lm[lane]
        params = list(params)
        if q:
            # A ref typed into the filter box is a lookup, not a text search.
            # Without this it matches no title and no company, and the lane
            # renders the "Lane clear" empty state -- which reads as "you have
            # finished this lane" rather than "nothing matched".  Requires the
            # J so a bare number stays a text search: titles carry digits
            # (`Engineer II`, `Tier 2`) and a ref is never typed without it.
            m = re.fullmatch(r"[Jj](\d+)", q.strip())
            if m:
                pred += " AND ref = ?"
                params.append(int(m.group(1)))
            else:
                pred += " AND (title LIKE ? OR company LIKE ?)"
                params += [f"%{q}%", f"%{q}%"]
        # The client sends how many rows it already holds that are STILL
        # undecided, not a page number: a row it decided has left UNDECIDED on
        # its own, so counting that one into the offset would skip a real row.
        try:
            off = max(0, int(offset or 0))
        except (TypeError, ValueError):
            off = 0
        sql = L.queue_sql(pred, CARD_COLS, order=order,
                          limit=max(1, min(int(n), 200)), offset=off)
        rows = [dict(r) for r in con.execute(sql, params)]
        # The card shows whichever score put the row in front of you, so it
        # reads as the lane's own ranking rather than a second one to reconcile.
        # Resolved here from lanes.LANE_SCORE rather than in the template: the
        # card used to choose the column with a ternary over lane names, which
        # meant every lane added afterwards rendered with no number and no
        # reason until someone remembered to edit index.html.
        s_col, w_col = L.score_cols(lane)
        for r in rows:
            r["lane"] = lane
            r["rank_score"] = r.get(s_col) or 0
            r["rank_why"] = r.get(w_col) or ""
        # Which resume to send, from lanes.LANE_RESUME — a constant map, not a column,
        # so adding a swath needs no migration and triage_export.py reads the same source.
        res = L.resume_for(lane)
        out = {"lane": lane, "label": label, "cards": rows}
        if res:
            code, name, pdf, draft = res
            out["resume"] = {"code": code, "name": name, "pdf": pdf, "draft": draft}
        return out

    @staticmethod
    def decide(con, decisions):
        """Batch of {job_key, verdict, lane, status?}.  Last write wins.

        `written` is the number of rows the database actually changed, not the
        number asked for.  The INSERT draws ct_key from `jobs`, so a job_key
        that isn't in the corpus writes nothing at all — reporting the request
        size here would let a decision vanish while the UI showed it saved.

        An optional `status` (apply_queue.STATUSES) records in the same
        keystroke what you have already *done* about the job — shift-A on a
        card is approve + 'applied', for the jobs you opened with `o`, applied
        to, and would otherwise have to find again on /apply.  It rides
        apply_queue.set_status rather than writing `applications` here, so the
        row is the same row that page writes: same applied_at stamp, same
        carry-forward over a rebuild, and 'to_apply' still stored as no row.
        The verdict is what puts it in the apply queue at all (only a manual
        approve is in it), so the two halves belong in one request — a flush
        that landed the status and lost the verdict would file an application
        against a job the queue cannot show.
        """
        # Two shapes, and the split is load-bearing.  A card from one of the
        # predicate lanes is a row in `jobs`, so its ct_key is drawn from there
        # and a job_key not in the corpus deliberately writes nothing (see
        # `written`, below).  A scout card is a row in corpus.db, which `jobs`
        # has never heard of -- 24 of the top 200 scout cards are not in `jobs`
        # at all -- so it carries its own ct_key and takes the VALUES path.
        # Keeping them apart means the existing path is byte-identical: a
        # caller that sends no ct_key cannot start writing blank-ct_key rows.
        good = [(d.get("job_key"), d.get("verdict"), d.get("lane") or "")
                for d in decisions
                if d.get("job_key") and d.get("verdict") in VERDICTS
                and not d.get("ct_key")]
        byct = [(d.get("job_key"), d.get("ct_key"), d.get("verdict"),
                 d.get("lane") or "")
                for d in decisions
                if d.get("job_key") and d.get("verdict") in VERDICTS
                and d.get("ct_key")]
        work = [(d.get("job_key"), d.get("status")) for d in decisions
                if d.get("job_key") and d.get("status") in AQ.STATUSES]
        if not good and not byct and not work:
            return {"written": 0, "asked": len(decisions)}
        before = con.total_changes
        if good:
            con.executemany("""
                INSERT INTO decisions
                    (job_key, ct_key, verdict, reason, lane, decided_at)
                SELECT ?, ct_key, ?, 'manual', ?, datetime('now')
                  FROM jobs WHERE job_key = ?
                ON CONFLICT(job_key) DO UPDATE SET
                    verdict=excluded.verdict, reason='manual',
                    lane=excluded.lane, decided_at=excluded.decided_at
            """, [(k, v, ln, k) for k, v, ln in good])
            con.commit()
        if byct:
            con.executemany("""
                INSERT INTO decisions
                    (job_key, ct_key, verdict, reason, lane, decided_at)
                VALUES (?, ?, ?, 'manual', ?, datetime('now'))
                ON CONFLICT(job_key) DO UPDATE SET
                    verdict=excluded.verdict, reason='manual',
                    lane=excluded.lane, decided_at=excluded.decided_at
            """, byct)
            con.commit()
            # Hide every copy of the job in the live index at once: the corpus
            # keeps all 382,092 duplicate postings, so retiring one row alone
            # would let the same job return under the next source's copy.
            sc = _scout(con)
            if sc is not None:
                for _, ct, _, _ in byct:
                    sc.mark(ct)
        written = con.total_changes - before
        out = {"written": written, "asked": len(decisions)}
        if written < len(good) + len(byct):
            out["missing"] = [k for k in
                              [g[0] for g in good] + [b[0] for b in byct]
                              if not con.execute(
                                  "SELECT 1 FROM decisions WHERE job_key=?",
                                  (k,)).fetchone()]
        # After `written` is taken, never before: set_status writes rows of its
        # own and counting them as decisions would hide a lost verdict.
        if work:
            out["worked"] = sum(AQ.set_status(con, k, s) for k, s in work)
        return out

    # ----------------------------------------------------------------- scout
    @staticmethod
    def scout_cards(con, body):
        """Rank the whole gated corpus against a profile sent with the request.

        The profile travels in the body rather than living on the server so a
        slider is stateless: every drag is one self-contained request, two tabs
        cannot fight over the active weights, and nothing has to be persisted
        before you can see what a weight change does.
        """
        sc = _scout(con)
        if sc is None:
            return {"error": _SCOUT_ERR or "scout index not built",
                    "cards": [], "hint": "python3 corpus_build.py && "
                                         "python3 corpus_load.py && "
                                         "python3 scout_index.py"}
        import score_profile as SP
        prof = body.get("profile")
        prof = SP.normalise(prof) if prof else (
            SP.load_all().get(body.get("name") or "default")
            or SP.default_profile())
        if body.get("reset"):
            sc.served[:] = False
        try:
            n = max(1, min(int(body.get("n") or 60), 200))
        except (TypeError, ValueError):
            n = 60
        try:
            ex = min(max(float(body.get("explore", 0.15)), 0.0), 0.9)
        except (TypeError, ValueError):
            ex = 0.15
        # `con` rides along so the ref lookup reuses this request's triage
        # connection instead of opening its own (scout.Scout.refs).
        out = sc.cards(prof, body.get("q") or "", n, ex, tcon=con)
        out["lane"] = L.SCOUT_LANE
        out["label"] = SC_LABEL
        for c in out["cards"]:
            c["lane"] = L.SCOUT_LANE
            c["rank_score"] = c["score"]
            c["rank_why"] = " · ".join(f"{k} {v:+g}" for k, v in c["why"][:8])
        return out

    @staticmethod
    def scout_profile(con, body=None):
        import score_features as F
        import score_profile as SP
        if body is None:
            return {"profiles": SP.load_all(),
                    "schema": [{"name": n, "axis": a, "kind": k,
                                "default": pt} for n, a, k, pt in F.SCHEMA],
                    "axes": sorted(set(F.AXIS.values())),
                    "bonus_axes": list(F.BONUS_AXES),
                    "families": list(F.FAMILIES)}
        name = (body.get("name") or "").strip() or "default"
        prof = SP.normalise(body.get("profile") or {})
        prof["name"] = name
        if body.get("delete") and name != "default":
            all_p = SP.load_all()
            all_p.pop(name, None)
            SP.save_all(all_p)
            return {"deleted": name, "profiles": SP.load_all()}
        all_p = SP.load_all()
        all_p[name] = prof
        SP.save_all(all_p)
        return {"saved": name, "profiles": all_p}

    @staticmethod
    def company(con, norm):
        """What a kill would cost — the confirm-dialog number."""
        r = con.execute("""
            SELECT COUNT(*) n,
                   COALESCE(MAX(company), '') company
              FROM jobs WHERE company_norm = ? AND live = 1
               AND NOT EXISTS (SELECT 1 FROM decisions d
                               WHERE d.job_key = jobs.job_key)
        """, (norm,)).fetchone()
        total = con.execute("SELECT COUNT(*) FROM jobs WHERE company_norm=?",
                            (norm,)).fetchone()[0]
        return {"company_norm": norm, "company": r["company"],
                "undecided": r["n"], "total": total}

    @staticmethod
    def kill_company(con, norm):
        """Block the company and deny every live, undecided row it has."""
        if not norm:
            return {"error": "blank company cannot be killed as a group"}
        name = con.execute(
            "SELECT COALESCE(MAX(company),'') c FROM jobs WHERE company_norm=?",
            (norm,)).fetchone()["c"]
        cur = con.execute("""
            INSERT OR IGNORE INTO decisions
                (job_key, ct_key, verdict, reason, lane, decided_at)
            SELECT job_key, ct_key, 'deny', 'company_kill:' || ?, 'kill',
                   datetime('now')
              FROM jobs WHERE company_norm = ? AND live = 1
        """, (name, norm))
        killed = cur.rowcount
        con.execute("""
            INSERT INTO blocked_companies
                (company_norm, company, blocked_at, n_killed)
            VALUES (?, ?, datetime('now'), ?)
            ON CONFLICT(company_norm) DO UPDATE SET n_killed = n_killed + ?
        """, (norm, name, killed, killed))
        con.commit()
        TL.write_blocklist(con)          # keep the text mirror in step
        return {"company": name, "company_norm": norm, "killed": killed}

    @staticmethod
    def undo(con, job_key=None, unblock=None, drop_work=False):
        if unblock:
            name = con.execute(
                "SELECT COALESCE(MAX(company),'') c FROM blocked_companies "
                "WHERE company_norm=?", (unblock,)).fetchone()["c"]
            con.execute("DELETE FROM blocked_companies WHERE company_norm=?",
                        (unblock,))
            # Only the denials the block created; manual verdicts survive.
            cur = con.execute(
                "DELETE FROM decisions WHERE reason = 'company_kill:' || ?",
                (name,))
            con.commit()
            TL.write_blocklist(con)
            return {"unblocked": name, "restored": cur.rowcount}
        cur = con.execute("DELETE FROM decisions WHERE job_key=?", (job_key,))
        out = {"undone": cur.rowcount}
        con.commit()
        # Only when the undone keystroke was the one that filed the
        # application: an approve is not allowed to clear work you recorded on
        # /apply.  set_status('to_apply') is the delete, so absence keeps its
        # one meaning here too.
        if drop_work:
            out["work_cleared"] = AQ.set_status(con, job_key, "to_apply")
        return out

    @staticmethod
    def blocked(con):
        return {"blocked": [dict(r) for r in con.execute(
            "SELECT company, company_norm, n_killed, blocked_at "
            "FROM blocked_companies ORDER BY blocked_at DESC")]}

    # ------------------------------------------------------------ apply queue
    @staticmethod
    def apply_list(con, status=None, q=None, sel=None, sort=None):
        """The whole approved set in one response — no paging.

        222 rows is ~120 KB of JSON.  Paging it would buy nothing and would
        cost the thing that makes this page worth having over the card flow:
        being able to scan the list and work it out of order.

        The filters live here and not in the browser so that one definition of
        the queue serves the page, the tab badges and triage_export.py.  Filter
        client-side and the badges would describe a different set than the list
        under them.
        """
        try:
            rows = AQ.queue(con, status or None, q, sel=sel, sort=sort)
            # Counts honour the filters too, so the tab badges describe the
            # same set the list does rather than the whole queue.  The sort is
            # not among them: it reorders the list without changing what is in
            # it, so every count below is the same under all three.
            return {"status": status or "", "counts": AQ.counts(con, q, sel),
                    "facets": AQ.facet_counts(con, status or None, q, sel),
                    "statuses": list(AQ.STATUSES), "jobs": rows}
        except ValueError as e:
            return {"error": str(e)}

    @staticmethod
    def apply_status(con, items):
        try:
            return AQ.set_many(con, items)
        except ValueError as e:
            return {"error": str(e)}

    @staticmethod
    def apply_outcome(con, job_key, outcome):
        try:
            return {"changed": AQ.set_outcome(con, job_key, outcome),
                    "job_key": job_key, "outcome": outcome}
        except ValueError as e:
            return {"error": str(e)}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):                        # quiet by default
        if self.server.verbose:                       # noqa: SLF001
            super().log_message(*a)

    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, (dict, list)) or body is None:
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n))
        except ValueError:
            return {}

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        one = lambda k, d=None: (q.get(k) or [d])[0]   # noqa: E731
        con = TL.connect()
        try:
            if u.path in ("/", "/index.html"):
                with open(INDEX, "rb") as f:
                    return self._send(200, f.read(),
                                      "text/html; charset=utf-8")
            if u.path in ("/apply", "/apply.html"):
                with open(APPLY, "rb") as f:
                    return self._send(200, f.read(),
                                      "text/html; charset=utf-8")
            if u.path == "/api/apply/queue":
                sel = {k: one(k, "") for k in AQ.FACETS}
                return self._send(200, API.apply_list(
                    con, one("status"), one("q"), sel, one("sort")))
            if u.path == "/api/lanes":
                return self._send(200, API.lanes(con))
            if u.path == "/api/cards":
                return self._send(200, API.cards(
                    con, one("lane", "ai12"), one("n", 50), one("q"),
                    one("sort"), one("offset", 0)))
            if u.path == "/api/company":
                return self._send(200, API.company(con, one("norm", "")))
            if u.path == "/api/blocked":
                return self._send(200, API.blocked(con))
            if u.path == "/api/scout/profile":
                return self._send(200, API.scout_profile(con))
            return self._send(404, {"error": "no such route"})
        except Exception as e:                        # noqa: BLE001
            return self._send(500, {"error": str(e)})
        finally:
            con.close()

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        body = self._body()
        con = TL.connect()
        try:
            if u.path == "/api/decide":
                return self._send(200, API.decide(
                    con, body.get("decisions") or []))
            if u.path == "/api/kill-company":
                return self._send(200, API.kill_company(
                    con, body.get("company_norm") or ""))
            if u.path == "/api/undo":
                return self._send(200, API.undo(
                    con, body.get("job_key"), body.get("unblock"),
                    bool(body.get("drop_work"))))
            if u.path == "/api/apply/status":
                return self._send(200, API.apply_status(
                    con, body.get("updates") or []))
            if u.path == "/api/scout/cards":
                return self._send(200, API.scout_cards(con, body))
            if u.path == "/api/scout/profile":
                return self._send(200, API.scout_profile(con, body))
            if u.path == "/api/apply/outcome":
                return self._send(200, API.apply_outcome(
                    con, body.get("job_key") or "", body.get("outcome") or ""))
            return self._send(404, {"error": "no such route"})
        except Exception as e:                        # noqa: BLE001
            return self._send(500, {"error": str(e)})
        finally:
            con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8766)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    if not os.path.exists(TL.DB):
        sys.exit("triage.db missing — run: python3 triage_load.py")
    # Once, at startup, not per request: connect() deliberately doesn't run the
    # DDL, so a database written before a table existed (`applications` was
    # added after the first triage sessions) would 500 every read until the
    # next full load.  CREATE TABLE IF NOT EXISTS is free and this is the only
    # place it needs to happen.
    TL.init().close()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    srv.verbose = args.verbose
    con = TL.connect()
    print(f"Triage -> http://127.0.0.1:{args.port}")
    for L_ in L.counts(con):
        print(f"  {L_['label']:<28} {L_['remaining']:>7,}")
    ac = AQ.counts(con)
    print(f"\nApply  -> http://127.0.0.1:{args.port}/apply")
    print(f"  {'to apply':<28} {ac['to_apply']:>7,}")
    if ac["applying"]:
        print(f"  {'applying (in flight)':<28} {ac['applying']:>7,}")
    print(f"  {'applied':<28} {ac['applied']:>7,}"
          + (f"   (+{ac['skipped']:,} skipped, {ac['dead']:,} dead)"
             if ac["skipped"] or ac["dead"] else ""))

    con.close()
    print("\n  (localhost only; ctrl-C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
