#!/usr/bin/env python3
"""worklist_live.py -- probe the apply URLs in the worklist and record which are dead.

The corpus marks a row live when the last crawl still saw it; the crawl was 2026-08-16 and
postings decay fast (156 of 522 approvals had already gone dead by the time anyone looked --
about 30% in two weeks). So `live = 1` means "was live two days ago", which is not the same
question as "will this form open".

Each ATS announces a dead posting differently and none of them use 404 consistently:

  ashby       a retired posting still returns 200 with the SPA shell -- ~7 KB against
              24-34 KB for a live one. Size is the only signal in the HTML.
  greenhouse  redirects to the board root carrying ?error=true
  lever       404, or redirects to the board root
  workday     200 for BOTH, with an empty <title> for both, and no "no longer available"
              text anywhere -- the earlier note here was wrong. Measured against a bogus
              requisition id on a live tenant: dead 6,117 B, live 20,724 B. Across 25
              sampled live rows the split was 6.6 KB vs 15.5-33.5 KB, so size is the only
              signal, exactly as with ashby. Before this, EVERY dead workday posting
              scored live, and workday is ~19% of the queue.
  rippling    308 redirect. A LIVE rippling page carries the string "This job post is no
              longer available" in its embedded i18n bundle -- on every page, live or
              dead -- so matching the raw body inverted this source completely: 9 of 10
              live postings scored dead while the genuinely dead ones scored unknown.
  himalayas   403s every request from here (Cloudflare). Recorded as `blocked`, which is
              NOT `unknown`: it is a permanent property of the source, and 483 of the
              rows dated 2026-08-18 are himalayas. Nothing short of opening one in a real
              browser can tell you whether it is still live.

MATCH VISIBLE TEXT, NEVER THE RAW BODY. Both wrong verdicts above came from reading
markup as prose. The same trap caught a jobicy page whose stylesheet defines
`span.expired-title`. Everything goes through visible_text() now.

Writes worklist_live.json next to the worklist; apply_worklist.py reads it if present and
strikes the dead rows out. Read-only against the network -- it never submits anything.
"""
import concurrent.futures as cf, json, pathlib, re, sqlite3, sys, urllib.request, urllib.error

HERE = pathlib.Path(__file__).parent
OUT  = HERE / "worklist_live.json"
UA   = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TIMEOUT = 20
DEAD_TEXT = re.compile(
    r"(?i)(no longer (available|accepting|open)|position (has been )?(filled|closed)|"
    r"this job is not|posting (is )?closed|not currently accepting|job not found|"
    r"we are no longer)")

# Script and style bodies are where every false positive so far has come from: an i18n
# string table (rippling) and a CSS class name (jobicy) both read as prose otherwise.
_NOISE = re.compile(r"(?is)<(script|style|template)\b.*?</\1>")


def visible_text(body):
    """Page text with the machine-readable parts removed.

    DEAD_TEXT must only ever see what a reader would see. Matching the raw body is what
    made every live rippling posting score dead -- see the header.
    """
    return _NOISE.sub(" ", body.decode("utf-8", "replace"))


# SPA shells that answer 200 whether or not the posting exists. The only signal is
# weight, so each carries a measured floor: below it the page is the empty shell.
# Both margins are wide (ashby 7 vs 24-34 KB, workday 6.1 vs 15.5-33.5 KB).
SHELL_FLOOR = (("ashbyhq", 12_000, "ashby"), ("myworkdayjobs", 10_000, "workday"))


def probe(url):
    """-> (state, detail). state in {live, dead, unknown, blocked}."""
    low = url.lower()
    req = urllib.request.Request(url, headers={"User-Agent": UA,
                                               "Accept": "text/html,*/*"})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            final = r.geturl()
            body = r.read(400_000)
            code = r.getcode()
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            return "dead", f"HTTP {e.code}"
        # rippling retires a posting by redirecting it away; urllib surfaces the 308
        # because the Location is not followed to a readable page.
        if e.code == 308 and "rippling" in low:
            return "dead", "HTTP 308 (posting withdrawn)"
        # Not a verdict about the job -- a verdict about our access to it.
        if e.code in (403, 429):
            return "blocked", f"HTTP {e.code} (cannot be checked from here)"
        return "unknown", f"HTTP {e.code}"
    except Exception as e:                                   # DNS, TLS, timeout
        return "unknown", type(e).__name__

    n = len(body)

    if "greenhouse" in low:
        if "error=true" in final or re.search(r"/(embed/)?job_board", final):
            return "dead", "redirected to board (?error=true)"
    if "lever.co" in low and re.match(r"https?://jobs\.lever\.co/[^/]+/?$", final):
        return "dead", "redirected to board root"
    for host, floor, name in SHELL_FLOOR:
        if host in low:
            if n < floor:
                return "dead", f"{name} shell only ({n//1000} KB)"
            return "live", f"{n//1000} KB"
    if DEAD_TEXT.search(visible_text(body)):
        return "dead", "page says the posting is closed"
    if code == 200 and n < 2_000:
        return "unknown", f"tiny body ({n} B)"
    return "live", f"{n//1000} KB"


def _arg(flag, default=None):
    """The value after `flag` on argv, or `default`.  argparse would be tidier and would
    also change the meaning of the bare `--all` this script already ships with."""
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def lane_rows(con, key, top):
    """The top `top` UNDECIDED rows of a triage lane, in the lane's own order.

    Added 2026-08-27 for `fastapply`, whose whole premise is that a slot costs four
    minutes -- so a dead form is the most expensive thing in it.  The default query below
    cannot reach these rows: it starts from `decisions`, and a lane is by definition the
    work that has NOT been decided yet.

    Output goes to <lane>_live.json, never to worklist_live.json.  apply_worklist.py
    reads that file to strike rows out of the approved queue, and overwriting it with
    verdicts about a different row set would silently mark the queue's own rows unknown.
    """
    sys.path.insert(0, str(HERE / "triage"))
    import lanes as LN
    lm = LN.lane_map(21)
    if key not in lm:
        sys.exit(f"unknown lane '{key}' -- have: {', '.join(sorted(lm))}")
    _, sql, params = lm[key]
    q = LN.queue_sql(sql, "job_key, company, title, apply_url, posted",
                     order=LN.order_for(key), limit=top)
    return [dict(r) for r in con.execute(q, params)]


def main():
    only_fast = "--all" not in sys.argv
    lane = _arg("--lane")
    top = int(_arg("--top", 200))
    con = sqlite3.connect("file:triage.db?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    if lane:
        rows = lane_rows(con, lane, top)
        out_path = HERE / f"{lane}_live.json"
        print(f"probing top {len(rows)} of lane '{lane}' -> {out_path.name}")
        return run(rows, out_path)
    sql = """
      SELECT j.job_key, j.company, j.title, j.apply_url, j.posted
      FROM decisions d JOIN jobs j USING(job_key)
      WHERE d.verdict='approve' AND j.live=1
        AND NOT EXISTS (SELECT 1 FROM applications a WHERE a.job_key=d.job_key)
        AND j.apply_url NOT LIKE '%himalayas.app%'
        AND j.apply_url NOT LIKE '%/jobgether%'
    """
    if only_fast:
        # The set we can actually decide. workday and rippling joined it once their
        # detectors were fixed; icims and smartrecruiters answer with real text.
        # himalayas/jobgether stay excluded by the query above -- they are not
        # employer forms at all (apply_worklist.tier() ranks them 9 for that reason).
        sql += (" AND (j.apply_url LIKE '%ashbyhq%' OR j.apply_url LIKE '%greenhouse%'"
                " OR j.apply_url LIKE '%lever.co%' OR j.apply_url LIKE '%myworkdayjobs%'"
                " OR j.apply_url LIKE '%ats.rippling.com%'"
                " OR j.apply_url LIKE '%icims%'"
                " OR j.apply_url LIKE '%smartrecruiters%')")
    rows = [dict(r) for r in con.execute(sql + " ORDER BY j.posted DESC")]
    print(f"probing {len(rows)} rows ({'fast-apply only' if only_fast else 'all'})…")
    return run(rows, OUT)


def run(rows, out_path):
    out = {}
    # modest concurrency: these are a handful of ATS hosts and hammering them is both rude
    # and the fastest way to start getting 429s that look like dead postings
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(probe, r["apply_url"]): r for r in rows}
        for i, f in enumerate(cf.as_completed(futs), 1):
            r = futs[f]
            try:
                state, detail = f.result()
            except Exception as e:
                state, detail = "unknown", type(e).__name__
            out[r["job_key"]] = {"state": state, "detail": detail}
            if i % 20 == 0 or i == len(rows):
                print(f"  {i}/{len(rows)}")
            if state == "dead":
                print(f"    dead  {r['company'][:26]:26} {r['title'][:42]:42} {detail}")

    out_path.write_text(json.dumps(out, indent=1))
    tally = {}
    for v in out.values():
        tally[v["state"]] = tally.get(v["state"], 0) + 1
    print(f"\n  -> {out_path.name}")
    for k in ("live", "dead", "unknown", "blocked"):
        print(f"     {k:8s} {tally.get(k,0)}")
    if tally.get("blocked"):
        print(f"\n     `blocked` is not `unknown`: the host refused us, so those rows"
              f"\n     were never checked. Do not read them as live.")

if __name__ == "__main__":
    main()
