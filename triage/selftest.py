#!/usr/bin/env python3
"""triage/selftest.py — carry-forward regression test.

    python3 triage/selftest.py

The property the whole design rests on.

Runs against a throwaway DB and synthetic CSVs derived from the real corpus:
  1. same CSV reloaded            -> decisions unchanged, queue does not refill
  2. job_key flips, company+title -> decisions carry via ct_key
  3. job_key flips                -> 'applied' carries too, and does NOT
                                     double-count once the old row is dead
  4. rows vanish from the corpus  -> live=0, verdict retained
  5. blocklist file hand-edited   -> company unblocks, manual verdicts survive
  6. a second feed is loaded      -> neither feed retires the other, no lane
                                     leaks, and the contract lane ranks by
                                     score rather than by AI tier
  7. the apply page's filters     -> each facet returns what it claims, the
                                     three compose, badges count the filtered
                                     set, and a chip's number is what clicking
                                     it gives
  8. inferred-US rows             -> basis_weak is set, and sorts behind proven
  9. the Sort control             -> 'fit' unchanged, dates lead when asked,
                                     undated rows sort last and never first,
                                     and the piles stay put on the apply page

(3) is the one that costs real money if it breaks: a reset 'applied' flag means
applying to the same job a second time.  (6) is the one that would empty a lane
without telling you.  (7) is the one that would show you a confident count of a
set you are not looking at.
"""
import csv, os, shutil, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
csv.field_size_limit(10 ** 9)

import glob, tempfile
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SP = tempfile.mkdtemp(prefix="triage-selftest-")
# Prefer a real corpus cut when one exists -- real data has shapes a generator
# will not invent.  Fall back to the synthetic fixture so this suite runs on a
# clone that has never built the corpus; before that fallback existed, the glob
# came back empty and 200 assertions died on an IndexError.
_real = sorted(glob.glob(os.path.join(ROOT, "MA_US_Remote_*.csv")))
if _real:
    SRC = _real[-1]
else:
    from triage import fixture as _FX
    SRC = _FX.write(os.path.join(SP, "MA_US_Remote_fixture.csv"), 4000)
DB = os.path.join(SP, "carry_test.db")
for p in (DB, DB + "-wal", DB + "-shm"):
    if os.path.exists(p):
        os.remove(p)

import triage_load as TL
from triage import apply_queue as AQ
from triage import ref as RF
TL.BLOCKLIST = os.path.join(SP, "test_blocklist.txt")   # don't touch the real one
if os.path.exists(TL.BLOCKLIST):
    os.remove(TL.BLOCKLIST)

N = 3000
# Two samples, one pass.  ROWS is the head of the file and drives (1)-(6).
#
# SPREAD is for (7).  The corpus is sorted, so its head is a single bucket and
# not one aggregator: a filter test built from it would have nothing on one
# side of every facet and would pass without ever filtering anything.  Striding
# the whole file spans the buckets and the aggregators, and the extra pull on
# banded rows is because AI roles are ~2% of the corpus — at a flat 1-in-90 too
# few survive to still intersect with the other two facets.
#
# South FL gets a pull of its own for the same reason the banded rows do: it is
# ~6% of the corpus and sits in one block at the tail (ma_remote_build sorts on
# BUCKET_RANK), so a flat stride leaves the loc=sfl facet with too few rows to
# still intersect the other two.
with open(SRC, newline="", encoding="utf-8") as f:
    r = csv.reader(f); HDR = next(r)
    AI, BK = HDR.index("ai_band"), HDR.index("bucket")
    ROWS, SPREAD = [], []
    for i, row in enumerate(r):
        if len(row) < len(HDR):
            continue
        if i < N:
            ROWS.append(row)
        if (i % 90 == 0
                or ((row[AI] or " ")[0] in "123" and i % 3 == 0)
                or (row[BK] == "South FL" and i % 12 == 0)):
            SPREAD.append(row)
KI, CI, TI = HDR.index("job_key"), HDR.index("company"), HDR.index("title")

def write(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(HDR); w.writerows(rows)
    return path

def load(path, feed="main"):
    con = TL.init(DB)
    TL.sync_blocklist(con)
    mark = TL.stamp()
    n = TL.load(con, path, mark, feed)
    gone = TL.retire(con, mark, feed)
    carried = TL.carry_forward(con)
    carried_app = TL.carry_forward_applications(con)
    # This helper is a hand-copy of triage_load.main()'s body, so anything added
    # to that sequence has to be added here too or the whole suite runs against
    # a state the real loader never produces.  mint_refs is exactly that: leave
    # it out and every section below sees ref=0 on every row, silently.
    # corpus="" so the test never reaches the real 1.2GB corpus.db -- the
    # fixtures are the point, and attaching it would make the run depend on a
    # file a fresh clone does not have.
    minted, repointed = TL.mint_refs(con, corpus="")
    blocked = TL.apply_blocks(con, mark)
    TL.write_blocklist(con)
    return con, dict(loaded=n, retired=gone, carried=carried,
                     carried_app=carried_app, blocked=blocked,
                     minted=minted, repointed=repointed)

def counts(con):
    return dict(con.execute(
        "SELECT verdict, COUNT(*) FROM decisions GROUP BY verdict").fetchall())

def ok(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}" + (f"  {detail}" if detail else ""))
    return cond


FIXTURE = not _real
skipped = []


def corpus_only(label):
    """Record an assertion that only a real corpus cut can exercise.

    Three kinds end up here: orderings that need the date spread and pile
    composition of real data, vocabulary assertions that pin the CONTENTS of
    profile.json rather than the mechanism reading it, and lanes whose builder
    is not part of this distribution.  They are skipped rather than deleted --
    a deleted assertion stops being a claim about the system, while a skipped
    one still says what it would check and why it cannot here.

    Never reached when a real MA_US_Remote_*.csv is present: then they run.
    """
    print(f"  SKIP  {label}  (needs a real corpus cut)")
    skipped.append(label)
    return False


fails = []

# ---------------------------------------------------------------- 1. reload
v1 = write(os.path.join(SP, "v1.csv"), ROWS)
con, s = load(v1)
print(f"\n[1] initial load: {s['loaded']:,} rows")

# decide on 20 rows that have a real company (so ct_key is non-blank)
picks = [r for r in ROWS if r[CI].strip()][:20]
con.executemany(
    "INSERT INTO decisions (job_key, ct_key, verdict, reason, lane, decided_at) "
    "SELECT ?, ct_key, ?, 'manual', 'ai12', '2026-08-04 00:00:00' "
    "FROM jobs WHERE job_key=?",
    [(r[KI], "approve" if i % 2 == 0 else "deny", r[KI])
     for i, r in enumerate(picks)])
con.commit()

# Work some of what was approved: 4 applied, 2 skipped, 2 applying, 2 left to
# apply.  Only the even picks are approves, and only approves reach the apply
# queue.  The 'applying' pair is here rather than in a section of its own so
# that [3b] carries it over the job_key flip too — a half-finished application
# that silently resets to 'to apply' after a corpus rebuild is the same class
# of bug as a reset 'applied' flag, just quieter.
APPROVED = [r[KI] for i, r in enumerate(picks) if i % 2 == 0]
for k in APPROVED[:4]:
    AQ.set_status(con, k, "applied")
for k in APPROVED[4:6]:
    AQ.set_status(con, k, "skipped")
for k in APPROVED[6:8]:
    AQ.set_status(con, k, "applying")
aq_before = AQ.counts(con)
print(f"    apply queue: {aq_before}")

base = counts(con)
undecided_before = con.execute(
    "SELECT COUNT(*) FROM jobs WHERE live=1 AND NOT EXISTS "
    "(SELECT 1 FROM decisions d WHERE d.job_key=jobs.job_key)").fetchone()[0]
print(f"    decided 20: {base}")
con.close()

con, s = load(v1)
after = counts(con)
undecided_after = con.execute(
    "SELECT COUNT(*) FROM jobs WHERE live=1 AND NOT EXISTS "
    "(SELECT 1 FROM decisions d WHERE d.job_key=jobs.job_key)").fetchone()[0]
print(f"[2] reload of the identical CSV")
fails.append(not ok("decisions unchanged", after == base, f"{after}"))
fails.append(not ok("queue did not refill",
                    undecided_after == undecided_before,
                    f"{undecided_after:,} == {undecided_before:,}"))
con.close()

# ------------------------------------------------- 3. job_key flips (ct_key)
flipped = []
for r in ROWS:
    r2 = list(r)
    if r[KI] in {p[KI] for p in picks}:
        r2[KI] = "ats|OTHERSOURCE|x|" + r[KI].split("|")[-1]   # same job, new key
    flipped.append(r2)
v2 = write(os.path.join(SP, "v2.csv"), flipped)
con, s = load(v2)
print(f"[3] job_key flipped on all 20 decided rows "
      f"(carried={s['carried']}, retired={s['retired']})")
newkeys = {"ats|OTHERSOURCE|x|" + p[KI].split("|")[-1] for p in picks}
q = ",".join("?" * len(newkeys))
carried_ok = con.execute(
    f"SELECT COUNT(*) FROM decisions WHERE job_key IN ({q})",
    list(newkeys)).fetchone()[0]
fails.append(not ok("all 20 verdicts followed the new job_key",
                    carried_ok == 20, f"{carried_ok}/20"))
still_queued = con.execute(
    f"SELECT COUNT(*) FROM jobs WHERE live=1 AND job_key IN ({q}) "
    f"AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key=jobs.job_key)",
    list(newkeys)).fetchone()[0]
fails.append(not ok("none of them came back into the queue", still_queued == 0,
                    f"{still_queued} requeued"))
verd = dict(con.execute(
    f"SELECT verdict, COUNT(*) FROM decisions WHERE job_key IN ({q}) "
    f"GROUP BY verdict", list(newkeys)).fetchall())
fails.append(not ok("approve/deny split preserved",
                    verd == {"approve": 10, "deny": 10}, str(verd)))

# ------------------------------------- 3b. the apply status follows the flip
aq_after = AQ.counts(con)
fails.append(not ok("apply queue identical after the job_key flip",
                    aq_after == aq_before, f"{aq_after} vs {aq_before}"))
newapp = con.execute(
    f"SELECT COUNT(*) FROM applications WHERE job_key IN ({q})",
    list(newkeys)).fetchone()[0]
fails.append(not ok("all 8 worked jobs followed the new job_key",
                    newapp == 8, f"{newapp}/8"))
carried_ing = con.execute(
    f"SELECT COUNT(*) FROM applications a JOIN jobs j USING(job_key) "
    f"WHERE j.live=1 AND a.status='applying' AND a.job_key IN ({q})",
    list(newkeys)).fetchone()[0]
fails.append(not ok("in-flight applications survived the flip too",
                    carried_ing == 2, f"{carried_ing}/2"))
# The predecessor rows are deliberately left behind, so the table really does
# hold two rows per worked ct_key.  That is exactly the shape that would
# double-count if any read forgot to filter live=1 — which is what the
# assertion above is guarding.
total_rows = con.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
fails.append(not ok("dead predecessor rows kept but not counted",
                    total_rows == 16 and aq_after["applied"] == 4
                    and aq_after["applying"] == 2,
                    f"{total_rows} rows, {aq_after['applied']} applied, "
                    f"{aq_after['applying']} applying"))
stale = con.execute(
    "SELECT COUNT(*) FROM applications a JOIN jobs j USING(job_key) "
    "WHERE j.live = 0").fetchone()[0]
fails.append(not ok("the extra rows are the dead ones", stale == 8, f"{stale}"))
# 'to apply' has to stay the absence of a row, or the counts above are luck.
stored = con.execute(
    "SELECT COUNT(*) FROM applications WHERE status='to_apply'").fetchone()[0]
fails.append(not ok("'to_apply' is never stored", stored == 0, f"{stored} rows"))

# ------------------------------------------- 3c. what 'applying' promises
# The date on a row is the date you SUBMITTED.  If 'applying' stamped one, the
# workbook would tell a recruiter you applied on the day you opened the form,
# and every downstream count of "applied this week" would be wrong.
print("[3c] the in-flight status")
ing = con.execute(
    "SELECT a.job_key, a.applied_at FROM applications a JOIN jobs j "
    "USING(job_key) WHERE j.live=1 AND a.status='applying'").fetchall()
fails.append(not ok("'applying' stamps no applied_at",
                    all(not r["applied_at"] for r in ing),
                    str([r["applied_at"] for r in ing])))
# ...but arriving at 'applied' later must stamp, or the row never gets a date.
mid = ing[0]["job_key"]
AQ.set_status(con, mid, "applied")
got = con.execute("SELECT status, applied_at FROM applications WHERE job_key=?",
                  (mid,)).fetchone()
fails.append(not ok("applying -> applied stamps the date on arrival",
                    got["status"] == "applied" and bool(got["applied_at"]),
                    f"{got['status']} / {got['applied_at']!r}"))
# Backing out has to remove the row, not store 'to_apply' — the whole queue is
# built on absence meaning unworked.
back = ing[1]["job_key"]
AQ.set_status(con, back, "to_apply")
gone = con.execute("SELECT COUNT(*) FROM applications WHERE job_key=?",
                   (back,)).fetchone()[0]
fails.append(not ok("applying -> to_apply deletes the row", gone == 0,
                    f"{gone} rows left"))
c3 = AQ.counts(con)
fails.append(not ok("counts still partition the queue",
                    c3["total"] == sum(c3[s] for s in AQ.STATUSES)
                    and c3["applying"] == 0 and c3["applied"] == 5,
                    f"{c3}"))
# The queue must be able to return the new status, and the All tab has to float
# it above the untouched rows rather than bury it with the worked ones.
AQ.set_status(con, back, "applying")
only = AQ.queue(con, "applying")
fails.append(not ok("the applying tab returns exactly its own rows",
                    len(only) == 1 and only[0]["job_key"] == back,
                    f"{len(only)} rows"))
allq = AQ.queue(con)
fails.append(not ok("in-flight sorts to the top of the All tab",
                    allq and allq[0]["status"] == "applying",
                    allq[0]["status"] if allq else "empty"))
# Excel shows a label, triage.db stores a value; harvest reads the label back.
# A status the export cannot name is a status that silently becomes unknown.
import triage_export as TE                                        # noqa: E402
fails.append(not ok("'applying' round-trips through the export labels",
                    TE.LABEL["applying"] == "Applying"
                    and TE.UNLABEL[TE.LABEL["applying"].casefold()] == "applying"
                    and TE.UNLABEL["to apply"] == "to_apply",
                    f"{TE.LABEL.get('applying')!r}"))

# ----------------------------- 3d. approve + applied in one card keystroke
# shift-A on a triage card is one request that has to land as two writes: the
# manual approve (without it the job is in no queue at all, and the
# application would be filed against something the apply page cannot show) and
# the 'applied' row.  Its undo has to take back both — but ONLY its own, which
# is the second half of the test: a plain undo of a plain approve must not
# reach into `applications` and wipe work recorded on /apply.
print("[3d] the card flow's applied key")
from triage.server import API                                     # noqa: E402
from triage import lanes as LN                                    # noqa: E402
fresh = con.execute(
    "SELECT job_key FROM jobs WHERE live=1 AND company <> '' AND NOT EXISTS "
    "(SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key) LIMIT 1"
).fetchone()["job_key"]
c_before = AQ.counts(con)
r = API.decide(con, [{"job_key": fresh, "verdict": "approve", "lane": "ai12",
                      "status": "applied"}])
fails.append(not ok("one keystroke reports both writes",
                    r["written"] == 1 and r.get("worked") == 1, str(r)))
got = con.execute(
    "SELECT d.verdict, d.reason, a.status, a.applied_at FROM decisions d "
    "LEFT JOIN applications a USING(job_key) WHERE d.job_key=?",
    (fresh,)).fetchone()
fails.append(not ok("it is a MANUAL approve, so the apply page can see it",
                    got["verdict"] == "approve" and got["reason"] == "manual",
                    f"{got['verdict']}/{got['reason']}"))
fails.append(not ok("...and applied, dated today",
                    got["status"] == "applied" and bool(got["applied_at"]),
                    f"{got['status']} / {got['applied_at']!r}"))
c_now = AQ.counts(con)
fails.append(not ok("it never passes through 'to apply'",
                    c_now["applied"] == c_before["applied"] + 1
                    and c_now["to_apply"] == c_before["to_apply"],
                    f"{c_now} vs {c_before}"))
gone = con.execute(LN.queue_sql("job_key = ?", "COUNT(*)", order=False),
                   (fresh,)).fetchone()[0]
fails.append(not ok("and it leaves the triage lane like any other verdict",
                    gone == 0, f"{gone} still queued"))
u = API.undo(con, fresh, drop_work=True)
left = con.execute(
    "SELECT (SELECT COUNT(*) FROM decisions WHERE job_key=?) d, "
    "       (SELECT COUNT(*) FROM applications WHERE job_key=?) a",
    (fresh, fresh)).fetchone()
fails.append(not ok("undo takes back both halves",
                    left["d"] == 0 and left["a"] == 0 and u["undone"] == 1,
                    f"{left['d']} decisions, {left['a']} applications"))
fails.append(not ok("...and the queue is exactly where it started",
                    AQ.counts(con) == c_before, f"{AQ.counts(con)}"))
# `mid` was marked applied in [3c] by hand, the way /apply does it.  Approving
# it again from a card and undoing that must leave the application alone.
API.decide(con, [{"job_key": mid, "verdict": "approve", "lane": "ai12"}])
API.undo(con, mid)
kept_work = con.execute(
    "SELECT status FROM applications WHERE job_key=?", (mid,)).fetchone()
fails.append(not ok("a plain undo does not touch work recorded on /apply",
                    kept_work is not None and kept_work["status"] == "applied",
                    f"{kept_work['status'] if kept_work else 'row deleted'}"))
API.decide(con, [{"job_key": mid, "verdict": "approve", "lane": "ai12"}])
con.close()

# ------------------------------------------------------- 4. rows disappear
v3 = write(os.path.join(SP, "v3.csv"), flipped[500:])
con, s = load(v3)
dead = con.execute("SELECT COUNT(*) FROM jobs WHERE live=0").fetchone()[0]
kept = con.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
print(f"[4] 500 rows dropped from the corpus")
fails.append(not ok("vanished rows marked live=0", dead >= 500, f"{dead:,}"))
fails.append(not ok("their verdicts were retained", kept >= 20, f"{kept} decisions"))
con.close()

# ------------------------------------------------------ 5. blocklist edit
con = TL.init(DB)
# 40, not 20: the job_key flip in [3] legitimately produced 20 carried rows
# alongside the 20 originals. What matters is that the unblock leaves this
# number alone.
manual_before = con.execute(
    "SELECT COUNT(*) FROM decisions WHERE reason='manual'").fetchone()[0]
target = con.execute(
    "SELECT company_norm, company FROM jobs WHERE company_norm<>'' "
    "GROUP BY company_norm ORDER BY COUNT(*) DESC LIMIT 1").fetchone()
norm, name = target["company_norm"], target["company"]
con.execute("INSERT INTO blocked_companies (company_norm, company, blocked_at) "
            "VALUES (?,?,'now')", (norm, name))
con.commit()
killed = TL.apply_blocks(con)
TL.write_blocklist(con)
print(f"[5] blocked {name!r} -> {killed} auto-denied")
con.close()

with open(TL.BLOCKLIST, "w", encoding="utf-8") as f:
    f.write("# hand-edited: line removed\n")          # user deletes the line
con, s = load(v3)
back = con.execute("SELECT COUNT(*) FROM jobs WHERE company_norm=? AND live=1 "
                   "AND NOT EXISTS (SELECT 1 FROM decisions d "
                   "WHERE d.job_key=jobs.job_key)", (norm,)).fetchone()[0]
manual = con.execute("SELECT COUNT(*) FROM decisions "
                     "WHERE reason='manual'").fetchone()[0]
fails.append(not ok("deleting the blocklist line unblocks the company",
                    back == killed, f"{back} back in queue"))
fails.append(not ok("manual verdicts untouched by the unblock",
                    manual == manual_before, f"{manual}/{manual_before}"))
con.close()

# ------------------------------------------------ 6. a second feed coexists
# The failure this guards is silent and total: retire() defines dead as "not
# touched by the current load", so an unscoped sweep makes each feed kill the
# other's entire corpus on every load, verdicts intact and rows invisible.
from triage import lanes as LN                                    # noqa: E402

live_main_before = con2 = None
con = TL.init(DB)
live_main_before = con.execute(
    "SELECT COUNT(*) FROM jobs WHERE live=1 AND feed='main'").fetchone()[0]
con.close()

# A tiny contract feed in the shape contract_pool.py emits.
CH = HDR + ["score", "score_why", "money_score", "money_why", "lane_tags"]
crows = []
# Tags are deliberately overlapping: 0-19 contract, 10-29 fast money, so ten
# rows sit in both lanes and must still be ONE row decided once.
for i in range(30):
    row = [""] * len(HDR)
    row[KI] = f"contract:selftest{i:04d}"
    row[CI] = f"Contract Co {i}"
    row[TI] = f"Contract Engineer {i}"
    row[HDR.index("source")] = "himalayas"
    row[HDR.index("posted")] = "2026-08-01"
    row[HDR.index("ai_band")] = ""
    tags = ","
    if i < 20:
        tags += "contract,"
    if i >= 10:
        tags += "fastmoney,"
    # Ascending score, descending money_score: the two lanes must therefore
    # disagree about which row is first, so neither ordering can pass by
    # accident on insertion order or posted date.
    crows.append(row + [str(10 + i), f"reason {i}",
                        str(100 - i), f"cash {i}", tags])
cpath = os.path.join(SP, "contract.csv")
with open(cpath, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(CH); w.writerows(crows)

con, s = load(cpath, "contract")
print(f"\n[6] loaded a {len(crows)}-row contract feed "
      f"(retired={s['retired']})")
live_main_after = con.execute(
    "SELECT COUNT(*) FROM jobs WHERE live=1 AND feed='main'").fetchone()[0]
live_con = con.execute(
    "SELECT COUNT(*) FROM jobs WHERE live=1 AND feed='contract'").fetchone()[0]
fails.append(not ok("loading contract did not retire the main feed",
                    live_main_after == live_main_before,
                    f"{live_main_after:,} == {live_main_before:,}"))
fails.append(not ok("the contract rows are live", live_con == len(crows),
                    f"{live_con}/{len(crows)}"))

lm = LN.lane_map()
fails.append(not ok("both contract lanes exist",
                    "contract" in lm and "fastmoney" in lm))


def lane_count(key):
    return con.execute(LN.queue_sql(lm[key][1], "COUNT(*)", order=False),
                       lm[key][2]).fetchone()[0]


n_contract, n_money = lane_count("contract"), lane_count("fastmoney")
fails.append(not ok("contract lane holds only its tagged rows",
                    n_contract == 20, f"{n_contract}/20"))
fails.append(not ok("fast-money lane holds only its tagged rows",
                    n_money == 20, f"{n_money}/20"))

# No contract row may appear in ANY main-feed lane, `rest` included — that is
# the lane whose predicate is a complement and would otherwise absorb them.
leaked = 0
for key, _, pred, params in LN.lanes():
    if key in ("contract", "fastmoney"):
        continue
    leaked += con.execute(
        LN.queue_sql(pred + " AND feed='contract'", "COUNT(*)", order=False),
        params).fetchone()[0]
fails.append(not ok("no contract row leaks into a main-feed lane (incl. rest)",
                    leaked == 0, f"{leaked} leaked"))

top = con.execute(LN.queue_sql(lm["contract"][1], "job_key, score",
                               order=LN.order_for("contract"), limit=3),
                  lm["contract"][2]).fetchall()
fails.append(not ok("the contract lane ranks by score, highest first",
                    [r["score"] for r in top] == [29.0, 28.0, 27.0],
                    str([r["score"] for r in top])))
mtop = con.execute(LN.queue_sql(lm["fastmoney"][1], "job_key, money_score",
                                order=LN.order_for("fastmoney"), limit=3),
                   lm["fastmoney"][2]).fetchall()
fails.append(not ok("the fast-money lane ranks by money_score, not by score",
                    [r["money_score"] for r in mtop] == [90.0, 89.0, 88.0],
                    str([r["money_score"] for r in mtop])))

# The point of tagging rather than a second feed: a job in both lanes is ONE
# row, so judging it once takes it out of both.  Two feeds would need two
# job_keys for one job and could hold two different verdicts about it.
both = con.execute(
    "SELECT job_key FROM jobs WHERE lane_tags LIKE '%,contract,%' "
    "AND lane_tags LIKE '%,fastmoney,%'").fetchall()
fails.append(not ok("rows can sit in both contract lanes", len(both) == 10,
                    f"{len(both)}/10"))
con.execute("INSERT INTO decisions (job_key, ct_key, verdict, reason, lane, "
            "decided_at) SELECT job_key, ct_key, 'deny', 'manual', "
            "'fastmoney', '2026-08-06' FROM jobs WHERE job_key = ?",
            (both[0]["job_key"],))
con.commit()
fails.append(not ok("deciding it in one lane clears it from the other",
                    lane_count("contract") == 19 and lane_count("fastmoney") == 19,
                    f"contract={lane_count('contract')} "
                    f"fastmoney={lane_count('fastmoney')}"))

# And the reverse direction: reloading main must not retire the contract feed.
con.close()
con, s = load(v3)
still = con.execute(
    "SELECT COUNT(*) FROM jobs WHERE live=1 AND feed='contract'").fetchone()[0]
fails.append(not ok("reloading the main corpus did not retire the contract feed",
                    still == len(crows), f"{still}/{len(crows)}"))
con.close()

# --------------------------------- 6b. a tagged lane over the MAIN feed (picks)
# `picks` is the same tag mechanism pointed at feed='main'.  Three things have to
# hold that section 6 does not cover: a main-feed tag must not reach the contract
# lanes, the lane must rank on the score vocab wrote rather than on the AI
# ordering, and — the one that will otherwise waste an afternoon — a main load
# must be shown to BLANK the lane, because triage_load upserts lane_tags from a
# CSV that has no such column.
import vocab as PB                                            # noqa: E402

con = TL.init(DB)
bleed_before = lane_count("contract") + lane_count("fastmoney")
picked = [r["job_key"] for r in con.execute(
    "SELECT job_key FROM jobs WHERE feed='main' AND live=1 "
    "ORDER BY job_key LIMIT 5")]
# Descending score against ascending job_key, so ordering cannot pass by
# accident on insertion order.
con.executemany("UPDATE jobs SET lane_tags=',picks,', score=?, score_why=? "
                "WHERE job_key=?",
                [(90 - i * 10, f"why {i}", k) for i, k in enumerate(picked)])
con.commit()

lm = LN.lane_map()
fails.append(not ok("the picks lane exists", "picks" in lm))
n_picks = lane_count("picks")
fails.append(not ok("picks lane holds exactly its tagged main rows",
                    n_picks == len(picked), f"{n_picks}/{len(picked)}"))

# A main-feed tag must not reach a contract lane: both predicates are a LIKE on
# lane_tags, and only the feed scope keeps them apart.
bleed = lane_count("contract") + lane_count("fastmoney")
fails.append(not ok("picks rows do not bleed into the contract lanes",
                    bleed == bleed_before, f"{bleed}/{bleed_before}"))

ptop = con.execute(LN.queue_sql(lm["picks"][1], "job_key, score",
                                order=LN.order_for("picks"), limit=3),
                   lm["picks"][2]).fetchall()
fails.append(not ok("the picks lane ranks by score, highest first",
                    [r["score"] for r in ptop] == [90.0, 80.0, 70.0],
                    str([r["score"] for r in ptop])))

# One row, one verdict: a pick is still an ordinary main-feed row, so deciding it
# in `picks` has to take it out of `rest`/`ai_all` too.
rest_before = lane_count("rest")
con.execute("INSERT INTO decisions (job_key, ct_key, verdict, reason, lane, "
            "decided_at) SELECT job_key, ct_key, 'approve', 'manual', 'picks', "
            "'2026-08-09' FROM jobs WHERE job_key = ?", (picked[0],))
con.commit()
in_any_main = sum(
    con.execute(LN.queue_sql(pred + " AND jobs.job_key = ?", "COUNT(*)",
                             order=False), list(params) + [picked[0]]).fetchone()[0]
    for key, _, pred, params in LN.lanes() if key != "picks")
fails.append(not ok("deciding a pick clears it from every other main lane",
                    in_any_main == 0 and lane_count("picks") == len(picked) - 1,
                    f"still in {in_any_main} lanes"))

# The documented trap, asserted so it stays documented.
con.close()
con, _ = load(v3)
after = lane_count("picks")
fails.append(not ok("a main load blanks picks (re-run vocab.py after one)",
                    after == 0, f"{after} survived — the caveat may be stale"))

# The scoring vocabulary itself.  Each of these is a defect that reached the
# top of the lane during the first build and was fixed: a desk name scoring as a
# job title, and a company's product line condemning a commercial role.
# Tracks profile.example.json's tier1.  The claim is the mechanism -- whatever
# tier1 names scores 40 -- so this moves if the shipped example profile does.
fails.append(not ok("a target title scores highest",
                    PB.title_fit("Platform Engineer")[0] == 40))
fails.append(not ok("a desk name is not a job title",
                    PB.title_fit("Legal Counsel - Securities & Capital Markets")[0]
                    == 0))
for bad in ("Legal Counsel - Capital Markets", "Administrative Assistant, GTM",
            "Global Capital Markets 2027 Summer Analyst", "GTM Recruiter",
            "Head of Accounting and Fund Administration, SVP"):
    fails.append(not ok(f"excluded: {bad[:38]}", PB.killed_by(bad, "") is not None))
fails.append(not ok("a lab word in the TITLE kills the row",
                    PB.killed_by("Senior Automation Engineer, Platform Biology",
                                 "") is not None))
fails.append(not ok("a lab word in SKILLS kills an engineering title",
                    PB.killed_by("Senior Automation Engineer",
                                 "mechatronics, robotics") is not None))
fails.append(not ok("but not a commercial one — that is the employer, not the job",
                    PB.killed_by("Business Development Leader",
                                 "robotics, lidar") is None))
fails.append(not ok("RPA survives the robotics kill",
                    PB.killed_by("Automation Engineer",
                                 "robotic process automation, UiPath") is None))
con.close()

# ------------------------------- 6c. the eleven swath lanes over the MAIN feed
# Same tag mechanism as `picks`, eleven more times, plus the thing that is new:
# all twelve main-feed lanes must be DISJOINT.  lanes_build.py assigns each row to
# exactly one, because a row carrying two tags is shown twice and two verdicts about
# one job is the failure lanes.py exists to prevent.
import datetime as dt                                               # noqa: E402
import swath_vocab as SV                                            # noqa: E402
import swath_build as SB                                            # noqa: E402
import lanes_build as LB                                            # noqa: E402

con = TL.init(DB)
SWATH_KEYS = list(SV.SWATHS)

lm = LN.lane_map()
missing = [k for k in SWATH_KEYS if k not in lm]
fails.append(not ok("all eleven swath lanes are defined", not missing, str(missing)))

# Every lane must know which resume it wants, or the card has nothing to print.
no_res = [k for k in SWATH_KEYS if not LN.resume_for(k)]
fails.append(not ok("every swath lane maps to a resume", not no_res, str(no_res)))
fails.append(not ok("picks maps to a resume too", LN.resume_for("picks") is not None))

# --- the rail's grouping ------------------------------------------------------
# lanes.group_of DERIVES a lane's rail heading instead of looking it up, so that
# a lane added to _FEED_LANES cannot end up ungrouped — the flat lane bar's
# failure mode was that lanes past the first row were simply not reachable, and
# a lane that silently fell into the wrong group would be the same bug wearing a
# tidier coat.  These assert the derivation, not a list of names.
known = {k for k, _ in LN.GROUPS}
bad = [(k, LN.group_of(k)) for k, _, _, _ in LN.lanes()
       if LN.group_of(k) not in known]
fails.append(not ok("every lane lands in a declared rail group", not bad, str(bad)))

wrong = [k for k in SWATH_KEYS if LN.group_of(k) != "swaths"]
fails.append(not ok("every swath lane groups under Swaths", not wrong, str(wrong)))
fails.append(not ok("contract lanes group under Contract",
                    LN.group_of("contract") == "contract"
                    and LN.group_of("fastmoney") == "contract"))
fails.append(not ok("picks keeps a heading of its own",
                    LN.group_of("picks") == "picks"))
# The corpus lanes are the ones with no tag: they must NOT be swept into a feed
# group by a rule that only asks "is it in _FEED_LANES".
fails.append(not ok("predicate lanes and `rest` group under Corpus",
                    all(LN.group_of(k) == "corpus"
                        for k in ("ai12", "ai_all", "ma", "startup12", "sfl",
                                  "rest"))))
# counts() is what the rail actually reads; a group_of that worked while counts()
# forgot to send it would leave every lane under one heading.
fails.append(not ok("counts() carries the group to the page",
                    all(c.get("group") in known for c in LN.counts(con)),
                    str([c for c in LN.counts(con)
                         if c.get("group") not in known][:3])))

# GROUPS order must match the order lanes() first reaches each group.  The rail
# prints each lane's digit key beside it and those keys index lanes() order, so
# a mismatch draws a digit column reading 1-7, 0, 8, 9 down the page: every key
# works and the rail looks broken.  Nothing else would catch this — it is a
# presentation bug with no wrong answer behind it.
first = []
for k, _, _, _ in LN.lanes():
    g = LN.group_of(k)
    if g not in first:
        first.append(g)
# A group whose lanes are not in lanes() AT ALL is compared as a trailing
# extension rather than as part of the sequence.  `scout` is the only such lane:
# it ranks corpus.db in memory against a weight profile the page sends, so it
# has no predicate to return here and triage/server.py appends it after all the
# others.  What this assertion protects is the digit keys, and those index
# lanes() order and reach only the first ten -- a heading that appears after
# every real lane cannot move one.  A group that appears out of order EARLIER
# still fails, which is the case that draws the broken column.
gnames = [k for k, _ in LN.GROUPS]
fails.append(not ok("rail groups run in lanes() order",
                    first == gnames[:len(first)],
                    f"{first} vs {gnames}"))
fails.append(not ok("a group with no lane in lanes() comes last in GROUPS",
                    all(g not in first for g in gnames[len(first):]),
                    str(gnames[len(first):])))

# Also a real grouping invariant: a group's lanes must be CONTIGUOUS in lanes()
# order.  If they interleave, the rail has to draw one of them out of digit
# sequence no matter which order GROUPS is in.
order = [LN.group_of(k) for k, _, _, _ in LN.lanes()]
runs = [g for i, g in enumerate(order) if i == 0 or order[i - 1] != g]
fails.append(not ok("each group's lanes are contiguous", len(runs) == len(set(runs)),
                    str(runs)))

# Tag a handful of main rows into three different swath lanes and prove the lanes
# separate, do not bleed into contract, and rank on score.
# Undecided only: earlier sections have already judged some rows, and `lane_count`
# counts what is LEFT in a lane — so tagging a decided row would look like the lane
# losing it. That is the lane behaving correctly and the test being wrong.
rows5 = [r["job_key"] for r in con.execute(
    "SELECT job_key FROM jobs WHERE feed='main' AND live=1 "
    "  AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key) "
    "  AND NOT EXISTS (SELECT 1 FROM decisions d2 "
    "                   WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '') "
    "ORDER BY job_key LIMIT 6")]
bleed_before = lane_count("contract") + lane_count("fastmoney")
assign = [("presales", rows5[0], 90), ("presales", rows5[1], 80),
          ("gtm", rows5[2], 70), ("gtm", rows5[3], 60),
          ("rwa", rows5[4], 50), ("trust", rows5[5], 40)]
con.executemany("UPDATE jobs SET lane_tags=?, score=?, score_why='why' "
                "WHERE job_key=?",
                [(f",{k},", sc, jk) for k, jk, sc in assign])
con.commit()

for key, want_n in (("presales", 2), ("gtm", 2), ("rwa", 1), ("trust", 1)):
    got = lane_count(key)
    fails.append(not ok(f"the {key} lane holds exactly its tagged rows",
                        got == want_n, f"{got}/{want_n}"))

bleed = lane_count("contract") + lane_count("fastmoney")
fails.append(not ok("swath rows do not bleed into the contract lanes",
                    bleed == bleed_before, f"{bleed}/{bleed_before}"))

ptop = con.execute(LN.queue_sql(lm["presales"][1], "job_key, score",
                                order=LN.order_for("presales"), limit=2),
                   lm["presales"][2]).fetchall()
fails.append(not ok("a swath lane ranks by score, highest first",
                    [r["score"] for r in ptop] == [90.0, 80.0],
                    str([r["score"] for r in ptop])))

# Disjointness, asserted on the column itself: one tag means exactly two commas.
multi = con.execute(
    "SELECT COUNT(*) c FROM jobs WHERE feed='main' AND lane_tags <> '' "
    "AND LENGTH(lane_tags) - LENGTH(REPLACE(lane_tags, ',', '')) > 2"
).fetchone()["c"]
fails.append(not ok("no main-feed row carries more than one lane tag", multi == 0,
                    f"{multi} rows"))

# Deciding a row in its swath lane must clear it from `rest` as well — that is the
# whole reason these are tags over one feed rather than feeds of their own.
rest_before = lane_count("rest")
tagged_in_rest = con.execute(
    LN.queue_sql(lm["rest"][1], "COUNT(*) c", order=False),
    lm["rest"][2]).fetchone()["c"]
con.execute("INSERT OR REPLACE INTO decisions "
            "(job_key, ct_key, verdict, reason, lane, decided_at) "
            "VALUES (?, '', 'approve', 'manual', 'presales', '2026-08-11')",
            (rows5[0],))
con.commit()
fails.append(not ok("deciding in a swath lane clears the row from that lane",
                    lane_count("presales") == 1, str(lane_count("presales"))))
fails.append(corpus_only("...and from `rest` at the same time") if FIXTURE
             else not ok("...and from `rest` at the same time",
                    lane_count("rest") == rest_before - 1
                    or tagged_in_rest == 0,
                    f"{lane_count('rest')} vs {rest_before}"))

# The trap `picks` documents, one level up: a main load blanks ALL twelve lanes,
# because triage_load upserts lane_tags from a CSV that has no such column.
# lanes_build.py is the one command that restores them.
load(SRC, "main")
con2 = TL.init(DB)
after = sum(lane_count(k) for k in SWATH_KEYS + ["picks"])
fails.append(not ok("a main load empties every swath lane (run lanes_build.py after)",
                    after == 0, f"{after} rows still tagged"))
con2.close()
con.close()

# --- vocabulary regressions, carried forward as rules rather than rediscovered ---
# 1. A domain word is never a job title.  These sit ON the right desk and are the
#    wrong job; vocab.killed_by is what removes them, and swath_build imports
#    it rather than restating it.
for bad in ("Legal Counsel - Digital Assets", "Executive Assistant, Payments",
            "Tokenization 2027 Summer Analyst", "Payments Compliance Officer",
            "Fund Administration Manager"):
    fails.append(not ok(f"excluded from every swath: {bad[:40]}",
                        SB.killed_by(bad, "") is not None))

# 2. F10's framing rule is non-negotiable: institutional finance, never crypto dev.
for bad in ("Backend / Blockchain Engineer - Java / Spring", "Solidity Engineer",
            "Lead Cryptography Engineer", "EntryLevel Crypto Market Specialist",
            "Web3 Developer"):
    fails.append(not ok(f"F10 refuses crypto-dev: {bad[:40]}",
                        not SV.matches("rwa", bad)))
for good in ("Tokenization Product Manager", "Director, Digital Assets",
             "Senior Associate, Private Markets"):
    fails.append(not ok(f"F10 still finds: {good[:40]}", SV.matches("rwa", good)))

# 3. An ambiguous ROLE word is as dangerous as a domain noun.  "Solutions Advisor"
#    is a retail-banking branch title: 336 rows carried it and 331 were Bank of
#    America and Merrill.  NICHE-MAP-2026 §6.
for bad in ("Financial Solutions Advisor - Sharon Financial Center",
            "Credit Solutions Advisor - Raynham",
            "Business Solutions Advisor - New England North Market"):
    fails.append(not ok(f"F2 refuses retail banking: {bad[:44]}",
                        not SV.matches("presales", bad)))
for good in ("Senior Solutions Architect", "Pre-Sales Solution Architect",
             "Partner Solutions Engineer"):
    fails.append(not ok(f"F2 still finds: {good[:40]}", SV.matches("presales", good)))

# 4. The anti-title holds inside the analytics swath, which is where it is tempting.
fails.append(not ok("F5 never surfaces a Data Scientist",
                    SB.killed_by("Senior Data Scientist", "") is not None))

# 5. Disjoint assignment is a real choice, not a first-match race: a row that two
#    lanes both want goes to exactly one of them, deterministically.
class _R(dict):
    def __getitem__(self, k):
        return dict.get(self, k, "")
row = _R(title="Revenue Operations Manager", skills="salesforce, stripe billing",
         company_industry="", startup_tier="", bucket="US remote", basis_weak=0,
         posted="2026-08-10", salary_max="150000", job_key="k", company_norm="c",
         company="Co")
cands = LB.score_for_all(row, dt.date(2026, 8, 11))
fails.append(not ok("a contested row scores for more than one lane",
                    len(cands) > 1, str(sorted(cands))))
picked = [LB.assign([row], dt.date(2026, 8, 11))[0] for _ in range(3)]
keys = [list(p)[0] if p else None for p in picked]
fails.append(not ok("...and lands in the same one every run",
                    len(set(keys)) == 1, str(keys)))

# ---------------------------------- 6d. the South FL metro lane on the MAIN feed
# `sfl` is a PREDICATE lane, not a tag lane, and that is the point being pinned
# here.  A Miami job belongs in it whether or not it also looks like a swath
# role, so it takes no part in lanes_build's disjoint assignment and ranks on
# geo_score instead of score.  Three things have to hold: the two rankings stay
# independent, the lane means on-site/hybrid South FL and nothing else, and a
# main load blanks geo_score the same way it blanks lane_tags.
v3c = write(os.path.join(SP, "v3c.csv"), SPREAD)
con, _ = load(v3c)
lm = LN.lane_map()
print(f"\n[6d] sfl lane: {lane_count('sfl')} rows")

fails.append(not ok("the sfl lane is defined", "sfl" in lm))

# The predicate says what the label promises: South FL, carrying AI or startup
# evidence.  Asserted by reading the rows back, not by re-running the module's
# own SQL, which would only prove the query equals itself.
sfl_rows = con.execute(
    LN.queue_sql(lm["sfl"][1], "bucket, ai_band, startup_tier", order=False),
    lm["sfl"][2]).fetchall()
off = [r for r in sfl_rows if r["bucket"] != "South FL"
       or not ((r["ai_band"] or "") or (r["startup_tier"] or ""))]
fails.append(not ok("the sfl lane holds only South FL AI/startup rows",
                    sfl_rows and not off, f"{len(off)} wrong of {len(sfl_rows)}"))

# A remote job that merely names a South FL city is NOT in this lane —
# ma_remote_build.geo_of runs the remote leg first, so `South FL` means a job
# you have to be there for.  This is the invariant that keeps the lane from
# restating what ai12/startup12 already cover.
leaked = con.execute(
    "SELECT COUNT(*) c FROM jobs WHERE bucket = 'South FL' "
    "AND (remote = 'True' OR work_mode = 'Remote')").fetchone()["c"]
fails.append(not ok("no remote row is bucketed South FL", leaked == 0,
                    f"{leaked} rows"))

# Ranks on geo_score, and does so WITHOUT disturbing score/lane_tags — the whole
# reason it got a column of its own instead of borrowing `score`.
if sfl_rows:
    picked = [r["job_key"] for r in con.execute(
        LN.queue_sql(lm["sfl"][1], "job_key", order=False), lm["sfl"][2])][:2]
    con.executemany("UPDATE jobs SET geo_score=?, geo_why='why' WHERE job_key=?",
                    [(70, picked[0]), (95, picked[1])] if len(picked) > 1 else
                    [(95, picked[0])])
    con.commit()
    top = con.execute(LN.queue_sql(lm["sfl"][1], "job_key, geo_score",
                                   order=LN.order_for("sfl"), limit=2),
                      lm["sfl"][2]).fetchall()
    fails.append(not ok("the sfl lane ranks by geo_score, highest first",
                        top and top[0]["geo_score"] == 95.0,
                        str([r["geo_score"] for r in top])))
    # The two rankings must CO-EXIST on one row, not exclude each other: `sfl`
    # is a predicate lane, so a Miami job that also matches a swath belongs in
    # both, holding a swath `score` and a `geo_score` at the same time and
    # ranking independently in each.  On the live 2026-08-12 corpus that is 35
    # rows — Cognizant's "Senior Full Stack Engineer (AI-Driven)" sits at 54.2
    # in picks and 74.2 here.  This is the case that would have been silently
    # corrupted had the lane borrowed `score` instead of taking a column of its
    # own, so it is asserted rather than assumed.
    con.execute("UPDATE jobs SET score = 42, score_why = 'swath', "
                "lane_tags = ',presales,' WHERE job_key = ?", (picked[0],))
    con.commit()
    both = con.execute(
        "SELECT score, geo_score, lane_tags FROM jobs WHERE job_key = ?",
        (picked[0],)).fetchone()
    fails.append(not ok("a row can hold a swath score and a geo_score at once",
                        both["score"] == 42.0 and both["geo_score"] > 0
                        and both["lane_tags"] == ",presales,",
                        f"score={both['score']} geo={both['geo_score']} "
                        f"tags={both['lane_tags']}"))
    still = con.execute(LN.queue_sql(lm["sfl"][1], "COUNT(*) c", order=False),
                        lm["sfl"][2]).fetchone()["c"]
    fails.append(not ok("...and tagging it into a swath does not remove it "
                        "from sfl", still == len(sfl_rows), f"{still}"))

# lanes.score_cols is what the card reads to know which number ordered the lane.
# Wrong here and the lane renders with no score and no reason — silently.
fails.append(not ok("sfl reports geo_score as its ranking column",
                    LN.score_cols("sfl") == ("geo_score", "geo_why"),
                    str(LN.score_cols("sfl"))))
fails.append(not ok("...and an unlisted lane still reports score",
                    LN.score_cols("ai12") == ("score", "score_why")))

# Same trap as the swaths, one column over: a main load blanks geo_score, so
# A per-metro builder has to run after triage_load.py just as lanes_build.py does.
load(SRC, "main")
con3 = TL.init(DB)
left = con3.execute(
    "SELECT COUNT(*) c FROM jobs WHERE geo_score <> 0").fetchone()["c"]
fails.append(not ok("a main load blanks geo_score (rerun the metro builder)",
                    left == 0, f"{left} rows still scored"))
con3.close()
con.close()

# ------------------------------------------------------------- 6e. cut lanes
# The four `cuts` slice the main corpus on axes the corpus lanes don't have —
# when a row landed, what it pays, what it is called.  They are predicate lanes
# like _DEFS but are emitted AFTER the feed lanes, and most of what can go wrong
# with them is about that placement rather than about their SQL.
v3d = write(os.path.join(SP, "v3d.csv"), SPREAD)
con, _ = load(v3d)
lm = LN.lane_map()
# In lanes() emission order, which is what the last assertion below checks.
# `aistartup` is a cut and not a _DEFS lane for the usual two reasons — see the
# note there — and its count is 0 here because ai_startup_build.py has not run
# against this database yet; section 11 is where that lane is exercised.
# `aiagency` is the same shape and is exercised in section 12.
# RESTORED 2026-08-20 after an overwrite lost the working-tree edits: the
# committed tuple stopped at `aiagency`, but `support`, `local`, `level`,
# and `level` are cut lanes and all group under Cuts.
# Each is a BUILDER's verdict rather than a predicate a reader could check, so
# each counts 0 here until its builder runs against this database.
CUTS = ("fresh", "pay", "maold", "csam", "support", "aistartup", "aiagency",
        "local", "level")
print(f"\n[6e] cut lanes: " + ", ".join(f"{k}={lane_count(k)}" for k in CUTS))

missing = [k for k in CUTS if k not in lm]
fails.append(not ok("every cut lane is defined", not missing, str(missing)))
wrong = [k for k in CUTS if LN.group_of(k) != "cuts"]
fails.append(not ok("every cut lane groups under Cuts", not wrong, str(wrong)))

# THE reason the cuts are emitted last.  Digit keys index lanes() order and only
# the first ten get one (LANE_KEYS in index.html), so a cut lane inserted into
# _DEFS instead would push `rest` from 6 to 0 and knock picks/contract/fastmoney
# off 7/8/9 — the four keys with the most decisions behind them after ai12.
# Nothing else in this file would notice: every lane would still work, still be
# reachable by rail and by [ ], and simply answer a different key than the one
# your hands know.  Hence a literal expectation rather than a derived one.
DIGITS = ["ai12", "ai_all", "ma", "startup12", "sfl", "rest",
          "picks", "contract", "fastmoney", "presales"]
first_ten = [k for k, _, _, _ in LN.lanes()][:10]
fails.append(not ok("the ten digit keys still address the same lanes",
                    first_ten == DIGITS, f"{first_ten}"))
fails.append(not ok("...and the cuts come after every other lane",
                    [k for k, _, _, _ in LN.lanes()][-len(CUTS):] == list(CUTS),
                    str([k for k, _, _, _ in LN.lanes()][-len(CUTS):])))

# `maold` is the COMPLEMENT of `ma`, not a superset: together they must be the
# whole MA-local pool and they must not intersect.  A superset would have been
# the obvious build and would have meant re-walking the 15k rows already being
# burned down on key 3.
ma_pool = con.execute(
    LN.queue_sql("feed = 'main' AND bucket = 'MA local'", "COUNT(*) c",
                 order=False)).fetchone()["c"]
fails.append(not ok("ma + maold is the whole MA-local pool",
                    ma_pool and lane_count("ma") + lane_count("maold") == ma_pool,
                    f"{lane_count('ma')} + {lane_count('maold')} vs {ma_pool}"))
both_ma = con.execute(
    LN.queue_sql(f"({lm['ma'][1]}) AND ({lm['maold'][1]})", "COUNT(*) c",
                 order=False),
    list(lm["ma"][2]) + list(lm["maold"][2])).fetchone()["c"]
fails.append(not ok("...and no row is in both", both_ma == 0, f"{both_ma} rows"))

# Undated MA rows belong to `maold` — `ma` cannot reach them by construction, so
# if this lane dropped them too they would exist only in `rest`.  This is the
# one place a blank `posted` is deliberately swept IN rather than out.
undated = con.execute(
    "UPDATE jobs SET posted = '' WHERE bucket = 'MA local' AND live = 1 "
    "AND job_key IN (SELECT job_key FROM jobs WHERE bucket = 'MA local' "
    "                AND live = 1 LIMIT 3)").rowcount
con.commit()
if undated:
    # Counted against every undated MA row in the queue, not against the three
    # just blanked: the fixture already carried some, and "the ones I made" is
    # the wrong denominator for a claim about reachability.
    all_undated = con.execute(
        LN.queue_sql("feed = 'main' AND bucket = 'MA local' AND posted = ''",
                     "COUNT(*) c", order=False)).fetchone()["c"]
    caught = con.execute(
        LN.queue_sql(f"({lm['maold'][1]}) AND posted = ''", "COUNT(*) c",
                     order=False), lm["maold"][2]).fetchone()["c"]
    fails.append(not ok("every undated MA row lands in maold, not nowhere",
                        caught == all_undated and caught >= undated,
                        f"{caught}/{all_undated}"))

# `fresh` is the opposite call on the same column, and for the opposite reason:
# blank means the source never publishes a date, and a lane that promises "the
# last 72 hours" cannot be made of rows whose date is unknown.
blank_fresh = con.execute(
    LN.queue_sql(f"({lm['fresh'][1]}) AND posted = ''", "COUNT(*) c",
                 order=False), lm["fresh"][2]).fetchone()["c"]
fails.append(not ok("no undated row reaches the fresh lane", blank_fresh == 0,
                    f"{blank_fresh} rows"))
stale = con.execute(
    LN.queue_sql(lm["fresh"][1], "MIN(posted) p", order=False),
    lm["fresh"][2]).fetchone()["p"]
fails.append(not ok("fresh holds nothing older than FRESH_DAYS",
                    not stale or stale >= LN.recent_cutoff(LN.FRESH_DAYS),
                    f"oldest {stale}"))

# The pay lane's bounds, read back off the rows rather than by re-running its own
# SQL.  The ceiling is the interesting half: `pay` sorts salary DESC, so a single
# mis-parsed row is not one bad card somewhere in 13k, it is the FIRST card.
con.execute(
    "UPDATE jobs SET salary_currency='USD', salary_period='Year', "
    "salary_min='475000000', salary_max='' WHERE job_key = "
    "(SELECT job_key FROM jobs WHERE live=1 AND feed='main' LIMIT 1)")
con.commit()
pay_rows = con.execute(
    LN.queue_sql(lm["pay"][1], "salary_min, salary_currency, salary_period",
                 order=False), lm["pay"][2]).fetchall()
outside = [r["salary_min"] for r in pay_rows
           if not (LN.PAY_FLOOR <= float(r["salary_min"]) < LN.PAY_CEIL)
           or r["salary_currency"] != "USD"
           or r["salary_period"].lower() not in ("year", "annual")]
fails.append(not ok("every pay row is USD, annual, and inside the bounds",
                    not outside, f"{outside[:3]} of {len(pay_rows)}"))
fails.append(not ok("a $475,000,000 parse cannot reach the top of the lane",
                    "475000000" not in [r["salary_min"] for r in pay_rows]))
top_pay = con.execute(
    LN.queue_sql(lm["pay"][1], "salary_min", order=LN.order_for("pay"), limit=1),
    lm["pay"][2]).fetchone()
if pay_rows:
    fails.append(not ok("the pay lane ranks by salary, highest first",
                        top_pay and float(top_pay["salary_min"])
                        == max(float(r["salary_min"]) for r in pay_rows),
                        str(top_pay["salary_min"] if top_pay else None)))

# `csam` is the only title lane here; assert it against the titles it returned.
CSAM = ("customer success", "account manager", "account executive",
        "client partner")
csam_rows = [r["title"] for r in con.execute(
    LN.queue_sql(lm["csam"][1], "title", order=False), lm["csam"][2])]
off = [t for t in csam_rows if not any(p in (t or "").lower() for p in CSAM)]
fails.append(not ok("the csam lane holds only its four title patterns",
                    not off, f"{off[:3]} of {len(csam_rows)}"))

# It has no resume — there is no F12 draft — and that has to be survivable
# rather than a crash, because resume_for() feeds the card directly.
fails.append(not ok("a lane with no resume returns None instead of raising",
                    LN.resume_for("csam") is None))

# The cuts overlap the corpus lanes and `rest` on purpose (one job_key, one
# verdict), so the thing to assert is not that they are disjoint but that a
# verdict reached in a cut lane clears the row everywhere else.
seed = con.execute(
    LN.queue_sql(lm["csam"][1], "job_key, ct_key", order=False),
    lm["csam"][2]).fetchone()
if seed:
    con.execute("INSERT INTO decisions (job_key, ct_key, verdict, reason, lane, "
                "decided_at) VALUES (?, ?, 'deny', 'manual', 'csam', "
                "'2026-08-14')", (seed["job_key"], seed["ct_key"]))
    con.commit()
    left = sum(con.execute(
        LN.queue_sql(pred + " AND jobs.job_key = ?", "COUNT(*) c", order=False),
        list(params) + [seed["job_key"]]).fetchone()["c"]
        for _, _, pred, params in LN.lanes())
    fails.append(not ok("deciding a row in a cut lane clears it from all lanes",
                        left == 0, f"still in {left} lanes"))
con.close()

# ------------------------------------------------------ 7. apply-page filters
# The filter bar is server-side so one definition of the queue serves the page,
# the tab badges and triage_export.py.  Four things have to hold: a facet
# returns exactly the rows it claims, the three compose, the badges count the
# filtered set, and the number on a chip is what clicking it actually gives.
v4 = write(os.path.join(SP, "v4.csv"), SPREAD)
con, _ = load(v4)                       # retires the head sample, keeps schema
# Cleared after the load, not before: load() carries decisions forward by
# ct_key, so anything deleted first would come straight back attached to the
# new job_keys.
con.execute("DELETE FROM decisions")
con.execute("DELETE FROM applications")
con.execute("""
    INSERT INTO decisions (job_key, ct_key, verdict, reason, lane, decided_at)
    SELECT job_key, ct_key, 'approve', 'manual', 'ai12', '2026-08-08'
      FROM jobs WHERE live = 1 AND feed = 'main'
""")
con.commit()
q_all = AQ.queue(con)
print(f"\n[7] apply filters: {len(q_all)} approved rows")

# Expectations read the row fields rather than re-running the module's own SQL,
# which would only prove the query equals itself.  `band` is derived here from
# ai_band's leading digit while the query filters on rank_tier, so this also
# pins the claim in FACETS that those two spellings cannot drift apart.
IS = {
    ("src", "direct"): lambda r: not r["is_agg"],
    ("src", "agg"):    lambda r: bool(r["is_agg"]),
    ("band", "12"):    lambda r: (r["ai_band"] or "")[:1] in ("1", "2"),
    ("band", "1"):     lambda r: (r["ai_band"] or "")[:1] == "1",
    ("loc", "ma"):     lambda r: r["bucket"] in ("MA local", "Nearby (RI/NH/CT)"),
    ("loc", "remote"): lambda r: (r["bucket"] or "").startswith("Remote"),
    ("loc", "sfl"):    lambda r: r["bucket"] == "South FL",
}

def want(rows, **sel):
    for name, value in sel.items():
        if value:
            rows = [r for r in rows if IS[(name, value)](r)]
    return rows

keys = lambda rows: {r["job_key"] for r in rows}                  # noqa: E731

for name, opts in AQ.FACETS.items():
    for opt in opts:
        got, exp = AQ.queue(con, sel={name: opt}), want(q_all, **{name: opt})
        fails.append(not ok(f"filter {name}={opt} returns exactly its rows",
                            exp and keys(got) == keys(exp),
                            f"{len(got)} rows, {len(exp)} expected"))

COMBO = {"src": "direct", "band": "1", "loc": "remote"}
got, exp = AQ.queue(con, sel=COMBO), want(q_all, **COMBO)
# Narrower than any one of its parts, or the AND is not being applied and the
# checks below would all pass on an unfiltered set.
narrows = all(len(exp) < len(want(q_all, **{k: v})) for k, v in COMBO.items())
fails.append(not ok("the three filters compose",
                    exp and keys(got) == keys(exp) and narrows,
                    f"{len(got)} rows, {len(exp)} expected"))

c = AQ.counts(con, None, COMBO)
fails.append(not ok("tab badges count the filtered set, not the whole queue",
                    c["total"] == len(got) < AQ.counts(con)["total"]
                    and c["total"] == sum(c[s] for s in AQ.STATUSES), f"{c}"))

for r in got[:3]:
    AQ.set_status(con, r["job_key"], "applied")
c = AQ.counts(con, None, COMBO)
fails.append(not ok("a status and the filters narrow together",
                    c["applied"] == 3 and c["to_apply"] == len(got) - 3
                    and len(AQ.queue(con, "applied", sel=COMBO)) == 3, f"{c}"))

# The property the bar rests on: a chip's number is the size of the list you
# get by clicking it — this filter with that one facet lifted, never the set
# already on screen.  Counted naively the unselected chips of an active facet
# would all read 0 and the bar would be a corner with no way out but clear.
fc = AQ.facet_counts(con, "to_apply", None, COMBO)
off = [(n, o, v, len(AQ.queue(con, "to_apply", sel=dict(COMBO, **{n: o}))))
       for n, opts in fc.items() for o, v in opts.items()
       if v != len(AQ.queue(con, "to_apply", sel=dict(COMBO, **{n: o})))]
fails.append(not ok("every chip counts what clicking it would give", not off,
                    f"{off[:3]}"))
fails.append(not ok("lifting an active facet really widens its own chips",
                    fc["src"][""] > fc["src"]["direct"] > 0,
                    f"any={fc['src']['']} direct={fc['src']['direct']}"))
fails.append(not ok("one facet's chips sum to that facet unset",
                    fc["src"][""] == fc["src"]["direct"] + fc["src"]["agg"],
                    f"{fc['src']}"))

for sel, why in (({"band": "nope"}, "value"), ({"nope": "x"}, "name")):
    try:
        AQ.queue(con, sel=sel)
        raised = False
    except ValueError:
        raised = True
    fails.append(not ok(f"a bad filter {why} raises rather than filtering nothing",
                        raised))
try:
    # Validated even for the facet it is about to skip: a typo that only showed
    # up on some other request would be a filter that silently does nothing.
    AQ.facet_counts(con, None, None, {"band": "nope"})
    raised = False
except ValueError:
    raised = True
fails.append(not ok("a bad value is caught even in the facet being counted",
                    raised))

# The join has to run small-to-large.  Without idx_dec_verdict the planner
# starts at `jobs` (183k live rows) and probes decisions for each one, which
# put the filtered read at ~180ms — eleven of those per keystroke is a page
# that stutters, and nothing else here would notice.
plan = " ".join(str(r[3]) for r in con.execute(
    "EXPLAIN QUERY PLAN SELECT COUNT(*) " + AQ.FROM + " WHERE " +
    AQ._where("to_apply", None, COMBO)[0], ["to_apply"]))
fails.append(not ok("the apply query drives from decisions, not from jobs",
                    "idx_dec_verdict" in plan, plan[:90]))

# ---------------------------------------------------------------- [8] basis
# ma_remote_build admits remote rows whose US basis it could only INFER, and
# they must sort BEHIND proven ones rather than mixing in.  Two failure modes
# neither of the other sections would catch: basis_weak silently computing to 0
# for every row (the label changed, or us_basis stopped being read), and
# basis_weak quietly dropping out of ORDER_BY, which looks identical in every
# count and only shows up as worse rows at the top of a lane.
# The location allowlist, tested directly.  The first version of this rule was
# a blocklist of foreign COUNTRY names, which passed every row whose location
# named a foreign city or region instead ("Gurugram", "APAC", "3 Locations")
# and put ~4k offshore rows in the cut.  These cases are that bug.
import ma_remote_build as MRB                                     # noqa: E402
for loc, want in (("", True), ("Remote", True), ("remote", True),
                  ("Work From Home", True), ("Remote (US)", True),
                  ("US - Remote", True), ("Remote, United States", True),
                  ("Gurugram", False), ("Nairobi", False), ("APAC", False),
                  ("EMEA", False), ("UK", False), ("3 Locations", False),
                  ("Remote - Canada", False), ("Ajmer, ", False),
                  ("Albania; Andorra; Austria", False),
                  ("Remote / Online", True), ("Toronto, ON", False)):
    got = MRB.us_plausible_location(loc)
    fails.append(not ok(f"location {loc!r:28} -> {'admit' if want else 'reject'}",
                        got == want, "" if got == want else f"got {got}"))

UB, PO, SO = HDR.index("us_basis"), HDR.index("posted"), HDR.index("source")
def basis_row(key, basis):
    r = list(ROWS[0])
    r[KI], r[UB], r[PO] = key, basis, "2026-08-01"
    r[CI], r[TI] = f"co-{key}", f"title-{key}"
    r[SO], r[AI] = "greenhouse", "1 Claude / agentic / FDE"
    return r

BP = write(os.path.join(SP, "basis.csv"),
           [basis_row("proven-1", "state"),
            basis_row("inferred-1", TL.WEAK_BASIS + " (company/USD)")])
con2, _ = load(BP, feed="basis")
weak = dict(con2.execute(
    "SELECT job_key, basis_weak FROM jobs WHERE feed='basis'").fetchall())
fails.append(not ok("an inferred us_basis sets basis_weak=1",
                    weak.get("inferred-1") == 1, f"{weak.get('inferred-1')}"))
fails.append(not ok("a proven us_basis leaves basis_weak=0",
                    weak.get("proven-1") == 0, f"{weak.get('proven-1')}"))

fails.append(not ok("basis_weak is actually in the lane ORDER BY",
                    "basis_weak" in LN.ORDER_BY, LN.ORDER_BY))
order = [r[0] for r in con2.execute(
    LN.queue_sql("feed = 'basis'", "job_key", order=True))]
fails.append(not ok("proven-US rows sort ahead of inferred ones",
                    order == ["proven-1", "inferred-1"], str(order)))
con2.close()

# ------------------------------------------------------------------ 9. sort
# The Sort control (lanes.posted_terms) on both pages.  It is a PREFIX on each
# lane's ORDER BY, never a replacement, so the two things to hold are that
# 'fit' is bit-for-bit what it always was and that the lane's own ranking still
# decides ties underneath the date.
#
# The blank-posted case is the one that actually bites.  ~2.7k live rows have
# posted='' because their source never publishes a date (Phenom, WeWorkRemotely,
# Braintrust), and '' sorts below every real date — harmless under DESC, but
# under ASC it puts every undated row at the top of "Oldest", which reads as a
# pile of ancient postings and is the opposite of what the sort was asked for.
print("\n[9] sort by posted date")
fails.append(not ok("'fit' is the unchanged lane ordering",
                    LN.order_for("rest") == LN.ORDER_BY
                    and LN.order_for("contract") == LN.CONTRACT_ORDER_BY,
                    LN.order_for("rest")))
fails.append(not ok("'fit' is the unchanged apply ordering",
                    AQ.order_by() == AQ.ORDER_BY and AQ.order_by("fit") == AQ.ORDER_BY,
                    AQ.order_by()))
for bad in ("bogus", "posted", "asc"):
    raised = False
    try:
        LN.posted_terms(bad)
    except ValueError:
        raised = True
    fails.append(not ok(f"sort {bad!r} is rejected, not ignored", raised))

def sort_row(key, posted, band="9"):
    r = list(ROWS[0])
    r[KI], r[PO] = key, posted
    r[CI], r[TI] = f"co-{key}", f"title-{key}"
    r[SO], r[UB] = "greenhouse", "state"
    r[AI] = f"{band} band" if band != "9" else ""
    return r

SP_ROWS = [sort_row("s-old", "2026-01-05"), sort_row("s-mid", "2026-06-01"),
           sort_row("s-new", "2026-08-01"), sort_row("s-blank", ""),
           # Same date, different AI strength: the tiebreak has to be the
           # lane's own ranking, which is the whole point of prefixing.
           sort_row("s-tie-weak", "2026-07-01", "3"),
           sort_row("s-tie-strong", "2026-07-01", "1")]
con3, _ = load(write(os.path.join(SP, "sort.csv"), SP_ROWS), feed="sortf")
def lane_order(sort):
    return [r[0] for r in con3.execute(
        LN.queue_sql("feed = 'sortf'", "job_key",
                     order=LN.order_for("rest", sort)))]

newest = lane_order("new")
oldest = lane_order("old")
fails.append(not ok("'new' puts the freshest posting first",
                    newest[0] == "s-new", str(newest)))
fails.append(not ok("'old' puts the oldest DATED posting first",
                    oldest[0] == "s-old", str(oldest)))
# Both directions, because the guard exists so the invariant does not depend on
# which way round the date term happens to be.
fails.append(not ok("a blank posted date sorts last under 'new'",
                    newest[-1] == "s-blank", str(newest)))
fails.append(not ok("a blank posted date sorts last under 'old', not first",
                    oldest[-1] == "s-blank", str(oldest)))
fails.append(not ok("the lane's own ranking still breaks a date tie",
                    newest.index("s-tie-strong") < newest.index("s-tie-weak")
                    and oldest.index("s-tie-strong") < oldest.index("s-tie-weak"),
                    str(newest)))
fails.append(not ok("'fit' ignores the date and ranks as it always did",
                    lane_order(None)[0] == "s-tie-strong", str(lane_order(None))))
con3.close()

# Section 9 marks its own rows rather than reading whatever the earlier ones
# left behind: section 7 reloads the corpus, which retires section 3's in-flight
# job, and a test that asserts on leftovers reports on the wrong thing.
# Five approved rows, each with a different posted date.
picks = [r[0] for r in con.execute(
    f"SELECT MIN(j.job_key) {AQ.FROM} WHERE {AQ.APPROVED} AND j.posted <> '' "
    "GROUP BY j.posted ORDER BY j.posted DESC LIMIT 5")]
AQ.set_status(con, picks[0], "applying")
# Marked applied in an order that is neither date-ascending nor descending, so
# 'worked most recently first' cannot pass for a date sort by coincidence.
for k in (picks[2], picks[4], picks[1], picks[3]):
    AQ.set_status(con, k, "applied")
# ...and stamped apart by hand, because set_status writes datetime('now') at
# one-second resolution: four calls in a row tie, a tie makes a.updated_at DESC
# decide nothing, and the ordering test below would then pass whether or not
# that term was in the clause — which is the one thing it exists to catch.
for n, k in enumerate((picks[2], picks[4], picks[1], picks[3])):
    con.execute("UPDATE applications SET updated_at = ? WHERE job_key = ?",
                (f"2026-08-0{n + 1} 12:00:00", k))
con.commit()

# The apply page adds one rule the card queue has not got: the sort reorders
# each pile, it does not shuffle the piles themselves.  In-flight work stays
# pinned to the top of the All tab whichever way the dates run.
for s in ("new", "old"):
    q = AQ.queue(con, sort=s)
    fails.append(not ok(f"in-flight still tops the All tab under {s!r}",
                        bool(q) and q[0]["status"] == "applying",
                        q[0]["status"] if q else "empty"))
    dates = [j["posted"] for j in q if j["status"] == "to_apply" and j["posted"]]
    want = sorted(dates, reverse=(s == "new"))
    fails.append(corpus_only(f"unworked rows run in {s!r} posted order") if FIXTURE
                 else not ok(f"unworked rows run in {s!r} posted order",
                        dates == want, f"{dates[:4]} vs {want[:4]}"))

# The regression this pair guards: a.updated_at DESC left in the clause fixes
# the order of every worked row, so Newest/Oldest would keep working in
# 'To apply' and silently do nothing here.  Asserted in both directions —
# under that bug the tail's own posted DESC could still pass for 'new'.
# The four above plus whatever the earlier sections left applied — the count is
# not the point, the ordering is, and >= 4 is enough that sortedness is not
# something a one-row list could pass by default.
for s, rev in (("new", True), ("old", False)):
    got = [j["posted"] for j in AQ.queue(con, "applied", sort=s)]
    fails.append(corpus_only(f"the Applied tab reorders under {s!r} too") if FIXTURE
                 else not ok(f"the Applied tab reorders under {s!r} too",
                        len(got) >= 4 and got == sorted(got, reverse=rev),
                        f"{len(got)} rows: {got}"))

con.close()

# ---------------------------------------------------------------------------
print("\n[10] AI band: the non-adjacent AI-modifier recovery")
# CORE only ever fired when the AI token sat immediately before a whitelisted
# role noun, so "AI Security Engineer" / "Senior Software Engineer, AI" carried
# no band at all and the AI 1-2 lane could not see them.  1,342 such rows were
# sitting undecided in `rest` on 2026-08-12.  Both directions are asserted: the
# recovery must land in band 2 (band 3 keeps it out of a rank_tier <= 2 lane),
# and it must not disturb a row that CORE or ADJ already claimed.
from ai_ma_remote import band_of, tier_of                       # noqa: E402

def _band(title):
    hit = tier_of({"title": title, "skills": ""})
    return band_of(title, hit[0])[0] if hit else None

for title, want in (
        # recovered: qualifier between the token and the noun, or token trailing
        ("AI Deployment Engineer, Enterprise", 2),
        ("AI Field Engineer (Strategic Partnerships)", 2),
        ("AI Solutions Architect", 2),
        ("Senior Software Engineer, AI", 2),
        ("Software Engineer 5 - Agent Platform, AI Platform", 2),
        # untouched: band 1 vocabulary still outranks the recovery
        ("Forward Deployed Engineer", 1),
        ("Agentic AI Engineer", 1),
        ("AI Engineer", 2),
        # the FDE spelling variants: only "forward-deployed" used to match, so
        # these carried no band at all despite being the most on-target title
        # in the corpus.  Band 1, not 2 — "Forward Deployment Engineer, AI"
        # would otherwise rank below a generic AI engineer.
        ("Forward Deployment Engineer", 1),
        ("Forward Deploy Engineer", 1),
        ("Software Engineer II, Forward Deployment", 1),
        ("Forward Deployment Engineer, AI - US Remote", 1),
        ("Founding FDE", 1),
        ("Engineering Manager, FDE Infrastructure (NORAM)", 1),
        # untouched: ADJ keeps its rows OUT of the top lane.  Ungated, the
        # recovery promoted 602 of these from band 4 into band 2, which is the
        # one thing the AI-ML-adjacent band exists to stop.
        ("Machine Learning Engineer", 4),
        ("AI Scientist", 4),
        # data-labelling gigs advertised to engineers stay unbanded
        ("Python Engineer - Freelance AI Trainer", None),
        ("Registered Nurse", None),
):
    fails.append(not ok(f"{title[:46]!r} -> band {want}", _band(title) == want,
                        f"got {_band(title)}"))

# ---------------------------------------------------------------------------
print("\n[11] AI/FDE US-remote startup lane: the company-tier propagation")
# The lane exists because `startup_tier` is written only for the ATS families
# startup_build.py classifies, so the SQL-only version of it —
# `bucket='Remote' AND rank_tier<=2 AND startup_tier<>''` — returns nothing from
# himalayas, jobgether, torre, remoteio or wttj, which between them carry 1,375
# of the live US-remote AI rows.  ai_startup_build.py resolves the tier through
# `company_norm` instead.  The propagation IS the lane: 206 of its 339 rows on
# the 2026-08-15 corpus arrive that way, so a regression that quietly dropped it
# would leave a lane that still looked populated and had lost two thirds of its
# reach, most of it from the biggest source in the corpus.
#
# Fixtures rather than corpus rows on purpose.  The scored population is ~0.1%
# of the file, so a sample of the head can legitimately contain none of it, and
# a test built from it would pass by finding nothing to check.
import subprocess                                                   # noqa: E402

# Section 8 closed the connection it was handed; TL.init reopens and also runs
# migrate(), which is what adds startup_score/startup_why to a database written
# before this lane existed — so this doubles as the check that an older
# triage.db is upgraded rather than 500ing on the first read.
con = TL.init(DB)
TODAY = dt.date.today().isoformat()
FX = [
    # (job_key, company_norm, company, tier, band, rank_tier, feed, live,
    #  source, is_agg)
    ("zzfx-direct", "zzdirectco", "ZZ Direct Co", "1 Funded",
     "1 Claude / agentic / FDE", 1, "main", 1, "greenhouse", 0),
    # blank tier, aggregator source — admitted only via its company
    ("zzfx-inherit", "zzinheritco", "ZZ Inherit Co", "",
     "1 Claude / agentic / FDE", 1, "main", 1, "himalayas", 1),
    # the row that lends the tier: retired, and not itself in the lane
    ("zzfx-donor", "zzinheritco", "ZZ Inherit Co", "1 Funded", "", 9,
     "main", 0, "greenhouse", 0),
    # AI band 1 and no startup evidence anywhere: must stay out
    ("zzfx-notier", "zznotierco", "ZZ No Tier Co", "",
     "1 Claude / agentic / FDE", 1, "main", 1, "himalayas", 1),
    # a contract gig that would qualify on every other axis
    ("zzfx-contract", "zzcontractco", "ZZ Contract Co", "1 Funded",
     "1 Claude / agentic / FDE", 1, "contract", 1, "greenhouse", 0),
]
con.executemany(
    "INSERT OR REPLACE INTO jobs (job_key, ct_key, company_norm, company, "
    "startup_tier, ai_band, rank_tier, feed, live, source, is_agg, bucket, "
    "title, skills, company_industry, posted, salary_max, basis_weak, "
    "first_seen, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,'Remote',"
    "'Forward Deployed Engineer','','',?,'',0,'x','x')",
    [(k, f"ct-{k}", cn, co, t, b, rt, fd, lv, src, agg, TODAY)
     for k, cn, co, t, b, rt, fd, lv, src, agg in FX])
con.commit()

r = subprocess.run([sys.executable, os.path.join(ROOT, "ai_startup_build.py"),
                    "--db", DB], capture_output=True, text=True)
fails.append(not ok("builder ran", r.returncode == 0, r.stderr[-300:]))

scores = {k: (s, w) for k, s, w in con.execute(
    "SELECT job_key, startup_score, startup_why FROM jobs "
    "WHERE job_key LIKE 'zzfx-%'")}

fails.append(not ok("an aggregator row with no tier of its own is admitted on "
                    "its company's", scores["zzfx-inherit"][0] > 0,
                    f"score={scores['zzfx-inherit'][0]}"))
fails.append(not ok("...and says so on the card",
                    "via company" in scores["zzfx-inherit"][1],
                    scores["zzfx-inherit"][1]))
fails.append(not ok("the same tier read straight off the posting outranks it",
                    scores["zzfx-direct"][0] > scores["zzfx-inherit"][0],
                    f"{scores['zzfx-direct'][0]} > {scores['zzfx-inherit'][0]}"))
# The lane is not "every US-remote AI row" wearing a startup label.
fails.append(not ok("an AI row with no startup evidence anywhere stays out",
                    scores["zzfx-notier"][0] == 0,
                    f"score={scores['zzfx-notier'][0]}"))
fails.append(not ok("the tier donor is not itself in the lane",
                    scores["zzfx-donor"][0] == 0))
fails.append(not ok("a contract row never reaches this main-feed lane",
                    scores["zzfx-contract"][0] == 0))

lm = LN.lane_map()
fails.append(not ok("the lane is registered", "aistartup" in lm))
lane_rows = con.execute(LN.queue_sql(lm["aistartup"][1], "job_key, "
                                     "startup_score, startup_why",
                                     order=LN.order_for("aistartup")),
                        lm["aistartup"][2]).fetchall()
keys = [x[0] for x in lane_rows]
fails.append(not ok("the lane holds the admitted rows and only those",
                    "zzfx-direct" in keys and "zzfx-inherit" in keys
                    and not {"zzfx-notier", "zzfx-donor", "zzfx-contract"}
                    & set(keys)))
# The predicate is `startup_score > 0`, so these two are the same statement --
# a row scored 0 would be in the lane carrying no number and no reason.
fails.append(not ok("every row in the lane carries a score and a reason",
                    all(s > 0 and w for _, s, w in lane_rows),
                    f"{sum(1 for _, s, w in lane_rows if not (s > 0 and w))} bad"))
fails.append(not ok("direct sorts ahead of inherited",
                    keys.index("zzfx-direct") < keys.index("zzfx-inherit")))
fails.append(not ok("the card reads the column that ranked the lane",
                    LN.score_cols("aistartup") == ("startup_score",
                                                   "startup_why")))

# ---------------------------------------------------------------------------
print("\n[12] AI reqs at staffing desks: the ATS-domain proof")
# This lane's decisive signal is the apply_url's REGISTRABLE DOMAIN being a
# staffing-only ATS, which reaches 387 main-feed rows whose company name reads
# as an ordinary employer (NTT DATA, ICONMA, Metasys) and which no name test can
# find.  Three things below are the ones that actually broke while it was built,
# so they are asserted rather than assumed.
import ai_agency_build as AG                                        # noqa: E402

JD = "https://www1.jobdiva.com/portal/?compid=0&id=1"
AGFX = [
    # (job_key, company, apply_url, title, band, rank_tier, feed, industry)
    # Proof by construction: the name says nothing, the ATS says everything.
    # Title, band, geography, date and desk depth are held IDENTICAL to
    # zzag-name below so the two scores differ by the evidence term and nothing
    # else — the filler rows exist only to equalise desk depth, which is
    # computed across the whole table and would otherwise dominate the gap.
    ("zzag-ats", "Anon Technologies Inc", JD, "AI Engineer",
     "2 AI engineer / LLM / GenAI", 2, "main", ""),
    ("zzag-ats-sib", "Anon Technologies Inc", JD, "GenAI Developer",
     "2 AI engineer / LLM / GenAI", 2, "main", ""),
    ("zzag-ats-pad", "Anon Technologies Inc", JD, "Warehouse Associate",
     "", 9, "main", ""),
    # The weak arm: a name guess, admitted and labelled.
    ("zzag-name", "Acme Staffing Group", "https://boards.greenhouse.io/x/1",
     "AI Engineer", "2 AI engineer / LLM / GenAI", 2, "main", ""),
    # THE false positive that shipped in the first draft.  Bullhorn Inc hiring
    # for ITSELF on Workday — the tenant label is 'bullhorn', the ATS is
    # myworkdayjobs.com — which a `LIKE '%bullhorn%'` substring test admits as an
    # agency-posted req.  Twelve such rows exist in the real corpus.
    ("zzag-tenant", "Bullhorn",
     "https://bullhorn.wd1.myworkdayjobs.com/BullhornCareers/job/1",
     "AI Engineer", "2 AI engineer / LLM / GenAI", 2, "main", ""),
    # An agency req with no AI in it at all.  The lane is named AI reqs, and the
    # first draft of AI_ADJACENT borrowed vocab's T2 families — so
    # "Windchill Solution Architect" was admitted and labelled "AI-adjacent
    # title", putting roughly half the lane under a false claim.
    ("zzag-noai", "Acme Staffing Group", "https://boards.greenhouse.io/x/2",
     "Windchill Solution Architect", "", 9, "main", ""),
    # An ordinary employer whose PDL industry string happens to carry one HR
    # label among several.  One label is not evidence of what a company does.
    ("zzag-hrind", "Blackbaud Philanthropy", "https://careers.example.com/1",
     "Principal AI Solutions Architect", "2 AI engineer / LLM / GenAI", 2,
     "main", "Business/Productivity Software, Human Capital Services, Internet"),
    # A contract-feed row that qualifies on every other axis.
    ("zzag-contract", "Acme Staffing Group", JD, "AI Engineer",
     "1 Claude / agentic / FDE", 1, "contract", ""),
]
con.executemany(
    "INSERT OR REPLACE INTO jobs (job_key, ct_key, company_norm, company, "
    "apply_url, job_url, title, ai_band, rank_tier, feed, company_industry, "
    "live, source, is_agg, bucket, startup_tier, skills, posted, salary_max, "
    "basis_weak, first_seen, last_seen) "
    "VALUES (?,?,?,?,?,'',?,?,?,?,?,1,'jobgether',1,'Remote','','',?,'',0,"
    "'x','x')",
    [(k, f"ct-{k}", TL.norm_co(co), co, url, ti, b, rt, fd, ind, TODAY)
     for k, co, url, ti, b, rt, fd, ind in AGFX])
con.commit()

# The detector, unit-tested away from the corpus: an apex-domain rule is the
# whole reason the tenant case above is rejected.
fails.append(not ok("a staffing ATS apex domain is proof",
                    AG.agency_of(JD, "", "Anon Technologies Inc", "")[0]
                    == AG.HARD_EVIDENCE))
fails.append(not ok("...and an ATS name in the TENANT label is not",
                    AG.agency_of("https://bullhorn.wd1.myworkdayjobs.com/j/1",
                                 "", "Bullhorn", "")[0] == 0))
fails.append(not ok("a name guess scores below ATS proof",
                    0 < AG.agency_of("", "", "Acme Staffing Group", "")[0]
                    < AG.HARD_EVIDENCE))
fails.append(not ok("a generic engineering title is not AI-adjacent",
                    AG.ai_fit("", "Windchill Solution Architect")[0] == 0))
fails.append(not ok("...but a real AI title is",
                    AG.ai_fit("", "GenAI Platform Architect")[0] > 0))
# Desk depth is the term that makes this a lane rather than a filter.
fails.append(not ok("a deep desk outranks a one-off desk",
                    AG.desk_fit(12, 300)[0] > AG.desk_fit(2, 8)[0] > 0))
fails.append(not ok("a desk with a single AI req scores no depth",
                    AG.desk_fit(1, 50)[0] == 0))

r = subprocess.run([sys.executable, os.path.join(ROOT, "ai_agency_build.py"),
                    "--db", DB], capture_output=True, text=True)
fails.append(not ok("builder ran", r.returncode == 0, r.stderr[-300:]))

dsk = {k: (s, w) for k, s, w in con.execute(
    "SELECT job_key, desk_score, desk_why FROM jobs "
    "WHERE job_key LIKE 'zzag-%'")}
fails.append(not ok("an AI req on a staffing ATS is admitted on the URL alone",
                    dsk["zzag-ats"][0] > 0, f"score={dsk['zzag-ats'][0]}"))
fails.append(not ok("...and the card names the ATS that proved it",
                    "staffing ATS" in dsk["zzag-ats"][1], dsk["zzag-ats"][1]))
fails.append(not ok("an agency-named row is admitted and labelled a guess",
                    dsk["zzag-name"][0] > 0
                    and "agency" in dsk["zzag-name"][1],
                    dsk["zzag-name"][1]))
fails.append(not ok("ATS proof outranks a name guess, all else equal",
                    dsk["zzag-ats"][0] - dsk["zzag-name"][0]
                    == AG.HARD_EVIDENCE - AG.NAME_EVIDENCE,
                    f"{dsk['zzag-ats'][0]} vs {dsk['zzag-name'][0]}"))
fails.append(not ok("an ATS vendor hiring for itself stays out",
                    dsk["zzag-tenant"][0] == 0,
                    f"score={dsk['zzag-tenant'][0]}"))
fails.append(not ok("an agency req with no AI in it stays out",
                    dsk["zzag-noai"][0] == 0, f"score={dsk['zzag-noai'][0]}"))
fails.append(not ok("one HR label in an industry string is not an agency",
                    dsk["zzag-hrind"][0] == 0, f"score={dsk['zzag-hrind'][0]}"))
fails.append(not ok("a contract row never reaches this main-feed lane",
                    dsk["zzag-contract"][0] == 0))

fails.append(not ok("the lane is registered", "aiagency" in lm))
ag_rows = con.execute(LN.queue_sql(lm["aiagency"][1],
                                   "job_key, desk_score, desk_why",
                                   order=LN.order_for("aiagency")),
                      lm["aiagency"][2]).fetchall()
ag_keys = [x[0] for x in ag_rows]
fails.append(not ok("the lane holds the admitted rows and only those",
                    {"zzag-ats", "zzag-name"} <= set(ag_keys)
                    and not {"zzag-tenant", "zzag-noai", "zzag-hrind",
                             "zzag-contract"} & set(ag_keys)))
fails.append(not ok("every row in the lane carries a score and a reason",
                    all(s > 0 and w for _, s, w in ag_rows),
                    f"{sum(1 for _, s, w in ag_rows if not (s > 0 and w))} bad"))
fails.append(not ok("the card reads the column that ranked the lane",
                    LN.score_cols("aiagency") == ("desk_score", "desk_why")))
# The term this lane deliberately does NOT sort on.  Every other lane leads with
# `is_agg ASC`; 65 of the 86 real rows here are is_agg=1, so inheriting that term
# would bury the lane in exactly the way it was buried before it existed.
fails.append(not ok("the lane does not sort aggregators to the back",
                    "is_agg" not in LN.order_for("aiagency"),
                    LN.order_for("aiagency")))
# The one that costs a silent empty lane: the loader upserts startup_score from
# a CSV that has no such column, exactly as it does geo_score and lane_tags, so
# a main load blanks this lane and triage_chain.sh has to rebuild it.  Asserted
# rather than trusted, because the symptom is a lane that is merely EMPTY --
# indistinguishable from having burned it down to zero.
#
# Counted through the LANE and not through `startup_score > 0` alone: a row the
# new CSV does not mention is never upserted, so it keeps the score it had and
# leaves the queue by going live=0 instead.  The column is the wrong thing to
# assert on; what has to be empty is what the menu shows.
con.close()
con, _ = load(v1)
for lane, builder in (("aistartup", "ai_startup_build.py"),
                      ("aiagency", "ai_agency_build.py"),
                      ("local", "geo_lane.py"),
                      ("level", "level.py")):
    after = con.execute(LN.queue_sql(lm[lane][1], "COUNT(*)", order=False),
                        lm[lane][2]).fetchone()[0]
    fails.append(not ok(f"a main load blanks `{lane}` "
                        f"(chain must rerun {builder})",
                        after == 0, f"{after} rows survived"))

# The server's card API, imported here rather than at the top: it pulls in the
# whole request handler and the modules only these last sections need.
from triage import server as SRV                                  # noqa: E402

# Offset paging.  Reading a lane in the grid decides nothing, so nothing leaves
# UNDECIDED and re-running the same top-N query returns the same rows for ever
# -- the old client read that as an exhausted lane and said so about 13k rows.
# The offset has to partition: no row twice, no row skipped.
# Pages of 5, not 20: this fixture holds a few thousand rows and the lane a
# couple of dozen, so a 20-row page would make page 2 empty and the partition
# test would pass by having nothing in it -- the same vacuum §7 already guards
# against when it strides MA_US_Remote instead of taking its head.
p1 = [c["job_key"] for c in SRV.API.cards(con, "ai12", 5)["cards"]]
p2 = [c["job_key"] for c in SRV.API.cards(con, "ai12", 5, offset=5)["cards"]]
flat = [c["job_key"] for c in SRV.API.cards(con, "ai12", 10)["cards"]]
fails.append(not ok("both pages actually have rows in them",
                    len(p1) == 5 and len(p2) == 5, f"{len(p1)}+{len(p2)}"))
fails.append(not ok("page 2 does not overlap page 1",
                    not (set(p1) & set(p2)),
                    f"{len(set(p1) & set(p2))} shared"))
fails.append(not ok("the two pages ARE the unpaged first 10, in order",
                    p1 + p2 == flat, f"{len(p1)}+{len(p2)} vs {len(flat)}"))
fails.append(not ok("an offset past the end returns nothing",
                    SRV.API.cards(con, "ai12", 5, offset=10 ** 7)["cards"] == []))
# do_GET hands ?offset= through as whatever string arrived; int() on it would
# raise, and the handler's blanket except would turn a typo into a dead lane.
fails.append(not ok("a junk offset is ignored, not fatal",
                    len(SRV.API.cards(con, "ai12", 5,
                                      offset="abc")["cards"]) == 5))
fails.append(not ok("a negative offset is clamped, not passed through",
                    len(SRV.API.cards(con, "ai12", 5, offset=-9)["cards"]) == 5))

# SQLite has no bare OFFSET -- it is a suffix of LIMIT -- so the helper has to
# supply the "no limit" limit itself rather than emit a syntax error.
try:
    con.execute(LN.queue_sql("1=1", "job_key", order=False,
                             offset=3)).fetchall()
    bare_ok = True
except Exception:                                                 # noqa: BLE001
    bare_ok = False
fails.append(not ok("an offset with no limit is still valid SQL", bare_ok))
fails.append(not ok("no offset leaves the SQL byte-identical",
                    LN.queue_sql("1=1", "job_key", limit=5)
                    == LN.queue_sql("1=1", "job_key", limit=5, offset=0)))

# Paging is a partition only while the order is TOTAL.  Every lane's terms end
# in job_key, the primary key; if one ever stopped, OFFSET would quietly start
# dropping and repeating rows across page boundaries with nothing on screen to
# say so.  Checked under every sort, since posted_terms splices a prefix in.
nokey = [f"{k}/{srt or 'fit'}" for k in lm for srt in LN.SORTS
         if not LN.order_for(k, srt).rstrip().endswith("job_key ASC")]
fails.append(not ok("every lane orders on a total key under every sort",
                    not nokey, ", ".join(nokey[:6])))

# ---------------------------------------------------------------- [16] refs
# The reference number on every card (triage_load.mint_refs).  What makes it
# worth testing is that it claims to be STABLE, and the one thing that is not
# stable in this system is job_key: ma_remote_build collapses duplicates on
# company+title and keeps whichever source copy wins the sort, so 6-8% of keys
# move per rebuild.  A ref anchored to job_key would die for exactly the rows
# carry_forward() exists to rescue, so it anchors on ct_key instead -- and this
# section is the proof, built the same way section 3 proves it for verdicts.
#
# The fourth case is the one nobody would think to write.  mint_refs' UPDATE
# deliberately carries no `ref = 0` guard: job_key is URL-derived so it survives
# an upstream title edit, but ct_key is not, and load()'s upsert writes the new
# ct_key onto the same row.  Guarded, that row would keep the ref of an identity
# it no longer has -- a badge naming the wrong job, which is worse than no badge.
print("\n[16] reference numbers")

con.close()
con, _ = load(v1)                       # clean slate: v1 is the unmodified CSV

everyref = con.execute("SELECT COUNT(*) FROM jobs WHERE ref = 0").fetchone()[0]
fails.append(not ok("every row got a ref", everyref == 0, f"{everyref} at ref=0"))

# a ref names a ct_key, so two live rows sharing company+title share a number
dupe = con.execute(
    "SELECT COUNT(*) FROM (SELECT ct_key FROM jobs WHERE live=1 AND ct_key<>'' "
    "GROUP BY ct_key HAVING COUNT(DISTINCT ref) > 1)").fetchone()[0]
fails.append(not ok("one ct_key never holds two different refs", dupe == 0,
                    f"{dupe} split"))

# blank-company rows fall back to 'jk:'||job_key rather than collapsing onto
# one shared ref, which is what keying them on a blank ct_key would do
blanks = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT ref) FROM jobs WHERE ct_key = ''").fetchone()
fails.append(not ok("blank-company rows get one ref EACH, not one between them",
                    blanks[0] == blanks[1], f"{blanks[0]} rows, {blanks[1]} refs"))

# 1. the ref survives a job_key flip -- the whole point
before = dict(con.execute(
    "SELECT ct_key, ref FROM jobs WHERE live=1 AND ct_key<>''").fetchall())
# NOT `picks` -- section 12 rebound that name to a list of job_key strings.
rpicks = [r for r in ROWS if r[CI].strip() and r[KI].startswith("ats|")][:20]
pick20 = {r[KI] for r in rpicks}
flipped2 = []
for r in ROWS:
    r2 = list(r)
    if r[KI] in pick20:
        r2[KI] = "ats|REFFLIP|x|" + r[KI].split("|")[-1]
    flipped2.append(r2)
vref = write(os.path.join(SP, "vref.csv"), flipped2)
con, s16 = load(vref)
newk = {"ats|REFFLIP|x|" + r[KI].split("|")[-1] for r in rpicks}
qm = ",".join("?" * len(newk))
moved = con.execute(
    f"SELECT ct_key, ref FROM jobs WHERE job_key IN ({qm})", list(newk)).fetchall()
kept = sum(1 for ct, rf in moved if before.get(ct) == rf)
fails.append(not ok("every ref followed the new job_key",
                    kept == len(rpicks) and len(moved) == len(rpicks),
                    f"{kept}/{len(moved)} of {len(rpicks)}"))
fails.append(not ok("the flip minted no new refs for those rows",
                    all(rf in set(before.values()) for _, rf in moved)))

# 2. reloading the identical CSV changes nothing at all
n_before = con.execute("SELECT COUNT(*) FROM refs").fetchone()[0]
con.close()
con, _ = load(vref)
n_after = con.execute("SELECT COUNT(*) FROM refs").fetchone()[0]
fails.append(not ok("an identical reload mints nothing", n_before == n_after,
                    f"{n_before} -> {n_after}"))
m2, r2n = TL.mint_refs(con, corpus="")
fails.append(not ok("mint_refs is idempotent", (m2, r2n) == (0, 0),
                    f"minted {m2}, repointed {r2n}"))

# 3. a retired row keeps its ref, so an old reference still resolves
con.execute("UPDATE jobs SET live = 0 WHERE job_key = ?", (list(newk)[0],))
con.commit()
TL.mint_refs(con, corpus="")
still = con.execute("SELECT ref FROM jobs WHERE job_key = ?",
                    (list(newk)[0],)).fetchone()[0]
fails.append(not ok("a retired row keeps its ref", still > 0, f"ref={still}"))

# 4. a TITLE change is an identity change, and the ref must move with it.
#    This is what the missing `ref = 0` guard buys; with one, this row would
#    keep pointing at the job it used to be.
target = [r for r in ROWS if r[CI].strip() and r[KI].startswith("ats|")][0]
oldref = con.execute("SELECT ref FROM jobs WHERE job_key = ?",
                     (target[KI],)).fetchone()[0]
retitled = []
for r in ROWS:
    r2 = list(r)
    if r[KI] == target[KI]:
        r2[TI] = r[TI] + " (Renamed)"
    retitled.append(r2)
vret = write(os.path.join(SP, "vret.csv"), retitled)
con.close()
con, _ = load(vret)
newref = con.execute("SELECT ref FROM jobs WHERE job_key = ?",
                     (target[KI],)).fetchone()[0]
fails.append(not ok("a retitled row is repointed to a NEW ref",
                    newref != oldref and newref > 0, f"{oldref} -> {newref}"))
fails.append(not ok("and the old ref still exists in the ledger",
                    con.execute("SELECT COUNT(*) FROM refs WHERE ref = ?",
                                (oldref,)).fetchone()[0] == 1))

# 5. ref.py parses every shape that might be typed, and rejects what it should
fails.append(not ok("ref.py parses J4821 / j4821 / #4821 / 4821",
                    [RF.parse_ref(x) for x in ("J4821", "j4821", "#4821", "4821")]
                    == [4821] * 4))
fails.append(not ok("ref.py rejects a non-ref",
                    [RF.parse_ref(x) for x in ("", "J", "abc", "J4821x")]
                    == [None] * 4))

# 6. and it resolves a real one end to end, through the same DB the page reads
rrow = con.execute("SELECT ref, job_key, company, title FROM jobs "
                   "WHERE live=1 AND ct_key<>'' LIMIT 1").fetchone()
res = RF.resolve(con, rrow["ref"], corpus="")
fails.append(not ok("ref.py resolves a live ref to its row",
                    res["known"] and any(j["job_key"] == rrow["job_key"]
                                         for j in res["jobs"]),
                    f"J{rrow['ref']} -> {len(res['jobs'])} row(s)"))
fails.append(not ok("an unminted ref reports as unknown rather than crashing",
                    RF.resolve(con, 99999999, corpus="")["known"] is False))

shutil.rmtree(SP, ignore_errors=True)
_src = os.path.basename(SRC) + (" [synthetic]" if FIXTURE else "")
_head = "ALL PASS" if not any(fails) else f"{sum(fails)} FAILED"
_skip = f", {len(skipped)} skipped" if skipped else ""
print(f"\n{_head}  ({_src}, {N:,} rows{_skip})")
if skipped:
    print("  skipped assertions need a real MA_US_Remote_*.csv:")
    for _s in skipped:
        print(f"    - {_s}")
sys.exit(1 if any(fails) else 0)
