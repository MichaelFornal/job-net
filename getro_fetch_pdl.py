#!/usr/bin/env python3
"""
getro_fetch_pdl.py
Complete fetch of the People Data Labs board (collection 12896, ~954k jobs),
which exceeds the search API's 200,000-row result window (page*hits_per_page).

Strategy: enumerate every company on the board via search/companies (14.8k,
each with active_jobs_count), first-fit-decreasing pack company ids into
batches expected to stay under 150k jobs, then fetch each batch's jobs with
filters={"organization.id": [...ids]} — a disjoint, null-leak-free partition,
since every job belongs to exactly one organization.

Also marks collections 8012 (Invisible) and 13655 (Advisors) done-with-zero:
their ES jobs indices don't exist (job_board feature removed server-side).

Appends to getro_all_jobs.jsonl in the same format as getro_fetch_jobs.py and
updates getro_fetch_state.json so the sheet builder sees one unified state.
Resumable per batch via getro_pdl_state.json.
"""

import json
import os
import time

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, "getro_fetch_state.json")
PDL_STATE = os.path.join(BASE, "getro_pdl_state.json")

CID = 12896
BOARD_NAME = "People Data Labs"
BOARD_URL = "https://peopledatalabs.getro.com"
BATCH_JOB_LIMIT = 150_000
WINDOW = 200_000
HPP = 1000
PACE = 0.3
TIMEOUT = 60
TRIES = 4
BACKOFFS = [5, 15, 45]

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

S = requests.Session()
S.headers.update({"User-Agent": UA, "Accept": "application/json"})


def post(path, body):
    last = None
    for attempt in range(TRIES):
        try:
            r = S.post(f"https://api.getro.com/api/v2/collections/{CID}/{path}",
                       json=body, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}: {r.text[:120]}"
        except requests.RequestException as e:
            last = f"{type(e).__name__}: {e}"
        if attempt < TRIES - 1:
            time.sleep(BACKOFFS[min(attempt, len(BACKOFFS) - 1)])
    raise RuntimeError(f"{path}: {last}")


def load(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def enumerate_companies():
    companies = []
    page = 0
    while True:
        d = post("search/companies", {"hits_per_page": HPP, "page": page})
        res = d["results"]
        batch = res.get("companies", [])
        if page == 0:
            print(f"companies reported: {res.get('count')}", flush=True)
        if not batch:
            break
        companies.extend(
            {"id": c["id"], "n": c.get("active_jobs_count") or 0}
            for c in batch)
        if len(companies) >= res.get("count", 0):
            break
        page += 1
        time.sleep(PACE)
    return companies


def make_batches(companies):
    """First-fit decreasing bin pack by expected job count."""
    batches = []  # each: {"ids": [...], "expected": int}
    for c in sorted(companies, key=lambda x: -x["n"]):
        placed = False
        for b in batches:
            if b["expected"] + c["n"] <= BATCH_JOB_LIMIT:
                b["ids"].append(c["id"])
                b["expected"] += c["n"]
                placed = True
                break
        if not placed:
            batches.append({"ids": [c["id"]], "expected": c["n"]})
    return batches


def fetch_batch(ids):
    """Fetch all jobs for a set of org ids, streaming each page straight to
    the JSONL (tiny RAM footprint). Returns (n_written, count). Splits itself
    in half if live count crowds the API window. If a run dies mid-batch the
    partial lines stay on disk and the rerun re-fetches the batch; the sheet
    builder dedupes by job id, so duplicates are harmless."""
    d = post("search/jobs", {"hits_per_page": 1, "page": 0,
                             "filters": {"organization.id": ids}})
    count = d["results"]["count"]
    if count > WINDOW - 5000 and len(ids) > 1:
        mid = len(ids) // 2
        print(f"    batch drifted to {count}, splitting", flush=True)
        n1, c1 = fetch_batch(ids[:mid])
        n2, c2 = fetch_batch(ids[mid:])
        return n1 + n2, c1 + c2
    written = 0
    page = 0
    while written < count:
        d = post("search/jobs", {"hits_per_page": HPP, "page": page,
                                 "filters": {"organization.id": ids}})
        batch = d["results"]["jobs"]
        if not batch:
            break
        with open(OUT_JSONL, "a", encoding="utf-8") as out:
            for j in batch:
                j.pop("location_details", None)
                out.write(json.dumps({
                    "board_id": CID,
                    "board_name": BOARD_NAME,
                    "board_url": BOARD_URL,
                    "job": j,
                }, ensure_ascii=False) + "\n")
        written += len(batch)
        page += 1
        time.sleep(PACE)
    return written, count


def main():
    st = load(PDL_STATE, {})

    if "batches" not in st:
        print("enumerating companies...", flush=True)
        companies = enumerate_companies()
        total_expected = sum(c["n"] for c in companies)
        biggest = max(companies, key=lambda c: c["n"])
        print(f"{len(companies)} companies, {total_expected} expected jobs, "
              f"largest org {biggest['id']} = {biggest['n']}", flush=True)
        if biggest["n"] > WINDOW - 5000:
            raise SystemExit("an org alone exceeds the API window; "
                             "needs a finer split")
        st["batches"] = make_batches(companies)
        st["done"] = []
        save(PDL_STATE, st)

    batches = st["batches"]
    print(f"{len(batches)} batches "
          f"({[b['expected'] for b in batches]})", flush=True)

    total = 0
    for i, b in enumerate(batches):
        if i in st["done"]:
            continue
        written, count = fetch_batch(b["ids"])
        total += written
        st["done"].append(i)
        st[f"batch_{i}"] = {"expected": b["expected"], "count": count,
                            "fetched": written}
        save(PDL_STATE, st)
        print(f"[batch {i + 1}/{len(batches)}] expected {b['expected']} "
              f"live {count} fetched {written} | total {total}", flush=True)

    # verify against live board total
    d = post("search/jobs", {"hits_per_page": 1, "page": 0})
    board_count = d["results"]["count"]
    grand = sum(st[f"batch_{i}"]["fetched"] for i in range(len(batches))
                if f"batch_{i}" in st)
    print(f"\nboard reports {board_count}; partition fetched {grand} "
          f"({board_count - grand} outside org partition)", flush=True)

    # fold into the main fetch state
    main_state = load(STATE_JSON, {})
    main_state[str(CID)] = {"status": "done", "count": board_count,
                            "fetched": grand, "name": BOARD_NAME}
    for cid, name in (("8012", "Invisible"), ("13655", "Advisors")):
        main_state[cid] = {"status": "done", "count": 0, "fetched": 0,
                           "name": name,
                           "note": "no jobs index (job_board removed)"}
    save(STATE_JSON, main_state)
    print("state updated for 12896 / 8012 / 13655", flush=True)


if __name__ == "__main__":
    main()
