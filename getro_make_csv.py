#!/usr/bin/env python3
"""Turn enumerated Getro collections into the final job-board CSV + stats."""
import json, csv, sys
from collections import Counter

SRC = sys.argv[1] if len(sys.argv) > 1 else "getro_collections.jsonl"
OUT_CSV = sys.argv[2] if len(sys.argv) > 2 else "/Users/michaelfornal/Documents/jb/getro_job_boards.csv"
OUT_URLS = "/Users/michaelfornal/Documents/jb/getro_boards_urls.txt"

recs = {}
for line in open(SRC):
    line = line.strip()
    if not line:
        continue
    r = json.loads(line)
    recs[str(r["id"])] = r          # dedupe by id (last wins)
recs = list(recs.values())

def host_type(url):
    u = url.lower()
    if u.endswith(".getro.com"):
        return "getro_subdomain"
    return "custom_domain"

# A "job board" = a collection with a public host in `url`.
boards = [r for r in recs if r.get("url")]
for r in boards:
    u = r["url"].strip().rstrip("/")
    r["_host"] = u
    r["_board_url"] = "https://" + u
    r["_jobs_url"] = "https://" + u + "/jobs"
    r["_host_type"] = host_type(u)

boards.sort(key=lambda r: (r.get("name", "").lower(), r["_host"]))

cols = ["name", "board_url", "jobs_url", "host_type", "custom_domain", "kind",
        "has_job_board", "features", "tabs_order", "indexable", "connect_access",
        "created_at", "collection_id", "slug"]
with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for r in boards:
        w.writerow([
            r.get("name", ""), r["_board_url"], r["_jobs_url"], r["_host_type"],
            r.get("domain", ""), r.get("kind", ""), r.get("has_job_board", ""),
            r.get("features", ""), r.get("tabs_order", ""), r.get("index_job_pages", ""),
            r.get("connect_has_access", ""), r.get("kind_setup_at", ""),
            r.get("id", ""), r.get("slug", ""),
        ])

with open(OUT_URLS, "w", encoding="utf-8") as f:
    f.write("# Every Getro job board (jobs page). Feed to getro_scraper.py --boards\n")
    for r in boards:
        f.write(r["_jobs_url"] + "\n")

# ---- stats ----
total = len(recs)
n_board = len(boards)
n_company = sum(1 for r in recs if not r.get("url") and r.get("kind") == "company")
n_other = total - n_board - n_company
ht = Counter(r["_host_type"] for r in boards)
kinds = Counter(r.get("kind", "") for r in boards)
jb = Counter(bool(r.get("has_job_board")) for r in boards)
# ecosystems with no public url (board-like but unpublished)
eco_nourl = [r for r in recs if not r.get("url") and r.get("kind") == "ecosystem"]

print(f"collections parsed : {total}")
print(f"JOB BOARDS (url set): {n_board}")
print(f"  by host type     : {dict(ht)}")
print(f"  by kind          : {dict(kinds)}")
print(f"  has_job_board    : {dict(jb)}")
print(f"company records    : {n_company}")
print(f"ecosystem, no url  : {len(eco_nourl)}")
print(f"other 200s         : {n_other}")
print(f"\nwrote {n_board} boards -> {OUT_CSV}")
print(f"wrote url list     -> {OUT_URLS}")
print("\nsample boards:")
for r in boards[:15]:
    print(f"  {r.get('name','')[:34]:34} {r['_board_url']}")
