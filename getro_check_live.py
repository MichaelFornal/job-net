#!/usr/bin/env python3
"""Liveness-check every Getro board and write the final CSV with a status column."""
import json, csv, os, sys, urllib.request, urllib.error, warnings, threading, time
from concurrent.futures import ThreadPoolExecutor

BASE = os.path.dirname(os.path.abspath(__file__))
warnings.filterwarnings("ignore")

# argv[1] overrides; default to the file the enumerator actually produced
SRC = sys.argv[1] if len(sys.argv) > 1 else (
    "getro_all_collections.jsonl"
    if os.path.exists("getro_all_collections.jsonl")
    else "getro_collections.jsonl")
OUT = os.path.join(BASE, "getro_job_boards.csv")
URLS = os.path.join(BASE, "getro_boards_urls.txt")
UA=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

recs={}
for l in open(SRC):
    l=l.strip()
    if not l: continue
    r=json.loads(l); recs[str(r["id"])]=r
boards=[r for r in recs.values() if r.get("url")]

def host_type(u): return "getro_subdomain" if u.lower().endswith(".getro.com") else "custom_domain"

lock=threading.Lock(); done=[0]
def check(r):
    u=r["url"].strip().rstrip("/")
    r["_host"]=u; r["_board_url"]="https://"+u; r["_jobs_url"]="https://"+u+"/jobs"
    r["_host_type"]=host_type(u)
    status="dead"
    try:
        req=urllib.request.Request("https://"+u+"/jobs", headers={"User-Agent":UA})
        resp=urllib.request.urlopen(req, timeout=18)
        body=resp.read(600000).decode("utf-8","replace")
        if "__NEXT_DATA__" in body: status="live"
        else: status="up_not_getro"
    except urllib.error.HTTPError as e:
        status=f"dead_http_{e.code}"
    except Exception as e:
        status="dead_"+type(e).__name__
    r["_status"]=status
    with lock:
        done[0]+=1
        if done[0]%100==0: print(f"  checked {done[0]}/{len(boards)}", flush=True)
    return r

t0=time.time()
with ThreadPoolExecutor(max_workers=12) as ex:
    list(ex.map(check, boards))
print(f"liveness done in {time.time()-t0:.0f}s", flush=True)

boards.sort(key=lambda r:(r.get("name","").lower(), r["_host"]))
cols=["name","board_url","jobs_url","status","host_type","custom_domain","kind","has_job_board",
      "features","tabs_order","indexable","connect_access","created_at","collection_id","slug"]
with open(OUT,"w",newline="",encoding="utf-8") as f:
    w=csv.writer(f); w.writerow(cols)
    for r in boards:
        w.writerow([r.get("name",""),r["_board_url"],r["_jobs_url"],r["_status"],r["_host_type"],
                    r.get("domain",""),r.get("kind",""),r.get("has_job_board",""),r.get("features",""),
                    r.get("tabs_order",""),r.get("index_job_pages",""),r.get("connect_has_access",""),
                    r.get("kind_setup_at",""),r.get("id",""),r.get("slug","")])
# scraper-ready: only LIVE boards
with open(URLS,"w",encoding="utf-8") as f:
    f.write("# Live Getro job boards (jobs page). Feed to getro_scraper.py --boards\n")
    for r in boards:
        if r["_status"]=="live": f.write(r["_jobs_url"]+"\n")

from collections import Counter
c=Counter(r["_status"] for r in boards)
simple=Counter("live" if r["_status"]=="live" else ("up_not_getro" if r["_status"]=="up_not_getro" else "dead") for r in boards)
print("STATUS breakdown:", dict(c))
print("SIMPLE:", dict(simple))
print(f"total boards={len(boards)}  live={simple['live']}  wrote {OUT}")
