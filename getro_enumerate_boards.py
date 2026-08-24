#!/usr/bin/env python3
"""Enumerate every Getro collection via api.getro.com by ID (single keep-alive conn).
Usage: python getro_enumerate_boards.py START END OUT.jsonl
Resumable: skips IDs already present in OUT.jsonl and OUT.jsonl.scanned."""
import sys, os, json, time, random, warnings
warnings.filterwarnings("ignore")
import requests

START = int(sys.argv[1]); END = int(sys.argv[2])
OUT = sys.argv[3] if len(sys.argv) > 3 else "getro_collections.jsonl"
PROG = OUT + ".progress"; ERRS = OUT + ".errors"; SCAN = OUT + ".scanned"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
BASE = "https://api.getro.com/api/v2/collections/"
H = {"User-Agent": UA, "Accept": "application/json"}
RETRYABLE = {418, 429, 500, 502, 503, 504}

done_ids = set()
if os.path.exists(SCAN):
    for line in open(SCAN):
        line = line.strip()
        if line.isdigit():
            done_ids.add(int(line))

sess = requests.Session()
fout = open(OUT, "a", buffering=1)
ferr = open(ERRS, "a", buffering=1)
fscan = open(SCAN, "a", buffering=1)

def fetch(cid):
    # 418 is a terminal per-collection status (disabled/blocked board), common on old IDs:
    # give it ONE quick retry (covers the rare genuine throttle) then accept it.
    # 429/5xx/connection errors are real transients: back off and retry harder.
    backoff = 0.8; last = -1
    for _ in range(6):
        try:
            r = sess.get(BASE + str(cid), headers=H, timeout=15)
            sc = r.status_code; last = sc
            if sc == 418:
                return 418, None          # terminal per-collection flag; recheck pass covers rare transients
            if sc in (429, 500, 502, 503, 504):
                time.sleep(backoff + random.random() * 0.4); backoff = min(backoff * 1.8, 6); continue
            return sc, r.content
        except Exception:
            time.sleep(backoff + random.random() * 0.4); backoff = min(backoff * 1.8, 6); continue
    return last, None

def extract(raw):
    d = json.loads(raw)["data"]; a = d["attributes"]
    feats = a.get("features") or []
    return {
        "id": d.get("id"), "name": a.get("name") or "", "url": a.get("url") or "",
        "domain": a.get("domain") or "", "slug": a.get("slug") or "", "kind": a.get("kind") or "",
        "features": "|".join(feats), "has_job_board": "job_board" in feats,
        "tabs_order": a.get("tabs_order") or "", "index_job_pages": a.get("index_job_pages"),
        "connect_has_access": (a.get("connect") or {}).get("has_access"),
        "kind_setup_at": a.get("kind_setup_at") or "",
    }

def main():
    ids = [i for i in range(START, END + 1) if i not in done_ids]
    total = len(ids)
    c = {"board": 0, "company": 0, "other200": 0, "404": 0, "err": 0, "flagged": 0}
    t0 = time.time()
    for n, cid in enumerate(ids, 1):
        status, raw = fetch(cid)
        if status == 200 and raw:
            try:
                rec = extract(raw)
                if rec["url"]:
                    c["board"] += 1
                elif rec["kind"] == "company":
                    c["company"] += 1
                else:
                    c["other200"] += 1
                fout.write(json.dumps(rec) + "\n")
            except Exception as e:
                c["err"] += 1; ferr.write(json.dumps({"id": cid, "parse_error": str(e)}) + "\n")
        elif status == 404:
            c["404"] += 1
        elif status == 418:
            c["flagged"] += 1; ferr.write(json.dumps({"id": cid, "status": 418}) + "\n")
        else:
            c["err"] += 1; ferr.write(json.dumps({"id": cid, "status": status}) + "\n")
        fscan.write(str(cid) + "\n")
        if n % 500 == 0 or n == total:
            el = time.time() - t0; rate = n / el if el else 0
            eta = (total - n) / rate if rate else 0
            msg = (f"[{n}/{total}] {el:.0f}s {rate:.1f}/s eta{eta/60:.1f}m "
                   f"boards={c['board']} companies={c['company']} other={c['other200']} "
                   f"404={c['404']} flagged={c['flagged']} err={c['err']}")
            with open(PROG, "w") as p:
                p.write(msg + "\n")
            print(msg, flush=True)
    print("DONE " + json.dumps(c), flush=True)
    with open(PROG, "a") as p:
        p.write("DONE " + json.dumps(c) + "\n")

if __name__ == "__main__":
    main()
