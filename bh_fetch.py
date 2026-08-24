#!/usr/bin/env python3
"""
bh_fetch.py — fetch open, public JobBoardPost records from the Bullhorn OSCP
public REST API for every discovered (swimlane, corpToken) pair.

Endpoint (unauthenticated, verified 2026-07-19):
  GET https://public-rest{swimlane}.bullhornstaffing.com/rest-services/
      {corpToken}/query/JobBoardPost
      ?fields=...&where=isOpen=true AND isDeleted=false AND isPublic=1
      &orderBy=-dateLastPublished&count=200&start=N

BH_KEEP_ALL=1 -> keep every employment type (Permanent/Contract/Contract To
Hire/Temporary); default keeps contingent only (legacy behaviour).

Input : bh_tokens_merged.txt   ("<swimlane> <TOKEN>" per line)
        bh_token_map.json       (domain -> {swimlane, corpToken}) for names
Output: contract_norm/bullhorn_all.jsonl  (or bullhorn.jsonl if not KEEP_ALL)
State : bh_all_fetch_state.json  (or bh_fetch_state.json)
"""
import json
import os

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TOKENS = os.path.join(BASE, "bh_tokens_merged.txt")
TOKEN_MAP = os.path.join(BASE, "bh_token_map.json")
KEEP_ALL = os.environ.get("BH_KEEP_ALL") == "1"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
OUT_NAME = "bullhorn_all.jsonl" if KEEP_ALL else "bullhorn.jsonl"
STATE = os.path.join(BASE, "bh_all_fetch_state.json" if KEEP_ALL
                     else "bh_fetch_state.json")
FIELDS = ("id,title,employmentType,dateLastPublished,dateAdded,"
          "address(city,state,zip,countryName),salary,payRate,"
          "publishedZip,isOpen,isPublic")
WHERE = "isOpen=true AND isDeleted=false AND isPublic=1"
PAGE = 200


def company_index():
    """(swimlane, corpToken) -> a human-ish company name from the portal domain."""
    idx = {}
    if os.path.exists(TOKEN_MAP):
        for dom, v in json.load(open(TOKEN_MAP)).items():
            key = (str(v.get("swimlane")), str(v.get("corpToken")))
            name = dom.replace("www.", "").replace("careerportal.", "") \
                      .replace("careers.", "").replace("jobs.", "")
            idx[key] = name
    return idx


def main():
    pairs = []
    with open(TOKENS, encoding="utf-8") as f:
        for line in f:
            p = line.split()
            if len(p) == 2:
                pairs.append((p[0], p[1]))
    names = company_index()
    state = cc.load_state(STATE)
    out = cc.Writer(os.path.join(cc.NORM_DIR, OUT_NAME))
    s = cc.session()
    kept = live = 0
    by_type = {}
    for sl, tok in pairs:
        key = f"{sl}:{tok}"
        if state.get(key) == "done":
            continue
        company = names.get((sl, tok), f"Bullhorn agency {tok}")
        base = (f"https://public-rest{sl}.bullhornstaffing.com"
                f"/rest-services/{tok}/query/JobBoardPost")
        start = 0
        board_live = False
        while True:
            d = cc.get_json(s, base, params={
                "fields": FIELDS, "where": WHERE,
                "orderBy": "-dateLastPublished", "count": PAGE, "start": start})
            data = d.get("data")
            if data is None:
                break
            board_live = True
            for p in data:
                et = CLASSIFY(p.get("employmentType"), p.get("title"))
                if not et:
                    continue
                a = p.get("address") or {}
                pay = p.get("payRate")
                sal = p.get("salary")
                loc = ", ".join(x for x in (a.get("city"), a.get("state"),
                                            a.get("countryName")) if x)
                out.write(cc.row(
                    source="bullhorn", source_type="staffing_ats",
                    company=company, title=p.get("title"), employment_type=et,
                    work_mode=cc.work_mode(text=loc + " " + (p.get("title") or "")),
                    location=loc, city=a.get("city") or "",
                    state=a.get("state") or "",
                    country=a.get("countryName") or "",
                    salary_min=pay if isinstance(pay, (int, float)) and pay
                    else (sal if isinstance(sal, (int, float)) else ""),
                    salary_period="Hour" if isinstance(pay, (int, float)) and pay
                    else ("Year" if isinstance(sal, (int, float)) and sal
                          else ""),
                    posted=cc.ymd(ts=p.get("dateLastPublished")),
                    apply_url="", job_id=p.get("id"),
                    board=f"bullhorn:{tok}", board_url=f"https://{company}"))
                kept += 1
                by_type[et] = by_type.get(et, 0) + 1
            if len(data) < PAGE:
                break
            start += PAGE
            if start >= 10000:      # safety cap per board
                break
        state[key] = "done" if board_live else "dead"
        live += 1 if board_live else 0
        cc.save_state(STATE, state)
    out.close()
    s.close()
    print(f"DONE bullhorn: {live} live boards, {kept} rows kept", flush=True)
    print("  by type:", by_type, flush=True)


if __name__ == "__main__":
    main()
