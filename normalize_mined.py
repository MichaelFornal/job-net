#!/usr/bin/env python3
"""
normalize_mined.py — convert the re-mined Lever and Getro contract rows
(contract_from_lever.jsonl / contract_from_getro.jsonl) into the canonical
normalized schema in contract_norm/.
"""
import json
import os

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))

PERIOD = {"per-year-salary": "Year", "per-hour-wage": "Hour",
          "per-month-salary": "Month", "per-week-salary": "Week",
          "per-day-wage": "Day"}


def do_lever():
    src = os.path.join(BASE, "contract_from_lever.jsonl")
    if not os.path.exists(src):
        print("no lever mined file", flush=True)
        return
    out = cc.Writer(os.path.join(cc.NORM_DIR, "lever.jsonl"))
    n = 0
    with open(src, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            j = rec.get("job") or {}
            cats = j.get("categories") or {}
            commit = rec.get("_contract_commitment") or cats.get("commitment")
            et = cc.classify(commit, j.get("text"))
            if not et:
                continue
            locs = cats.get("allLocations") or (
                [cats.get("location")] if cats.get("location") else [])
            loc = "; ".join(x.strip() for x in locs if x and x.strip())
            sal = j.get("salaryRange") or {}
            out.write(cc.row(
                source="lever", source_type="lever_board",
                company=rec.get("company") or rec.get("slug"),
                title=j.get("text"), employment_type=et,
                work_mode=cc.work_mode(text=(j.get("workplaceType") or "")
                                       + " " + loc),
                location=loc, country=j.get("country") or "",
                remote="remote" in loc.lower()
                or (j.get("workplaceType") or "").lower() == "remote",
                salary_min=sal.get("min") if isinstance(
                    sal.get("min"), (int, float)) else "",
                salary_max=sal.get("max") if isinstance(
                    sal.get("max"), (int, float)) else "",
                salary_currency=(sal.get("currency") or "").upper(),
                salary_period=PERIOD.get(sal.get("interval") or "", ""),
                posted=cc.ymd(ts=j.get("createdAt")),
                apply_url=j.get("applyUrl") or j.get("hostedUrl") or "",
                job_id=j.get("id"), board=rec.get("slug"),
                board_url=rec.get("board_url") or ""))
            n += 1
    out.close()
    print(f"lever normalized: {n}", flush=True)


def do_getro():
    src = os.path.join(BASE, "contract_from_getro.jsonl")
    if not os.path.exists(src):
        print("no getro mined file", flush=True)
        return
    out = cc.Writer(os.path.join(cc.NORM_DIR, "getro.jsonl"))
    n = 0
    with open(src, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            j = rec.get("job") or {}
            et = cc.classify(None, j.get("title"))  # getro: title-only
            if not et:
                continue
            org = j.get("organization") or {}
            locs = j.get("searchable_locations") or []
            loc = "; ".join(x for x in locs[:3] if x)
            wm = {"remote": "Remote", "on_site": "On-site",
                  "hybrid": "Hybrid"}.get(j.get("work_mode") or "", "")
            mn = j.get("compensation_amount_min_cents")
            mx = j.get("compensation_amount_max_cents")
            out.write(cc.row(
                source="getro", source_type="vc_board",
                company=org.get("name") or rec.get("board_name") or "",
                title=j.get("title"), employment_type=et, work_mode=wm,
                location=loc, remote=(j.get("work_mode") == "remote"
                                      or "remote" in loc.lower()),
                salary_min=round(mn / 100) if isinstance(mn, (int, float))
                else "",
                salary_max=round(mx / 100) if isinstance(mx, (int, float))
                else "",
                salary_currency=(j.get("compensation_currency") or "").upper(),
                salary_period="",
                posted=cc.ymd(ts=j.get("created_at")),
                apply_url=j.get("url") or "", job_id=j.get("id"),
                board=rec.get("board_name") or "",
                board_url=rec.get("board_url") or ""))
            n += 1
    out.close()
    print(f"getro normalized: {n}", flush=True)


if __name__ == "__main__":
    do_lever()
    do_getro()
