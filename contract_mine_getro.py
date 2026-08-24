#!/usr/bin/env python3
"""
contract_mine_getro.py
Single streaming pass over getro_all_jobs.jsonl (~2.1M rows, 3.1 GB) doing
two independent jobs:

  1. Contract/temp/freelance detection. Getro job records carry NO
     employment-type field, so detection is title-regex only (same guarded
     regex as the Lever miner). Kept rows -> contract_from_getro.jsonl

  2. ATS org-slug harvest. Every Getro job's `url` is the employer's real
     apply link, frequently a first-party ATS board (Ashby, Greenhouse,
     Workable, SmartRecruiters, Recruitee, Breezy, JazzHR, BambooHR,
     Personio, Ashby-EU...). We pull the org slug out of each and tally it,
     so the ATS-board fetchers can re-hit those boards WITH an employment
     -type filter. Output -> getro_ats_slugs.json  {ats: {slug: count}}
"""
import json
import os
import re
from urllib.parse import urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(BASE, "getro_all_jobs.jsonl")
OUT_JOBS = os.path.join(BASE, "contract_from_getro.jsonl")
OUT_SLUGS = os.path.join(BASE, "getro_ats_slugs.json")

TITLE = re.compile(
    r"\b(freelance|freelancer|contractor|temporary|temp|interim|"
    r"seasonal|locum|per[\s-]*diem|fixed[\s-]*term|fractional|"
    r"contract[\s-]*to[\s-]*hire|1099|c2c)\b"
    r"|\(contract\)|\bcontract\s*[:-]|\bon contract\b|contract role|"
    r"contract position|contract opportunity|- contract\b|, contract\b",
    re.I)
TITLE_FP = re.compile(
    r"contract manager|contract managers|contract administ|contract analyst|"
    r"contract specialist|contract coordinat|contract negotiat|"
    r"contract attorney|contract counsel|contract lawyer|contract law|"
    r"contracts? engineer|contract review|contract lifecycle|"
    r"smart contract|contract logistics|government contract",
    re.I)
STRONG = re.compile(
    r"\bfreelance|\bcontractor\b|\btemporary\b|\binterim\b|\bseasonal\b|"
    r"\blocum\b|\bfractional\b|\(contract\)|contract[\s-]*to[\s-]*hire",
    re.I)


def title_is_contract(t):
    t = t or ""
    if TITLE_FP.search(t):
        return bool(STRONG.search(t))
    return bool(TITLE.search(t))


# host -> (ats_name, slug_extractor(host, path_parts))
def ats_of(url):
    try:
        p = urlparse(url)
    except ValueError:
        return None
    host = (p.netloc or "").lower().split(":")[0]
    if not host:
        return None
    parts = [x for x in (p.path or "").split("/") if x]

    def first(): return parts[0] if parts else None

    if host in ("jobs.ashbyhq.com", "ashbyhq.com") or host.endswith(
            ".ashbyhq.com"):
        return ("ashby", first())
    if host in ("boards.greenhouse.io", "job-boards.greenhouse.io",
                "boards.eu.greenhouse.io"):
        return ("greenhouse", first())
    if host.endswith(".greenhouse.io"):
        return ("greenhouse", host.split(".")[0])
    if host == "apply.workable.com":
        return ("workable", first())
    if host.endswith(".workable.com") and host != "www.workable.com":
        return ("workable", host.split(".")[0])
    if host in ("jobs.smartrecruiters.com", "careers.smartrecruiters.com",
                "api.smartrecruiters.com"):
        return ("smartrecruiters", first())
    if host.endswith(".recruitee.com") and host not in (
            "www.recruitee.com", "recruitee.com"):
        return ("recruitee", host.split(".")[0])
    if host.endswith(".breezy.hr"):
        return ("breezy", host.split(".")[0])
    if host.endswith(".applytojob.com"):
        return ("jazzhr", host.split(".")[0])
    if host.endswith(".bamboohr.com"):
        return ("bamboohr", host.split(".")[0])
    if ".jobs.personio." in host or host.endswith(".personio.de") or \
            host.endswith(".personio.com"):
        return ("personio", host.split(".")[0])
    if host == "jobs.lever.co" or host.endswith(".lever.co"):
        return ("lever", first())
    if host.endswith(".teamtailor.com"):
        return ("teamtailor", host.split(".")[0])
    if host == "ats.rippling.com":
        # Board slug is in the PATH, not the host: ats.rippling.com/{org}/jobs/
        # ...  A minority of URLs carry a locale prefix (ats.rippling.com/en-US/
        # {org}/jobs/...), so locate the org structurally by the "jobs" segment
        # rather than guessing which leading segment is a locale.
        # (host.split(".")[0] here returns the literal "ats", which is how one
        # bogus slug came to absorb every Rippling URL in getro_ats_slugs.json.)
        if len(parts) >= 2 and parts[1] == "jobs":
            return ("rippling", parts[0])
        if len(parts) >= 3 and parts[2] == "jobs":
            return ("rippling", parts[1])
        return None
    if host.endswith(".rippling-ats.com"):
        # Per-tenant host: {tenant}.rippling-ats.com/job/{numeric}/{slug}
        return ("rippling", host.split(".")[0])
    return None


def main():
    slugs = {}
    kept = 0
    total = 0
    with open(IN, encoding="utf-8") as f, \
            open(OUT_JOBS, "w", encoding="utf-8") as out:
        for line in f:
            total += 1
            if total % 250000 == 0:
                print(f"  ...{total} rows, {kept} contract, "
                      f"{sum(len(v) for v in slugs.values())} slugs",
                      flush=True)
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            j = rec.get("job") or {}
            url = j.get("url") or ""
            hit = ats_of(url)
            if hit and hit[1]:
                ats, slug = hit
                d = slugs.setdefault(ats, {})
                d[slug] = d.get(slug, 0) + 1
            if title_is_contract(j.get("title") or ""):
                out.write(json.dumps({
                    "board_id": rec.get("board_id"),
                    "board_name": rec.get("board_name"),
                    "board_url": rec.get("board_url"),
                    "job": j,
                }) + "\n")
                kept += 1
    with open(OUT_SLUGS, "w", encoding="utf-8") as f:
        json.dump(slugs, f)
    tot_slugs = sum(len(v) for v in slugs.values())
    print(f"getro: scanned {total}, contract-title {kept}, "
          f"{tot_slugs} unique ATS slugs across {len(slugs)} platforms",
          flush=True)
    for ats, d in sorted(slugs.items(), key=lambda kv: -len(kv[1])):
        print(f"  {ats:16} {len(d):6} slugs  "
              f"{sum(d.values())} jobs", flush=True)


if __name__ == "__main__":
    main()
