#!/usr/bin/env python3
"""
contract_mine_lever.py
Re-mine lever_all_jobs.jsonl for contract / temp / freelance / interim roles.

Two independent signals, unioned:
  1. Lever's own `categories.commitment` tag (authoritative when present).
  2. Title regex fallback (many boards leave commitment blank).

Permanent-contract traps are excluded: "CDI", "permanent contract",
"unbefristet", "vast contract", "regular", "employee" only ever mean FTE.
"Contract" alone is genuinely contingent in US/UK staffing usage.

Output: contract_from_lever.jsonl  (source-tagged, normalized-ready raw rows)
"""
import json
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(BASE, "lever_all_jobs.jsonl")
OUT = os.path.join(BASE, "contract_from_lever.jsonl")

# commitment values that MEAN contingent work
POS = re.compile(
    r"contract|contractor|contractual|freelance|freelancer|temp\b|temporary|"
    r"interim|seasonal|fixed[\s-]*term|fixed term|per[\s-]*diem|per diem|"
    r"locum|casual|\bgig\b|\bcdd\b|befristet|zzp|consultant|consulting|"
    r"independent contractor|1099|c2c|corp[\s-]*to[\s-]*corp|w2 contract|"
    r"contract[\s-]*to[\s-]*hire|c2h|day rate|\bfractional\b",
    re.I)
# permanent-contract traps: a "contract" token that actually means a
# permanent employment contract, not contingent work
NEG = re.compile(
    r"\bcdi\b|permanent contract|contrat permanent|unbefristet|"
    r"vast contract|vaste overeenkomst|festanstellung|contrato indefinido|"
    r"contrato permanente|permanent employment|regular full[\s-]*time|"
    r"contract type[:\s]*permanent",
    re.I)
# title regex: strong contingent phrases only (avoid "contract manager",
# "contract analyst", "contracts specialist", "contract negotiator" = FTE
# roles ABOUT contracts). Require standalone/temporal framings.
TITLE = re.compile(
    r"\b(freelance|freelancer|contractor|temporary|temp|interim|"
    r"seasonal|locum|per[\s-]*diem|fixed[\s-]*term|fractional|"
    r"contract[\s-]*to[\s-]*hire|1099|c2c)\b"
    r"|\(contract\)|\bcontract\s*[:-]|\bon contract\b|contract role|"
    r"contract position|contract opportunity|- contract\b|, contract\b",
    re.I)
# false-positive guard for the bare word "contract" in a title: these are
# permanent roles that merely mention contracts
TITLE_FP = re.compile(
    r"contract manager|contract managers|contract administ|contract analyst|"
    r"contract specialist|contract coordinat|contract negotiat|"
    r"contract attorney|contract counsel|contract lawyer|contract law|"
    r"contracts? engineer|contract review|contract lifecycle|"
    r"smart contract|contract logistics|government contract",
    re.I)


def title_is_contract(title):
    t = title or ""
    if TITLE_FP.search(t):
        # still allow if an unambiguous contingent token is also present
        return bool(re.search(
            r"\bfreelance|\bcontractor\b|\btemporary\b|\binterim\b|"
            r"\bseasonal\b|\blocum\b|\bfractional\b|\(contract\)|"
            r"contract[\s-]*to[\s-]*hire", t, re.I))
    return bool(TITLE.search(t))


def main():
    kept = 0
    total = 0
    by_reason = {"commitment": 0, "title": 0, "both": 0}
    with open(IN, encoding="utf-8") as f, \
            open(OUT, "w", encoding="utf-8") as out:
        for line in f:
            total += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            j = rec.get("job") or {}
            cats = j.get("categories") or {}
            commit = (cats.get("commitment") or "").strip()
            title = j.get("text") or ""

            c_hit = bool(commit and POS.search(commit)
                         and not NEG.search(commit))
            t_hit = title_is_contract(title)
            if not (c_hit or t_hit):
                continue
            reason = "both" if (c_hit and t_hit) else (
                "commitment" if c_hit else "title")
            by_reason[reason] += 1
            rec["_contract_reason"] = reason
            rec["_contract_commitment"] = commit
            out.write(json.dumps(rec) + "\n")
            kept += 1
    print(f"lever: scanned {total}, kept {kept}  {by_reason}", flush=True)


if __name__ == "__main__":
    main()
