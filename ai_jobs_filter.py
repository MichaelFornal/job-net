#!/usr/bin/env python3
"""
ai_jobs_filter.py — filter all_jobs_master.csv down to AI-building roles.

Tiers:
  Core AI Building : title matches Claude / agentic / LLM / GenAI / applied AI /
                     AI engineer-developer-builder / prompt engineer / forward deployed
  AI-ML Adjacent   : title matches ML engineer / AI-ML / NLP / AI architect etc.
  Skills Match     : engineering-role title whose skills list LLM/GenAI tooling
                     (only rows not already captured by the title tiers)

Outputs:
  AI_Builder_Jobs_<date>.xlsx  (Summary / Core AI Building / AI-ML Adjacent / Skills Match)
  ai_jobs_filtered.csv         (all tiers, with tier + matched columns)
"""

import csv
import os
import re
from datetime import datetime

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.utils import get_column_letter

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")
SNAP = datetime.now().strftime("%Y-%m-%d")
OUT_XLSX = os.path.join(BASE, f"AI_Builder_Jobs_{SNAP}.xlsx")
OUT_CSV = os.path.join(BASE, "ai_jobs_filtered.csv")

csv.field_size_limit(10**9)

CORE = re.compile(r"""(?ix)
    \bclaude\b | \banthropic\b | \bagentic\b | \bai\s+agents?\b | \bllms?\b |
    \blarge\s+language\s+model | \bgen\s?ai\b | \bgenerative\s+ai\b |
    \bapplied\s+ai\b | \bprompt\s+engineer |
    # "forward-deployed" was the only spelling matched, so "Forward Deployment
    # Engineer" and "Forward Deploy Engineer" — same role, ~22 live rows at
    # Clera, Unit21, SambaNova, SentinelOne, Flock Safety — carried no band at
    # all.  The bare FDE acronym is safe on measurement: 16 of 16 live rows
    # using it without the spelled-out phrase are genuine FDE postings.
    \bforward[\s-]+deploy(?:ed|ment)?\b | \bfde\b |
    \bai[\s/-]+(?:engineer|developer|dev\b|builder|application|apps?\b|product\s+engineer|
                full[\s-]?stack|software|solutions?\s+engineer|automation|platform\s+engineer|
                integration|tooling) |
    \bfounding\s+ai\b | \bopenai\b | \bgpt\b | \blangchain\b | \bRAG\b |
    \bai[\s-]native\b | \bcoding\s+agents?\b | \bcopilot\s+(?:engineer|developer)
""")

ADJ = re.compile(r"""(?ix)
    \bmachine\s+learning\b | \bml\s+(?:engineer|scientist|ops|platform|infra) | \bmlops\b |
    \bai/ml\b | \bml/ai\b | \bartificial\s+intelligence\b | \bdeep\s+learning\b |
    \bnlp\b | \bconversational\s+ai\b | \bchatbot | \bcomputer\s+vision\b |
    \bai\s+(?:architect|scientist|researcher|research|consultant|specialist|lead|manager|strategist)\b |
    \bcopilot\b | \bhugging\s?face\b
""")

SKILL = re.compile(r"""(?ix)
    \bllms?\b | \blangchain\b | \bllamaindex\b | \bopenai\b | \banthropic\b | \bclaude\b |
    \bgpt\b | \bretrieval[-\s]augmented | \bRAG\b | prompt\s+engineering | generative\s+ai |
    \bgen\s?ai\b | agentic | hugging\s?face | vector\s+database | ai\s+coding | copilot
""")

ENG_TITLE = re.compile(
    r"(?i)engineer|developer|\bswe\b|software|full[ -]?stack|back[ -]?end|"
    r"front[ -]?end|architect|programmer|\bdev\b"
)

# ---- the AI-modifier gap -----------------------------------------------------
# CORE only fires when the AI token sits IMMEDIATELY before a whitelisted role
# noun (`\bai[\s/-]+(?:engineer|developer|...)`).  Real titles rarely oblige:
# any qualifier between the two, or an AI token trailing the noun, and the row
# is invisible to every AI tier.  Measured on the 191k undecided rank_tier=9
# triage rows (2026-08-12), that silently dropped 1,342 AI engineering roles —
# among them OpenAI's four "AI Deployment Engineer" postings and Fireworks'
# four "AI Field Engineer" ones, which are forward-deployed roles in all but
# the literal phrase CORE looks for.
#
#   AI Security Engineer            qualifier between token and noun
#   AI Solutions Architect          "architect" absent from CORE's noun list
#   Senior Software Engineer, AI    token trails the noun
#   AI-First SRE/DevOps Engineer    three tokens between the two
#
# A bounded gap ("at most two words between") was tried and rejected: it still
# lost 149 of the 1,342, including Netflix's "Software Engineer 5 - Agent
# Platform, AI Platform".  Plain co-occurrence measured 96% precision on the
# real corpus, so the conjunction below is the whole rule — an AI token and a
# technical role noun in the same title, in either order, at any distance.
AI_TOKEN = re.compile(r"""(?ix)
    \bai\b | \ba\.i\.\b | \bartificial\s+intelligence\b | \bllms?\b |
    \bgen\s?ai\b | \bgenerative\s+ai\b
""")
AI_ROLE_NOUN = re.compile(r"""(?ix)
    \bengineer(?:ing)?\b | \bdeveloper\b | \barchitect\b | \bscientist\b |
    \bbuilder\b | \bprogrammer\b | \bswe\b | \bsoftware\b |
    \btechnical\s+staff\b
""")

# The one category the conjunction gets wrong often enough to be worth naming:
# data-labelling gigs advertised to engineers ("Python Engineer - Freelance AI
# Trainer").  Anchored on the PERSON noun, never on "AI training" — that phrase
# also names real infrastructure work ("AI Training Infrastructure Engineer"),
# and this repo has twice shipped a blocklist that killed legitimate rows.
AI_LABELING = re.compile(
    r"(?i)\b(?:trainer|annotator|labell?er|rater|tutor|transcriber)s?\b")


def combo_hit(title):
    """Match on the AI token when a title pairs one with a role noun, else None.

    Returns the AI-token match so callers keep a meaningful `matched` value.
    """
    if AI_LABELING.search(title) or not AI_ROLE_NOUN.search(title):
        return None
    return AI_TOKEN.search(title)

COLS = [
    "company", "title", "seniority", "employment_type", "work_mode", "remote",
    "location", "country", "posted", "salary_min", "salary_max",
    "salary_currency", "salary_period", "skills", "company_stage",
    "company_size", "company_industry", "apply_url", "job_url", "source",
]
OUT_COLS = ["tier", "matched"] + COLS

WIDTHS = {
    "tier": 16, "matched": 20, "company": 24, "title": 46, "seniority": 10,
    "employment_type": 13, "work_mode": 10, "remote": 8, "location": 28,
    "country": 8, "posted": 11, "salary_min": 10, "salary_max": 10,
    "salary_currency": 8, "salary_period": 8, "skills": 50, "company_stage": 12,
    "company_size": 12, "company_industry": 20, "apply_url": 45, "job_url": 45,
    "source": 14,
}


def clean(v):
    if v is None:
        return ""
    return ILLEGAL_CHARACTERS_RE.sub("", str(v))


def main():
    tiers = {"Core AI Building": [], "AI-ML Adjacent": [], "Skills Match": []}
    total = 0
    with open(SRC, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += 1
            title = row.get("title", "")
            m = CORE.search(title)
            if m:
                tier = "Core AI Building"
            else:
                m = ADJ.search(title)
                if m:
                    tier = "AI-ML Adjacent"
                else:
                    sk = row.get("skills", "")
                    if not (sk and ENG_TITLE.search(title)):
                        continue
                    m = SKILL.search(sk)
                    if not m:
                        continue
                    tier = "Skills Match"
            rec = {c: row.get(c, "") for c in COLS}
            rec["tier"] = tier
            rec["matched"] = re.sub(r"\s+", " ", m.group(0)).strip()
            tiers[tier].append(rec)

    for rows in tiers.values():
        rows.sort(key=lambda r: (r["posted"] or "", r["company"]), reverse=True)

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS)
        w.writeheader()
        for name in tiers:
            w.writerows(tiers[name])

    wb = Workbook(write_only=True)

    ws = wb.create_sheet("Summary")
    ws.column_dimensions["A"].width = 24
    ws.column_dimensions["B"].width = 14
    ws.column_dimensions["C"].width = 90
    ws.append([f"AI Builder Jobs — {SNAP}"])
    ws.append([])
    ws.append(["Sheet", "Jobs", "What it is"])
    ws.append(["Core AI Building", len(tiers["Core AI Building"]),
               "Title says the job IS building with AI: Claude/Anthropic, agentic, AI agents, "
               "LLM, GenAI, applied AI, AI engineer/developer, prompt engineer, forward deployed."])
    ws.append(["AI-ML Adjacent", len(tiers["AI-ML Adjacent"]),
               "Similar roles: ML engineer, AI/ML, MLOps, NLP, conversational AI, "
               "AI architect/researcher, computer vision, Copilot."])
    ws.append(["Skills Match", len(tiers["Skills Match"]),
               "Engineering titles without AI in the name whose listed skills include "
               "LLM/LangChain/OpenAI/Claude/RAG/prompt-engineering etc."])
    ws.append(["Total", sum(len(v) for v in tiers.values()),
               f"Filtered from {total:,} jobs in all_jobs_master.csv."])
    ws.append([])
    ws.append(["The 'matched' column shows which keyword put the job in its sheet."])

    for name, rows in tiers.items():
        ws = wb.create_sheet(name)
        for i, c in enumerate(OUT_COLS, 1):
            ws.column_dimensions[get_column_letter(i)].width = WIDTHS.get(c, 14)
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(OUT_COLS))}{len(rows) + 1}"
        ws.append(OUT_COLS)
        for r in rows:
            ws.append([clean(r[c]) for c in OUT_COLS])

    wb.save(OUT_XLSX)
    print(f"scanned {total:,} jobs")
    for name, rows in tiers.items():
        print(f"  {name}: {len(rows):,}")
    print(f"wrote {OUT_XLSX}")
    print(f"wrote {OUT_CSV}")


if __name__ == "__main__":
    main()
