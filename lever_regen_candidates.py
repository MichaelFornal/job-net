#!/usr/bin/env python3
"""
lever_regen_candidates.py
Rebuild lever_candidates.csv from the current lever_wayback_urls.txt (which
lever_discover_boards.py appends to while crawling) plus the Lever slugs
mined from the Getro dump. Safe to run while discovery is still crawling —
used between incremental probe/fetch waves.
"""

import csv
import importlib.util
import os

BASE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "d", os.path.join(BASE, "lever_discover_boards.py"))
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)

cand = {}


def add(slug, eu, src):
    e = cand.setdefault(slug, {"eu": False, "sources": set()})
    e["eu"] = e["eu"] or eu
    e["sources"].add(src)


with open(os.path.join(BASE, "lever_wayback_urls.txt"),
          encoding="utf-8") as f:
    for line in f:
        tag, _, url = line.partition("\t")
        if url:
            slug, eu = d.slug_of(url)
            if slug:
                add(slug, eu or tag.startswith("eu"), "wayback")

pre = os.path.join(os.environ.get("LEVER_SCRATCH", ""),
                   "lever_from_getro_raw.txt")
if os.environ.get("LEVER_SCRATCH") and os.path.exists(pre):
    with open(pre, encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if len(parts) == 2:
                slug, eu = d.slug_of("https://" + parts[1])
                if slug:
                    add(slug, eu, "getro")

out = os.path.join(BASE, "lever_candidates.csv")
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["slug", "eu_hint", "sources"])
    for slug in sorted(cand, key=str.lower):
        e = cand[slug]
        w.writerow([slug, int(e["eu"]), "|".join(sorted(e["sources"]))])
print(f"{len(cand)} candidates -> {out}", flush=True)
