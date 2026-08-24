#!/usr/bin/env python3
"""
bh_github_discover.py — mine committed Bullhorn career-portal configs on GitHub
for (swimlane, corpToken) pairs. Agencies fork bullhorn/career-portal and often
commit app.json / environment files containing a live corpToken + swimlane.

Uses the authenticated `gh api` legacy code-search endpoint (with retries, it is
reliable) then fetches each matching file's contents and regexes the token.

Output: bh_github_tokens.json  ("{swimlane}:{token}" -> repo)
"""
import base64
import json
import re
import subprocess
import time

TOK = re.compile(r"public-rest(\d+)\.bullhornstaffing\.com(?::\d+)?"
                 r"/rest-services/([A-Za-z0-9]{5,8})")
CORP = re.compile(r'corpToken["\']?\s*[:=]\s*["\']([A-Za-z0-9]{5,8})["\']')
SWIM = re.compile(r'swimlane["\']?\s*[:=]\s*["\']?(\d+)')

QUERIES = [
    "corpToken bullhornstaffing.com",
    "corpToken swimlane",
    "swimlane bullhornstaffing.com",
    "public-rest bullhornstaffing.com rest-services",
    "corpToken filename:app.json",
    "corpToken clientId swimlane",
]


def gh(args, tries=5):
    for i in range(tries):
        r = subprocess.run(["gh", "api"] + args, capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout
        if "No server is currently available" in (r.stderr + r.stdout) or \
           "rate limit" in (r.stderr + r.stdout).lower():
            time.sleep(4 * (i + 1))
            continue
        return r.stdout or ""
    return ""


def search(q, pages=8):
    files = set()
    for pg in range(1, pages + 1):
        out = gh(["-X", "GET", "search/code", "-f", f"q={q}",
                  "-f", "per_page=100", "-f", f"page={pg}"])
        try:
            d = json.loads(out or "{}")
        except json.JSONDecodeError:
            break
        its = d.get("items", [])
        if pg == 1:
            print(f"  '{q}' total={d.get('total_count')}", flush=True)
        for it in its:
            files.add((it["repository"]["full_name"], it["path"]))
        if len(its) < 100:
            break
        time.sleep(2.5)
    return files


def main():
    files = set()
    for q in QUERIES:
        files |= search(q)
        time.sleep(2.5)
    print(f"unique candidate files: {len(files)}", flush=True)

    tokens = {}
    for i, (repo, path) in enumerate(sorted(files), 1):
        out = gh([f"repos/{repo}/contents/{path}"])
        try:
            d = json.loads(out)
            txt = base64.b64decode(d.get("content", "")).decode("utf-8",
                                                                "ignore")
        except Exception:
            continue
        m = TOK.search(txt)
        if m:
            tokens[(m.group(1), m.group(2))] = repo
        else:
            c = CORP.search(txt)
            w = SWIM.search(txt)
            if c and w:
                tokens[(w.group(1), c.group(1))] = repo
        if i % 25 == 0:
            print(f"  {i}/{len(files)} files, {len(tokens)} tokens", flush=True)
        time.sleep(0.12)

    json.dump({f"{s}:{t}": r for (s, t), r in tokens.items()},
              open("bh_github_tokens.json", "w"), indent=0)
    print(f"DONE: {len(tokens)} unique (swimlane,corpToken) from GitHub",
          flush=True)
    for (s, t), r in sorted(tokens.items()):
        print(f"  sw{s} {t}  <- {r}", flush=True)


if __name__ == "__main__":
    main()
