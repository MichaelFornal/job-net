#!/usr/bin/env python3
"""
bh_extract_tokens.py — extract (swimlane, corpToken) from Bullhorn career
portals discovered via urlscan.

The open-source bullhorn/career-portal Angular app serves its config at
/app.json (corpToken + swimlane), and constructs API calls as
  https://public-rest{swimlane}.bullhornstaffing.com/rest-services/{corpToken}/
The token also appears verbatim in the built JS bundle. We try the config
endpoints first (cheap, authoritative), then fall back to scraping bundles.

Input : bh_urlscan_domains.txt   ("<swimlane_hint> <domain>" per line)
Output: bh_all_tokens.txt        (unique "<swimlane> <TOKEN>" per line)
        bh_token_map.json        (domain -> {swimlane, corpToken, via})
State : bh_extract_state.json     (domain -> "done"|"fail")
"""
import concurrent.futures as cf
import json
import os
import re
import warnings

import requests

warnings.filterwarnings("ignore")
BASE = os.path.dirname(os.path.abspath(__file__))
IN = os.path.join(BASE, "bh_urlscan_domains.txt")
OUT = os.path.join(BASE, "bh_all_tokens.txt")
MAP = os.path.join(BASE, "bh_token_map.json")
STATE = os.path.join(BASE, "bh_extract_state.json")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

TOK = re.compile(r"public-rest(\d+)\.bullhornstaffing\.com(?::\d+)?"
                 r"/rest-services/([A-Za-z0-9]{5,8})")
CORP = re.compile(r'corpToken["\']?\s*[:=]\s*["\']([A-Za-z0-9]{5,8})["\']')
SWIM = re.compile(r'swimlane["\']?\s*[:=]\s*["\']?(\d+)')
CFG_PATHS = ["/app.json", "/assets/config/app.json", "/config/app.json",
             "/assets/app.json", "/assets/data/app.json"]


def sess():
    s = requests.Session()
    s.headers.update({"User-Agent": UA,
                      "Accept": "application/json, text/plain, */*"})
    return s


def from_text(txt, swim_hint):
    """Return (swimlane, corpToken) from a config/bundle blob, or None."""
    m = TOK.search(txt)
    if m:
        return m.group(1), m.group(2)
    c = CORP.search(txt)
    if c:
        w = SWIM.search(txt)
        return (w.group(1) if w else swim_hint), c.group(1)
    return None


def try_json(s, url, swim_hint):
    try:
        r = s.get(url, timeout=15)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    txt = r.text
    # prefer structured parse
    try:
        j = r.json()
        ct = j.get("corpToken") or (j.get("settings") or {}).get("corpToken")
        sw = j.get("swimlane") or (j.get("settings") or {}).get("swimlane")
        if ct:
            return str(sw or swim_hint), str(ct)
    except ValueError:
        pass
    return from_text(txt, swim_hint)


def scrape_bundles(s, base, html, swim_hint):
    for j in re.findall(r'src=["\']([^"\']+\.js[^"\']*)', html):
        ju = j if j.startswith("http") else base.rstrip("/") + "/" + j.lstrip("/")
        # only bother with likely app bundles
        low = ju.lower()
        if not any(k in low for k in ("main", "app", "runtime", "vendor",
                                      "bundle", "polyfill", "scripts")):
            continue
        try:
            rj = s.get(ju, timeout=20)
        except requests.RequestException:
            continue
        if rj.status_code != 200:
            continue
        got = from_text(rj.text, swim_hint)
        if got:
            return got
    return None


def resolve(domain, swim_hint):
    """Try config endpoints then bundles across a few base URLs."""
    s = sess()
    bases = [f"https://{domain}"]
    if not domain.startswith("www."):
        bases.append(f"https://www.{domain}")
    for base in bases:
        for p in CFG_PATHS:
            got = try_json(s, base + p, swim_hint)
            if got:
                return got + ("app.json",)
        # fall back to the landing page + bundles
        try:
            r = s.get(base, timeout=20)
        except requests.RequestException:
            continue
        if r.status_code != 200:
            continue
        got = from_text(r.text, swim_hint)   # sometimes inlined in HTML
        if got:
            return got + ("html",)
        got = scrape_bundles(s, base, r.text, swim_hint)
        if got:
            return got + ("bundle",)
    return None


def main():
    pairs = []
    with open(IN, encoding="utf-8") as f:
        for line in f:
            p = line.split()
            if len(p) == 2:
                pairs.append((p[1], p[0]))   # (domain, swim_hint)
    state = json.load(open(STATE)) if os.path.exists(STATE) else {}
    tmap = json.load(open(MAP)) if os.path.exists(MAP) else {}
    todo = [(d, h) for d, h in pairs if state.get(d) not in ("done", "fail")]
    print(f"{len(pairs)} domains, {len(todo)} to process", flush=True)

    def work(item):
        d, h = item
        try:
            return d, resolve(d, h)
        except Exception:
            return d, None

    done = 0
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        for d, res in ex.map(work, todo):
            done += 1
            if res:
                sw, tok, via = res
                tmap[d] = {"swimlane": sw, "corpToken": tok, "via": via}
                state[d] = "done"
            else:
                state[d] = "fail"
            if done % 20 == 0:
                json.dump(state, open(STATE, "w"))
                json.dump(tmap, open(MAP, "w"))
                print(f"  {done}/{len(todo)} processed, "
                      f"{len(tmap)} tokens", flush=True)
    json.dump(state, open(STATE, "w"))
    json.dump(tmap, open(MAP, "w"))

    # unique (swimlane, token) pairs
    uniq = sorted({(v["swimlane"], v["corpToken"]) for v in tmap.values()})
    with open(OUT, "w", encoding="utf-8") as f:
        for sw, tok in uniq:
            f.write(f"{sw} {tok}\n")
    print(f"DONE: {len(tmap)}/{len(pairs)} domains yielded a token; "
          f"{len(uniq)} unique (swimlane,token) pairs -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
