#!/usr/bin/env python3
"""
notion_common.py — shared client for public notion.site careers pages.

Companies publish a careers page as a public Notion page.  The unofficial
/api/v3/ endpoints serve it as JSON with NO key and NO cookie:

  loadPageChunk      {pageId,limit,cursor:{stack:[]},chunkNumber} -> recordMap
  queryCollection    source+collectionView+loader                 -> row blocks
  getRecordValues    {requests:[{table:"collection",id}]}          -> schema
  getPublicPageData  {type:"block-space",spaceDomain:<sub>,...}    -> spaceName

www.notion.so and <sub>.notion.site return byte-identical responses; we use
www.notion.so so one throttle covers every tenant.

THREE TRAPS, all measured 2026-08-13:

 1. A BARE ROOT NEVER EXPOSES A PAGE ID.  https://<sub>.notion.site/ returns a
    ~19,148-byte JS shell with no "pageId" for every tenant, live or dead.
    Only a real path (a slug or Title-<32hex>) carries one.  Discovery must
    therefore yield full paths — a subdomain list is useless on its own.

 2. TWO URL GRAMMARS COEXIST.  `/Some-Title-<32hex>` carries the id inline;
    `/sdr` does not and must be resolved from the HTML.  The corpus holds both
    and the bare-slug form is invisible to any keyword-filtered CDX sweep.

 3. A NOTION DATABASE IS NOT A JOB BOARD.  reachcapital.notion.site is the
    single largest notion host in all_jobs_master.csv (340 rows) and it is a
    VC READING LIST: collection name "Our Reading Syllabus", title property
    "Publication", rows "Hechinger Report" / "Stratechery" / "HolonIQ".
    Getro labelled them jobs.  board_gate() exists to reject that shape; a
    pipe that trusts the host imports thousands more like it.

Rate limits: 4 workers at 2.0 rps produced 6x HTTP 429 over 518s, so the
default sits below that.  A 429 never fails a board — it pauses globally and
widens the interval — and the interval DECAYS back afterwards.  Both halves
matter: an early-ratcheting throttle with no decay is measurably worse than a
conservative one, because starting at 3.0 rps pinned the interval at its 1.0s
floor and halved sweep throughput (21 vs 39 candidates/min) for the rest of
the run.
"""
import json
import os
import re
import threading
import time

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
API = "https://www.notion.so/api/v3/%s"
SITE = "https://%s.notion.site/%s"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36")
TIMEOUT = 60
TRIES = 6

# The "no published page" sentinel.  Measured at 19,148 bytes across several
# tenants; compared loosely because Notion revs its bundle.
SHELL_MAX = 40000
PAGE_ID_RE = re.compile(r'"pageId"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
                        r'-[0-9a-f]{4}-[0-9a-f]{12})"', re.I)
HEX32 = re.compile(r"^[0-9a-f]{32}$", re.I)
UUID36 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                    r"[0-9a-f]{4}-[0-9a-f]{12}$", re.I)

_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / 1.5]
_base_interval = [1.0 / 1.5]
_pause_until = [0.0]
_ok_streak = [0]
# Clean requests needed before the throttle wins back one penalty step.
DECAY_AFTER = 150


def set_rps(rps):
    with _rate_lock:
        _interval[0] = _base_interval[0] = 1.0 / float(rps)


def throttle():
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[0]:
                wait = _pause_until[0] - now
            else:
                wait = _next_t[0] - now
                if wait <= 0:
                    _next_t[0] = max(_next_t[0] + _interval[0], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def _penalise():
    with _rate_lock:
        _pause_until[0] = time.time() + 20
        _interval[0] = min(_interval[0] * 1.5, 1.0)
        _ok_streak[0] = 0


def _reward():
    """Walk the penalty back after a clean stretch.

    Without this the penalty only ever RATCHETS, so one burst of 429s early
    permanently caps the rest of the run.  Measured: starting at 3.0 rps
    tripped enough 429s to pin the interval at its 1.0s floor, and the sweep
    then ran at 21 candidates/min — HALF the 39/min a conservative 1.5 rps
    start sustained over the same host.  On a multi-hour sweep a throttle
    that cannot recover is worse than one that never overreached."""
    if _interval[0] <= _base_interval[0]:
        return
    with _rate_lock:
        _ok_streak[0] += 1
        if _ok_streak[0] >= DECAY_AFTER:
            _ok_streak[0] = 0
            _interval[0] = max(_base_interval[0], _interval[0] / 1.5)


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json",
                      "Content-Type": "application/json"})
    return s


class Gone(Exception):
    """Settled negative: the page is deleted, private, or never existed."""


def api(s, endpoint, payload):
    """POST an /api/v3 endpoint.  Raises Gone for settled negatives and
    RuntimeError for exhausted retries; 429 is NEVER either of those."""
    url = API % endpoint
    last = ""
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.post(url, data=json.dumps(payload), timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code in (429, 503):
            _penalise()
            last = "HTTP %d" % r.status_code
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 400:
            # ValidationError — a malformed or non-existent id.  Settled.
            raise Gone("400 validation")
        if r.status_code in (401, 403, 404):
            raise Gone("HTTP %d" % r.status_code)
        if r.status_code == 200:
            body = r.text.lstrip()
            if not body or body[0] == "<":
                last = "html-not-json"
            else:
                try:
                    d = json.loads(body)
                except ValueError:
                    last = "bad-json"
                else:
                    # MemcachedCrossCellError (HTTP 500 seen on 0xjob) and
                    # friends arrive as 200-shaped errors on some paths.
                    if isinstance(d, dict) and d.get("isNotionError"):
                        last = d.get("name") or "NotionError"
                    else:
                        _reward()
                        return d
        else:
            last = "HTTP %d" % r.status_code
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last or "unknown")


def load_page_chunk(s, page_id, limit=100):
    return api(s, "loadPageChunk",
               {"pageId": dash(page_id), "limit": limit,
                "cursor": {"stack": []}, "chunkNumber": 0,
                "verticalColumns": False})


def get_record_values(s, table, rec_id):
    d = api(s, "getRecordValues",
            {"requests": [{"table": table, "id": dash(rec_id)}]})
    res = (d.get("results") or [{}])[0]
    return unwrap(res)


def get_public_page_data(s, host):
    """spaceName is the company name; isDeleted is the liveness flag."""
    url = "https://%s.notion.site/api/v3/getPublicPageData" % host
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.post(url, data=json.dumps(
                {"type": "block-space", "name": "page", "spaceDomain": host,
                 "requestedOnPublicDomain": True, "showMoveTo": False,
                 "saveParent": False, "shouldDuplicate": False}),
                timeout=TIMEOUT)
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code in (429, 503):
            _penalise()
            time.sleep(2 * (attempt + 1))
            continue
        try:
            d = r.json()
        except ValueError:
            return {}
        if isinstance(d, dict) and d.get("isNotionError"):
            # These ARE transient: hosts that returned MemcachedCrossCellError
            # on one pass returned a full spaceName on the next.  Retrying is
            # the difference between 15/28 and 26/28 boards getting a real
            # company name.  Give up quietly after TRIES — some tenants
            # genuinely never expose space data.
            time.sleep(2 * (attempt + 1))
            continue
        _reward()
        return d if isinstance(d, dict) else {}
    return {}


# Notion names a personal workspace "<Name>'s Notion"; that suffix is
# workspace furniture, not part of the company name.
_SPACE_SUFFIX_RX = re.compile(
    r"(?:'|’)?s?\s+(notion|workspace|wiki|hq|home|team space)\s*$", re.I)
_HOST_SUFFIX_RX = re.compile(
    r"[-_](careers?|jobs?|hiring|job-?board|recruiting|talent|work)$", re.I)


def clean_company(name):
    n = (name or "").strip()
    prev = None
    while n and n != prev:
        prev = n
        n = _SPACE_SUFFIX_RX.sub("", n).strip()
    return n


# "Careers at Ilara Health" -> "Ilara Health".  This is the only company
# signal for Notion's auto-generated hostnames (achieved-script-700,
# numerous-column-6e5), where the subdomain names nothing at all.
_TITLE_COMPANY_RX = (
    re.compile(r"^(?:careers?|jobs?|open roles?|open positions?|"
               r"opportunities|vacancies|work|hiring)\s+(?:at|with|@)\s+(.+)$",
               re.I),
    re.compile(r"^join\s+(?:the\s+)?(.+?)(?:\s+team)?$", re.I),
    re.compile(r"^(.+?)\s+(?:careers?|jobs?|job board|hiring|"
               r"open roles?|open positions?|is hiring)$", re.I),
)
_TITLE_TAIL_RX = re.compile(r"\s+(and|&)\s+.*$", re.I)


def company_from_title(title):
    t = (title or "").strip().strip("!?.,:;-—–").strip()
    for rx in _TITLE_COMPANY_RX:
        m = rx.match(t)
        if not m:
            continue
        name = _TITLE_TAIL_RX.sub("", m.group(1)).strip(" -—–:,")
        # Guard against a title that is pure furniture ("Careers at our
        # company") or a whole sentence.
        if 1 < len(name) <= 60 and len(name.split()) <= 6 \
                and not JUNK_TITLE_RX.match(name):
            return name
    return ""


# Notion's own auto-generated hostnames ("diamond-swift-896").  These name
# nothing, so where one appears the spaceName is the ONLY company signal and
# must be trusted rather than corroborated.
AUTOGEN_HOST_RX = re.compile(r"^[a-z]+-[a-z]+-[0-9a-z]{3}$", re.I)

# A personal workspace title is not a company.  aliendesign.notion.site
# reports spaceName "Yash repo" — using it would label 16 real jobs with a
# stranger's scratch-workspace name.
PERSONAL_SPACE_RX = re.compile(
    r"\b(repo|repository|personal|private|test|demo|sandbox|scratch|"
    r"untitled|workspace|my\s+notion|second brain|dashboard)\b", re.I)


def _norm(x):
    return re.sub(r"[^a-z0-9]", "", (x or "").lower())


def pick_company(space, host):
    """-> (company, source).

    spaceName is preferred ONLY when it corroborates the host, because Notion
    reports whatever the workspace is called — which may be a person's scratch
    space.  Measured separation (see notion_probe_boards.py): every real pair
    either contains the other or scores >= 0.6, while "Yash repo"/aliendesign
    scores 0.21.  When the host is auto-generated there is nothing to
    corroborate against, so spaceName wins by default."""
    import difflib
    space = clean_company(space)
    hostco = company_from_host(host)
    if space and not PERSONAL_SPACE_RX.search(space):
        if AUTOGEN_HOST_RX.match(host or ""):
            return space, "space"
        a, b = _norm(space), _norm(host)
        if a and b and (a in b or b in a
                        or difflib.SequenceMatcher(None, a, b).ratio() >= 0.6):
            return space, "space"
    if hostco:
        return hostco, "host"
    # Auto-generated host and no usable space name: the title is all that is
    # left, and if that fails too the row is dropped rather than guessed.
    return "", ""


def company_from_host(host):
    """Fallback when the tenant exposes no space data.  The subdomain is the
    company's own chosen identifier, so this is a derivation, not a guess —
    but it is recorded as company_src=host so precision stays visible."""
    h = _HOST_SUFFIX_RX.sub("", (host or "").strip().lower())
    if not h or re.fullmatch(r"[a-z]+-[a-z]+-[0-9a-f]{3}", h):
        # Notion's auto-generated hostnames (diamond-swift-896) name nothing.
        return ""
    parts = [p for p in re.split(r"[-_]+", h) if p]
    return " ".join(p if p.isupper() else p.capitalize() for p in parts)


def resolve_slug(s, host, path):
    """Bare-slug URL -> page id, from the HTML.  Returns None when the tenant
    serves the bare JS shell, which is the 'no published page' signal."""
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(SITE % (host, path.lstrip("/")), timeout=TIMEOUT,
                      headers={"Accept": "text/html"})
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code in (429, 503):
            _penalise()
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code != 200:
            return None
        _reward()
        m = PAGE_ID_RE.search(r.text)
        return m.group(1).lower() if m else None
    return None


# ---- record helpers --------------------------------------------------------
def dash(i):
    """32-hex -> canonical uuid.  Accepts either form."""
    i = (i or "").strip().lower().replace("-", "")
    if len(i) != 32:
        return i
    return "%s-%s-%s-%s-%s" % (i[:8], i[8:12], i[12:16], i[16:20], i[20:])


def undash(i):
    return (i or "").replace("-", "").lower()


def unwrap(rec):
    """Records arrive as {"value": {...}} or {"value": {"value": {...}}}
    depending on endpoint and API version."""
    v = (rec or {}).get("value", rec) or {}
    if isinstance(v, dict) and "value" in v and isinstance(v["value"], dict):
        v = v["value"]
    return v if isinstance(v, dict) else {}


def blocks(record_map):
    for bid, bl in (record_map.get("block") or {}).items():
        yield bid, unwrap(bl)


def rich_text(prop):
    """Notion property value -> plain string.

    Segments are [text, [annotations...]].  A date arrives as
    ["‣", [["d", {"start_date": "..."}]]] and a user/relation as
    ["‣", [["u", id]]] — the marker itself must never leak into a title."""
    if not isinstance(prop, list):
        return ""
    out = []
    for seg in prop:
        if not isinstance(seg, list) or not seg:
            continue
        t = seg[0]
        if not isinstance(t, str):
            continue
        if t == "‣":
            ann = seg[1] if len(seg) > 1 and isinstance(seg[1], list) else []
            for a in ann:
                if isinstance(a, list) and len(a) > 1 and a[0] == "d" \
                        and isinstance(a[1], dict):
                    out.append(a[1].get("start_date") or "")
            continue
        out.append(t)
    return " ".join(x for x in "".join(out).split() if x).strip()


def collection_views(record_map):
    """[(collection_id, view_id, space_id)] for every database on the page."""
    found = []
    for bid, v in blocks(record_map):
        if v.get("type") in ("collection_view", "collection_view_page") \
                and v.get("collection_id"):
            views = v.get("view_ids") or []
            found.append((v["collection_id"], views[0] if views else None,
                          v.get("space_id") or ""))
    return found


# Tier B cap.  A careers page has a handful of child job pages; anything
# larger is a wiki and must not be imported as jobs.
MAX_CHILD_PAGES = 60


def child_pages(record_map, page_id):
    """[(block_id, title)] for child sub-pages that plausibly are jobs.

    parent_table is the discriminator: a DATABASE ROW has
    parent_table == "collection" and is handled by queryCollection; a real
    child sub-page has parent_table == "block".  Mixing the two would emit
    every database row twice."""
    root = dash(page_id)
    order = {}
    self_v = unwrap((record_map.get("block") or {}).get(root, {}))
    for i, cid in enumerate(self_v.get("content") or []):
        order[dash(cid)] = i
    out = []
    for bid, v in blocks(record_map):
        if dash(bid) == root or v.get("type") != "page":
            continue
        if v.get("parent_table") != "block":
            continue
        if undash(v.get("parent_id") or "") != undash(root):
            continue
        if v.get("alive") is False:
            continue
        t = rich_text((v.get("properties") or {}).get("title"))
        if not t or is_junk_title(t):
            continue
        out.append((dash(bid), t, order.get(dash(bid), 9999)))
    out.sort(key=lambda x: x[2])
    return [(b, t) for b, t, _ in out[:MAX_CHILD_PAGES]]


def space_id_of(record_map):
    for bid, bl in (record_map.get("block") or {}).items():
        sid = (bl or {}).get("spaceId") or unwrap(bl).get("space_id")
        if sid:
            return sid
    return ""


def collection_schema(record_map):
    for cid, c in (record_map.get("collection") or {}).items():
        v = unwrap(c)
        return cid, rich_text(v.get("name")), (v.get("schema") or {})
    return "", "", {}


# ---- schema mapping --------------------------------------------------------
# Schema KEYS are per-tenant gibberish ("MIn~", "stOP", "\TU?") and sometimes
# UUIDs; only "title" is stable.  Everything else is matched on the human NAME.
FIELD_PATTERNS = (
    ("apply_url", r"apply|application|link\b|url\b|job post|posting"),
    ("location", r"location|where|city|country|region|based|office|geo"),
    ("employment_type", r"job type|employment|contract type|type of contract|"
                        r"\btype\b|commitment|schedule"),
    ("department", r"department|team|function|discipline|area|category|org"),
    ("posted", r"posted|date|opened|published|created|live since"),
    # Only a genuine employer NAME.  "Company Stage" / "Company Size" are
    # attributes OF a company, not the company itself — mapping those here
    # would label every acework row with its stage ("Start-up").
    ("company", r"^\s*(company|employer|organi[sz]ation|startup|client|firm)"
                r"(\s*name)?\s*$"),
    ("remote", r"remote|work mode|workplace|onsite|on-site|hybrid"),
    ("salary", r"salary|compensation|comp\b|pay|rate|range"),
    ("seniority", r"seniority|level|experience|years"),
)
_FIELD_RX = [(f, re.compile(p, re.I)) for f, p in FIELD_PATTERNS]


def map_schema(schema):
    """{canonical_field: schema_key}.  First match wins so an early, more
    specific pattern is not overwritten by a later loose one."""
    out = {}
    for key, spec in (schema or {}).items():
        name = (spec or {}).get("name") or ""
        stype = (spec or {}).get("type") or ""
        if key == "title":
            out.setdefault("title", "title")
            continue
        for field, rx in _FIELD_RX:
            if field in out:
                continue
            if rx.search(name):
                # a url-typed column is the strongest apply signal there is
                if field == "apply_url" and stype not in ("url", "text",
                                                          "rich_text"):
                    continue
                out[field] = key
                break
    return out


# ---- the job-board gate ----------------------------------------------------
# Collections that are emphatically not job boards.  Matched against the
# collection name AND the name of its title property, which is where
# reachcapital's "Publication" gives itself away.
NOT_A_BOARD_RX = re.compile(
    r"\b(publication|reading|syllabus|resource|article|book|podcast|"
    r"newsletter|blog|press|event|conference|tool|stack|glossary|faq|"
    r"template|directory|portfolio compan|investor|fund|lp\b|deal|"
    r"contact|people|member|mentor|advisor|speaker|course|curriculum|"
    r"reading list|link)\b", re.I)

# Collection names that disqualify a board OUTRIGHT, whatever the schema says.
# NOT_A_BOARD_RX below yields to a job-ish property; these do not, because a
# Notion database with a Location column can still be anything.  Every entry
# is a collection that shipped non-jobs into the master on the first full run:
#   "Goals Database"                -> OKRs  ("Refactor at least two modules")
#   "EDOCODE Articles"              -> blog posts (Advent Calendar entries)
#   "Use Tags to Filter Spotlights" -> student spotlights (people)
#   "ARCHIVE"                       -> real titles, but a retired board
HARD_NOT_A_BOARD_RX = re.compile(
    r"\b(archived?|goals?|okrs?|tasks?|todos?|backlog|roadmap|changelog|"
    r"articles?|blog|advent|spotlights?|students?|alumni|attendance|roster|"
    r"inventory|syllabus|publications?|newsletter|reading list|"
    r"meeting notes?|agenda|glossary|wishlist|bookmarks?)\b", re.I)

# Board names that advertise MANY employers.  These are real jobs but they are
# not the host's own, so without a per-row employer column they are
# unattributable and get dropped rather than mislabelled:
#   "100+ AI Startup Roles"          -> a recruiter's list, 101 rows
#   "Artswrker Suggested Side Jobs"  -> gigs at AMDA, NYC Elite Gymnastics...
AGGREGATOR_BOARD_RX = re.compile(
    r"\d{2,}\s*\+?\s*\S*\s*(roles|jobs|openings|positions)\b|"
    r"\bsuggested\b.*\b(jobs?|gigs?|roles?)\b|"
    r"\bside (jobs?|gigs?)\b|"
    r"\b(startup|curated|external|partner|portfolio) (roles|jobs)\b|"
    r"\bopportunities at\b|\broles at (startups|companies)\b", re.I)

# A property name that means this really is a job board.
JOB_PROP_RX = re.compile(
    r"role|position|title|location|department|team|job type|employment|"
    r"salary|apply|seniority|hiring|vacanc|opening", re.I)

# Titles that are page furniture, never a job.
JUNK_TITLE_RX = re.compile(
    r"^(about( us)?|our (values|culture|team|story|mission|process)|"
    r"benefits|perks|how we work|why join|life at|faq|contact|"
    r"open (roles|positions)|careers?|jobs?|hiring|we're hiring|"
    r"job board|application process|interview process|"
    r"pitch yourself|general application|open application|"
    r"for opportunities beyond our open roles|don'?t see|other|misc|"
    r"template|untitled|new page|readme|overview|introduction|index)\b",
    re.I)

# A property holding the actual employer NAME.  Anchored and closed so
# "Company Stage" and "Company Size" cannot match — they describe a company,
# they are not one.
COMPANY_NAME_RX = re.compile(
    r"^\s*(company|employer|organi[sz]ation|startup|client|firm)"
    r"(\s*name)?\s*$", re.I)

# A property that betrays a board describing OTHER companies.  Deliberately
# looser than COMPANY_NAME_RX: "Company Stage" is exactly the tell that
# acework (a recruiting agency) is not posting its own jobs.
MULTI_EMPLOYER_RX = re.compile(
    r"^\s*(company|employer|organi[sz]ation|startup|portfolio|client|firm)\b",
    re.I)


# A general "this reads like a job title" test.  Both tiers need it: a child
# sub-page could be anything ("Q1 Planning"), and a DATABASE can just as
# easily hold people or FAQ entries.  is_junk_title() only removes KNOWN
# furniture, so it cannot catch either.  Measured role-word rates:
#     allspice   100%   aliendesign 94%   altcarbon 73%    <- real boards
#     abhishekkaushal 0% (rows are PEOPLE: "Danny Sapio")
#     juro            0% (rows are an FAQ: "Location", "Flexibility")
# The gap is wide enough that MIN_ROLE_RATE can sit safely in the middle.
ROLE_RX = re.compile(
    r"\b(engineer|engineering|developer|dev|programmer|architect|designer|"
    r"design|scientist|researcher|analyst|manager|director|lead|head|chief|"
    r"officer|president|vp|founder|specialist|coordinator|associate|"
    r"assistant|consultant|advisor|administrator|admin|accountant|"
    r"controller|recruiter|marketer|writer|editor|copywriter|strategist|"
    r"producer|planner|buyer|technician|operator|mechanic|nurse|physician|"
    r"therapist|teacher|instructor|trainer|counsel|attorney|paralegal|"
    r"salesperson|representative|rep|executive|agent|clerk|steward|"
    r"supervisor|foreman|apprentice|intern|internship|fellow|"
    r"ambassador|evangelist|advocate|generalist|partner|principal|staff|"
    r"scrum master|product owner|sre|devops|qa|sdr|bdr|ae\b|cto|ceo|cfo|coo|"
    r"cmo|cpo|swe|pm\b|tpm|ux|ui|"
    # Non-English roles.  Notion boards are heavily international and an
    # English-only test silently drops whole countries: buserbrasil's "Vagas"
    # board (38 real jobs — "Analista de Qualidade", "Assistente de
    # Qualidade") scored 16% and maadsn's French board ("Responsable de la
    # flotte", "Chauffeur-Livreur") scored 29%.  Both were being thrown away.
    r"analista|assistente|asistente|agente|gerente|diretor|"
    r"engenheiro|ingeniero|ingegnere|ingenieur|ing[ée]nieur|"
    r"desenvolvedor|desarrollador|sviluppatore|entwickler|"
    r"estagi[aá]rio|est[aá]gio|practicante|stagiaire|praktikant|"
    r"coordenador|coordinador|especialista|consultor|conselheiro|"
    r"t[ée]cnico|techniker|technicien|auxiliar|operador|motorista|"
    r"respons[aá]vel|responsable|responsabile|gestionnaire|"
    r"commercial|livreur|chauffeur|vendedor|verk[aä]ufer|atendente|"
    r"recrutador|mitarbeiter|leiter|berater|addetto|redacteur|"
    r"r[ée]dacteur|conseiller|encargado|jefe|analyste|charg[ée])\b", re.I)


# Sits between the measured 0% (people, FAQ) and 73% (the weakest real board).
MIN_ROLE_RATE = 0.30


def looks_like_role(t):
    return bool(ROLE_RX.search(t or ""))


def is_junk_title(t):
    t = (t or "").strip()
    if len(t) < 3 or len(t) > 140:
        return True
    return bool(JUNK_TITLE_RX.match(t))


def board_gate(coll_name, schema, sample_titles):
    """Decide whether a collection is a job board.

    Returns (ok, reason).  This is the reachcapital guard — see the module
    docstring.  A collection passes only on positive evidence, never by
    default, because the failure mode (importing a reading list as jobs) is
    silent and lands straight in the triage queue."""
    title_prop = ((schema or {}).get("title") or {}).get("name") or ""
    names = [((spec or {}).get("name") or "")
             for spec in (schema or {}).values()]
    has_job_prop = any(JOB_PROP_RX.search(n) for n in names if n)

    # Unconditional rejections first — a job-ish property cannot rescue these.
    if HARD_NOT_A_BOARD_RX.search(coll_name or ""):
        return False, "collection name %r (hard)" % (coll_name or "")[:40]
    if HARD_NOT_A_BOARD_RX.search(title_prop):
        return False, "title property %r (hard)" % title_prop[:40]

    if NOT_A_BOARD_RX.search(coll_name or "") and not has_job_prop:
        return False, "collection name %r" % (coll_name or "")[:40]
    if NOT_A_BOARD_RX.search(title_prop) and not has_job_prop:
        return False, "title property %r" % title_prop[:40]

    good = [t for t in sample_titles if t and not is_junk_title(t)]
    if not good:
        return False, "no non-junk titles"

    # THE ROWS THEMSELVES MUST READ AS ROLES.  A job-ish property is NOT
    # sufficient on its own: abhishekkaushal's "Self-taught careers" table has
    # a Location column and rows that are people ("Danny Sapio"), and juro's
    # job page embeds an FAQ table whose rows are "Location"/"Flexibility".
    # Both cleared every schema check and would have shipped people and FAQ
    # entries into the corpus as jobs.
    roles = [t for t in good if looks_like_role(t)]
    rate = len(roles) / float(len(good))
    if rate < MIN_ROLE_RATE:
        return False, "%d/%d titles read as roles (%.0f%%)" % (
            len(roles), len(good), 100 * rate)
    return True, ""


def employer_props(schema):
    """(looks_multi_employer, company_name_key).

    A board serving many employers must take company per-row; emitting
    spaceName would label every reachcapital row 'Reach Capital'.  The two
    checks are separate on purpose: acework advertises other firms' jobs and
    exposes "Company Stage" but NO employer-name column, so it is detectably
    multi-employer AND unattributable — which is exactly the aggregator_drop
    case, not a keep-with-wrong-name case."""
    name_key, multi = "", False
    for key, spec in (schema or {}).items():
        name = (spec or {}).get("name") or ""
        if not name:
            continue
        if MULTI_EMPLOYER_RX.match(name):
            multi = True
        if not name_key and COMPANY_NAME_RX.match(name):
            name_key = key
    return (multi or bool(name_key)), name_key
