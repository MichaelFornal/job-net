#!/usr/bin/env python3
"""swath_vocab.py — the eleven job swaths, as patterns that actually match postings.

One definition, imported by everything. `picks_build.py` states the rule this follows:
"Imported, never copied: a second transcription of the AI vocabulary is a second thing to
keep in sync, and these two have already drifted once." The Phase 1 measurement, the
Phase 3 lane builder, and the selftest all read this module.

profile.json's `preferences.target_titles_<swath>` carries the human-readable list — what
he is targeting, in his words. This file carries the regex that finds it. Both exist for
the reason picks_build.py:67-70 gives: the profile says "Tokenization / RWA Consultant"
and no posting is ever titled that.

Two defects are carried forward from `picks` as design rules, not as bugs to rediscover:

  1. **A domain word is not a job title.** Scoring bare desk nouns ("capital markets",
     "payments", "ecommerce") as title fit put a Legal Counsel, an Administrative
     Assistant and a 2027 Summer Analyst at the top of `picks`. Every pattern here is
     anchored to a ROLE word. The desk noun earns its points in domain scoring instead.
  2. **Blocklists match titles, not the employer's product.** `skills` describes what the
     company sells, so killing on it drops a full-stack engineer at a biotech and a
     business-development lead at a robotics company. `picks_build.killed_by` judges the
     two separately and exempts commercial titles; this module reuses that function
     rather than restating the rule.
"""
import re

# --------------------------------------------------------------------------- swaths
# key -> (code, label, resume file stem, title pattern)
#
# Patterns are deliberately narrow at the role word and wide at the qualifier. Widening
# the role word is how a lane fills with jobs he cannot do; widening the qualifier only
# costs precision inside a pool that is already the right shape.

_F1 = re.compile(r"""(?ix)
    \bgtm\s+engineer | go[\s-]to[\s-]market\s+engineer |
    revenue\s+(?:operations|systems|engineer|enablement|technology) | \brevops\b |
    sales\s+(?:operations|systems|enablement)\s*(?:\w+\s+)?
        (?:manager|lead|analyst|engineer|specialist|director|associate|partner)? |
    marketing\s+(?:operations|systems|engineer|technolog) | \bmarketing\s+ops\b |
    business\s+systems\s+(?:analyst|engineer|manager|administrator|lead|architect) |
    (?:salesforce|hubspot|netsuite|marketo)\s*(?:\w+\s+)?
        (?:administrator|developer|engineer|architect|analyst|consultant|manager) |
    \bcrm\s+(?:administrator|developer|engineer|manager|analyst|architect) |
    growth\s+(?:engineer|operations) | lifecycle\s+(?:marketing\s+)?operations |
    deal\s+desk | \bgtm\s+(?:systems|operations|strategy)
""")

# `advisor` is deliberately NOT in the solutions alternation. "Solutions Advisor" is a
# retail-banking branch title, not a presales one: 336 rows carried it and 331 of them were
# Bank of America and Merrill Lynch ("Financial Solutions Advisor - Sharon Financial
# Center", "Credit Solutions Advisor - Raynham"). Including it put 12% garbage into the
# largest and best-paying swath — the same failure as scoring a desk noun as a title, one
# level down: here the ROLE word itself is ambiguous across industries.
_F2 = re.compile(r"""(?ix)
    solutions?\s+(?:architect|engineer|consultant|specialist|principal) |
    \bsales\s+engineer | pre[\s-]?sales | \bpresales\b |
    technical\s+(?:account\s+manager|consultant|advisor|architect|solutions) |
    \btam\b |
    (?:partner|customer|field|deployment|forward\s+deployed)\s+engineer |
    professional\s+services\s+(?:engineer|consultant|architect|manager) |
    implementation\s+(?:architect|consultant|engineer|specialist|manager) |
    customer\s+solutions? | value\s+engineer | \bsolution\s+design
""")

_F3 = re.compile(r"""(?ix)
    \bai\s+(?:trainer|tutor|expert|evaluator|reviewer|annotator|teacher|coach) |
    (?:subject\s+matter|domain)\s+expert |
    \bsme\b\s*[,-]? |
    (?:model|llm|ai|output|response)\s+(?:evaluation|evaluator|quality|grading|rating) |
    \brlhf\b | red[\s-]?team(?:er|ing)? |
    prompt\s+(?:engineer|specialist|writer) |
    (?:freelance|contract|expert)\s+(?:ai|llm|data)\s+ |
    human\s+data | data\s+(?:annotation|labeling|labelling)\s+(?:specialist|expert|lead) |
    expert\s+(?:contributor|network|reviewer) | \bai\s+quality\b
""")

_F4 = re.compile(r"""(?ix)
    chief\s+of\s+staff |
    business\s+operations?\s*(?:\w+\s+)?
        (?:manager|lead|director|analyst|associate|partner|specialist|principal)? |
    \bbizops\b |
    (?:strategy|strategic)\s+(?:and|&|\+)\s+operations |
    (?:business|corporate|company)\s+strategy\s*(?:\w+\s+)?
        (?:manager|lead|director|analyst|associate)? |
    founder'?s?\s+associate | special\s+projects\s*(?:\w+\s+)?
        (?:manager|lead|director|associate)? |
    operations?\s+strategy | \bstrategy\s+manager\b
""")

# The anti-title is enforced in `killed_by`; the pattern still avoids reaching for it.
_F5 = re.compile(r"""(?ix)
    analytics\s+engineer | data\s+analytics\s+engineer |
    business\s+intelligence\s*(?:\w+\s+)?
        (?:engineer|analyst|developer|manager|lead|architect|specialist)? |
    \bbi\s+(?:engineer|analyst|developer|manager|lead|architect|consultant) |
    (?:data|reporting|insights|business|marketing|product|revenue)\s+analyst |
    analytics\s+(?:manager|lead|director|consultant|specialist) |
    (?:tableau|power\s*bi|looker|sigma)\s*(?:\w+\s+)?
        (?:developer|engineer|analyst|consultant|architect) |
    decision\s+(?:support|intelligence)\s+(?:analyst|engineer)
""")

_F6 = re.compile(r"""(?ix)
    automation\s+(?:engineer|developer|architect|specialist|consultant|lead|manager) |
    integrations?\s+(?:engineer|developer|architect|specialist|consultant|lead|manager) |
    (?:systems?|software|application|api|data)\s+integration\s+
        (?:engineer|developer|architect|specialist|analyst|manager) |
    \brpa\s+(?:developer|engineer|consultant|analyst|architect) |
    robotic\s+process\s+automation |
    (?:workflow|process|business\s+process)\s+automation |
    middleware\s+(?:engineer|developer|architect) | \bipaas\b |
    (?:zapier|workato|mulesoft|boomi|tray\.io|n8n)\s*(?:\w+\s+)?
        (?:developer|engineer|consultant|architect|specialist)
""")

_F7 = re.compile(r"""(?ix)
    e[\s-]?commerce\s*(?:\w+\s+)?
        (?:manager|director|lead|analyst|specialist|engineer|developer|associate|
           coordinator|strategist|architect)? |
    \bdtc\b | direct[\s-]to[\s-]consumer |
    digital\s+commerce | online\s+(?:store|retail)\s+(?:manager|lead|specialist) |
    (?:shopify|bigcommerce|magento|woocommerce)\s*(?:\w+\s+)?
        (?:developer|engineer|specialist|manager|consultant|architect) |
    marketplace\s+(?:manager|specialist|operations|lead|analyst|strategist) |
    (?:catalog|merchandis\w+|storefront)\s+(?:manager|specialist|lead|operations|analyst)
""")

_F8 = re.compile(r"""(?ix)
    \bai\s+(?:enablement|transformation|adoption|strategy|readiness|literacy|
              evangelist|champion|program|change|adviser|advisor) |
    (?:generative\s+ai|gen\s?ai)\s*(?:\w+\s+)?
        (?:lead|manager|director|strategist|consultant|architect|program|head) |
    (?:head|director|lead|manager)\s+of\s+(?:ai|artificial\s+intelligence) |
    ai\s+center\s+of\s+excellence | \bai\s+coe\b |
    (?:digital|business)\s+transformation\s*(?:\w+\s+)?
        (?:lead|manager|director|consultant|architect)? |
    innovation\s+(?:lead|manager|director|consultant|strategist)
""")

_F9 = re.compile(r"""(?ix)
    payments?\s*(?:\w+\s+)?
        (?:engineer|manager|analyst|operations|specialist|lead|director|product|
           architect|associate|consultant) |
    billing\s*(?:\w+\s+)?
        (?:engineer|manager|analyst|operations|specialist|lead|director|product|
           architect|associate) |
    (?:revenue|monetization|subscription)\s+(?:engineer|systems|platform|operations) |
    fraud\s*(?:\w+\s+)?
        (?:analyst|manager|engineer|investigator|operations|specialist|lead|
           director|strategist) |
    (?:chargeback|disputes?|payout|settlement)\s+(?:analyst|manager|specialist|
                                                    operations|lead) |
    (?:payment|billing|fintech)\s+(?:operations|infrastructure|platform)
""")

# `crypto`, `web3`, `defi` and bare `blockchain` are deliberately NOT triggers, and
# `engineer` is not a role word here. F10's framing rule is non-negotiable
# (`profile.json._schema.lanes.finance`, and the draft header): **institutional finance,
# never "crypto/blockchain dev"** — the reader is a bank or an asset manager. With those
# terms in, the lane pulled "Backend / Blockchain Engineer – Java/Spring", "Lead
# Cryptography Engineer" and "EntryLevel Crypto Market Specialist", which is the exact
# document F10 refuses to be. Blockchain survives only next to an institutional role word.
_F10 = re.compile(r"""(?ix)
    (?:tokeni[sz]ation|digital\s+assets?|\brwa\b|real[\s-]world\s+assets?)\s*
        (?:\w+\s+)?
        (?:consultant|strategist|lead|architect|manager|analyst|associate|
           specialist|product|director|principal|advisor|head) |
    blockchain\s*(?:\w+\s+)?
        (?:consultant|strategist|product\s+manager|director|principal|advisor|head) |
    (?:associate|analyst|consultant|strategist|lead|director|vp|head|principal)\s*,?\s*
        (?:\w+\s+)?(?:tokeni[sz]ation|digital\s+assets?|capital\s+markets|
                      private\s+markets|fund\s+finance)
""")

_F11 = re.compile(r"""(?ix)
    trust\s*(?:and|&)\s*safety | \bt&s\b |
    content\s+(?:moderation|moderator|review|policy|safety) |
    (?:platform|community|marketplace)\s+(?:integrity|safety|operations|policy) |
    (?:abuse|spam|harm)\s+(?:prevention|operations|analyst|manager|specialist) |
    risk\s+operations | \brisk\s+ops\b |
    policy\s+(?:enforcement|operations|specialist|analyst)\s* |
    safety\s+(?:operations|analyst|manager|specialist|policy)
""")

# code -> (label, resume stem, pattern, one-line angle)
SWATHS = {
    "gtm":       ("F1",  "Revenue Systems / GTM Engineering",
                  "F1-revenue-systems-gtm", _F1),
    "presales":  ("F2",  "Solutions Architect / Presales",
                  "F2-solutions-architect-presales", _F2),
    "aiexpert":  ("F3",  "AI Expert & Evaluation",
                  "F3-ai-expert-profile", _F3),
    "bizops":    ("F4",  "BizOps / Chief of Staff",
                  "F4-bizops-chief-of-staff", _F4),
    "analytics": ("F5",  "Analytics Engineer / BI",
                  "F5-analytics-engineer-bi", _F5),
    "automation": ("F6", "Automation & Integration Engineer",
                   "F6-automation-integration", _F6),
    "ecom":      ("F7",  "Ecommerce / DTC Operator",
                  "F7-ecommerce-dtc-operator", _F7),
    "aienable":  ("F8",  "AI Enablement / Transformation",
                  "F8-ai-enablement-transformation", _F8),
    "fintech":   ("F9",  "Fintech Ops: Billing / Payments / Fraud",
                  "F9-fintech-ops-payments", _F9),
    "rwa":       ("F10", "Digital Assets / Tokenization / RWA",
                  "F10-digital-assets-rwa", _F10),
    "trust":     ("F11", "Trust & Safety / Risk Ops",
                  "F11-trust-safety-risk-ops", _F11),
}

# Researched, measured, and deliberately NOT built — the user declined it. Kept here so
# the research doc can price the door he closed, and so nothing silently reopens it.
DECLINED = {
    "support": ("Support Engineering / TAM / Customer Engineering",
                re.compile(r"""(?ix)
                    (?:support|success)\s+engineer |
                    technical\s+support\s+(?:engineer|specialist|manager) |
                    customer\s+(?:success|support)\s+(?:manager|engineer|lead) |
                    \bcsm\b | escalation\s+(?:engineer|manager)
                """)),
}


# --------------------------------------------------------------------------- domains
# Matched against `skills` + `company_industry`, NEVER against the title. This is the
# other half of the rule at the top of this file: a desk noun describes the employer's
# product, so it earns domain points on a row that ALREADY looks like the right job, and
# it never buys a row its way in on its own. `picks_build.title_fit` enforces the same
# separation for the AI/finance lane.
_DOM = {
    "gtm": r"salesforce|hubspot|marketo|outreach\.io|salesloft|clay\b|apollo\.io|"
           r"\bcrm\b|revenue\s+operations|pipeline|demand\s+gen|\babm\b|sales\s+engagement",
    "presales": r"pre[\s-]?sales|proof\s+of\s+concept|\bpoc\b|discovery\s+call|"
                r"solution(?:s)?\s+design|technical\s+win|rfp\b|customer\s+facing",
    "aiexpert": r"\brlhf\b|human\s+feedback|annotation|labeling|labelling|red\s?team|"
                r"model\s+evaluation|benchmark|rubric|training\s+data|expert\s+network",
    "bizops": r"business\s+operations|chief\s+of\s+staff|strategic\s+planning|\bokr\b|"
              r"board\s+report|cross[\s-]functional|process\s+improvement",
    "analytics": r"power\s?bi|tableau|looker|dbt\b|snowflake|redshift|bigquery|"
                 r"semantic\s+layer|dimensional\s+model|\betl\b|data\s+warehouse|sql\b",
    "automation": r"zapier|workato|mulesoft|boomi|tray\.io|\bn8n\b|make\.com|"
                  r"\bipaas\b|webhook|\bapi\s+integration|middleware|orchestration",
    "ecom": r"shopify|bigcommerce|magento|woocommerce|amazon\s+seller|etsy|"
            r"tiktok\s+shop|print[\s-]on[\s-]demand|fulfillment|\bdtc\b|merchandis",
    "aienable": r"change\s+management|enablement|adoption|upskill|center\s+of\s+excellence|"
                r"governance|responsible\s+ai|\bcoe\b|training\s+program",
    "fintech": r"stripe|adyen|braintree|payment|billing|invoicing|chargeback|"
               r"interchange|\bpci\b|settlement|reconciliation|fraud|\bkyc\b|\baml\b",
    "rwa": r"tokeni[sz]ation|\brwa\b|real[\s-]world\s+assets?|digital\s+assets?|"
           r"custody|\bdefi\b|smart\s+contract|\berc[\s-]?\d+|blockchain|"
           r"private\s+markets|fund\s+finance|capital\s+markets",
    "trust": r"content\s+moderation|trust\s*(?:and|&)\s*safety|abuse|integrity|"
             r"enforcement|appeals?\b|policy\s+violation|\bcsam\b|harmful\s+content|"
             r"rate\s+limit|bot\s+detection",
}
DOMAINS = {k: re.compile(v, re.I | re.X) for k, v in _DOM.items()}


def domain_of(key):
    return DOMAINS[key]


def code_of(key):
    return SWATHS[key][0]


def label_of(key):
    return SWATHS[key][1]


def stem_of(key):
    return SWATHS[key][2]


def pattern_of(key):
    return SWATHS[key][3]


def matches(key, title):
    """Does this title belong to this swath's pool? Title only — never `skills`."""
    return bool(SWATHS[key][3].search(title or ""))


def all_matching(title):
    """Every swath whose pool this title falls in. Overlap is expected and is resolved
    by disjoint assignment in lanes_build.py, not here."""
    return [k for k in SWATHS if matches(k, title)]
