#!/usr/bin/env python3
"""
build_sources.py — write contract_sources.csv, the master directory of every
contract / temp / freelance job source evaluated for this pipeline, with how
to reach it and whether it's harvested here.

Columns:
  source            name
  category          family of source
  access            open_api | rss | key_required | login_required |
                    scrape | closed_apply | partner_only
  contract_signal   the field/param that isolates contingent roles
  status            harvested | partial | documented | needs_key | gated
  endpoint          base URL / API entry
  notes             specifics, gotchas, volume
"""
import csv
import os

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(BASE, "contract_sources.csv")

COLS = ["source", "category", "access", "contract_signal", "status",
        "endpoint", "notes"]

ROWS = [
    # ---------------- staffing ATS platforms (agency back-ends) -----------
    ["Bullhorn (OSCP public REST)", "staffing_ats", "open_api",
     "employmentType in {Contract,Contract To Hire,Temporary}", "partial",
     "https://public-rest{swimlane}.bullhornstaffing.com/rest-services/{corpToken}/query/JobBoardPost",
     "Dominant US staffing ATS. Unauthenticated query/JobBoardPost with "
     "where=isOpen=true AND isDeleted=false AND isPublic=1. Each agency = one "
     "6-char corpToken on a numbered swimlane. Discovery is the bottleneck: "
     "tokens live in each career portal's Angular bundle, not in Wayback. "
     "Harvested the tokens discoverable via CDX/urlscan; full coverage needs "
     "a portal-domain crawl + per-site token extraction."],
    ["JobDiva", "staffing_ats", "scrape", "employment type on portal",
     "documented", "https://www1.jobdiva.com/portal/",
     "Large IT-staffing ATS. Public portals per client id; no clean JSON "
     "API, portal pages are server-rendered. Feasible via per-portal scrape."],
    ["Ceipal", "staffing_ats", "scrape", "job type field", "documented",
     "https://{client}.ceipal.com/",
     "IT staffing ATS; public career pages per client subdomain."],
    ["Crelate", "staffing_ats", "open_api", "jobType", "documented",
     "https://app.crelate.com/api/public/jobs/{portalId}",
     "Recruiting CRM; public job board API per portal id. Needs portal-id "
     "directory (harvestable via urlscan/Wayback)."],
    ["Loxo", "staffing_ats", "open_api", "job type", "documented",
     "https://app.loxo.co/api/... (agency slug)",
     "Recruiting platform with public agency career pages + JSON API per "
     "agency."],
    ["Avionte", "staffing_ats", "scrape", "employment type", "documented",
     "https://jobs.myavionte.com/{company}",
     "Light-industrial + clerical staffing ATS; public boards per company."],
    ["TempWorks", "staffing_ats", "scrape", "job type", "documented",
     "https://webcenter.tempworks.com/",
     "Staffing ATS with WebCenter/JobBoard portals (light industrial)."],
    ["Vincere", "staffing_ats", "scrape", "job type", "documented",
     "https://{agency}.vincere.io/careers",
     "Recruitment CRM popular in APAC/UK; public career pages per agency."],
    ["Zoho Recruit", "staffing_ats", "scrape", "job type", "documented",
     "https://{agency}.zohorecruit.com/jobs/Careers",
     "Career sites per agency subdomain; server-rendered."],

    # ---------------- direct staffing agencies (own boards) ---------------
    ["TEKsystems", "agency", "open_api", "type=Contractor", "harvested",
     "POST https://careers.teksystems.com/widgets",
     "Allegis IT-staffing giant on Phenom; ~1.2k jobs, ~91% contractor."],
    ["Actalent", "agency", "open_api", "type=Contractor", "harvested",
     "POST https://careers.actalentservices.com/widgets",
     "Allegis engineering/sciences staffing on Phenom; ~5k jobs, contract-"
     "heavy."],
    ["Aston Carter", "agency", "open_api", "type=Contractor", "harvested",
     "POST https://careers.astoncarter.com/widgets",
     "Allegis professional staffing on Phenom; ~1.1k jobs."],
    ["Aerotek / Allegis brands", "agency", "scrape", "type=Contractor",
     "documented", "https://www.aerotek.com/en/job-search",
     "Same Phenom family; widget host differs per brand (probe the site's "
     "network calls for its /widgets or search API host)."],
    ["Robert Half", "agency", "key_required", "employmentType facet",
     "documented",
     "POST https://www.roberthalf.com/bin/jobSearchServlet (Cloudflare)",
     "Contract/temp specialist. JSON servlet exists but is Cloudflare-"
     "protected; needs browser-context headers."],
    ["Insight Global", "agency", "scrape", "employment type", "documented",
     "https://jobs.insightglobal.com/find-a-job/",
     "Major US contract staffing; ASP.NET board, search API behind it."],
    ["Kforce", "agency", "scrape", "type", "documented",
     "https://www.kforce.com/find-work/search-jobs/",
     "Tech/finance contract staffing; API behind the site."],
    ["Apex Systems", "agency", "scrape", "employment type", "documented",
     "https://itcareers.apexsystems.com/ListJobs",
     "IT contract staffing (ASGN); dedicated job host."],
    ["Randstad US", "agency", "open_api", "job type", "documented",
     "https://www.randstadusa.com/api/search/jobs",
     "Live JSON search API (returns 400 without required params; supply "
     "q/page/pagesize)."],
    ["CyberCoders", "agency", "open_api", "employment type", "documented",
     "https://www.cybercoders.com/ (search API)",
     "Permanent + contract tech; page carries JSON-LD; search API present."],
    ["Motion Recruitment", "agency", "scrape", "Contract filter",
     "documented", "https://motionrecruitment.com/tech-jobs",
     "Tech contract staffing (Next.js); has a contract employment filter."],
    ["Kelly Services", "agency", "scrape", "job type", "documented",
     "https://www.kellyservices.us/us/careers/search/",
     "Temp/contract staffing; careers host differs from marketing site."],
    ["Adecco US", "agency", "scrape", "job type", "documented",
     "https://www.adeccousa.com/jobs/",
     "Temp/contract staffing."],
    ["The Judge Group", "agency", "scrape", "job type", "documented",
     "https://www.judge.com/jobs/", "IT/healthcare contract staffing."],
    ["Beacon Hill", "agency", "scrape", "job type", "documented",
     "https://www.beaconhillstaffing.com/find-a-job",
     "Professional staffing (temp + contract)."],

    # ---------------- freelance marketplaces ------------------------------
    ["Freelancer.com", "marketplace", "open_api", "all projects freelance",
     "harvested",
     "https://www.freelancer.com/api/projects/0.1/projects/active/",
     "Fully open projects API; ~6k active projects (fixed + hourly). "
     "Global, noisy, but pure contract."],
    ["Braintrust", "marketplace", "open_api", "all roles contract",
     "harvested", "https://app.usebraintrust.com/api/jobs/",
     "User-owned talent network; open jobs API (~150 live). W2/1099 "
     "contract, mostly remote, hourly USD budgets."],
    ["Upwork", "marketplace", "key_required", "all freelance", "needs_key",
     "https://www.upwork.com/developer (GraphQL API)",
     "Largest freelance marketplace; API requires an approved OAuth app / "
     "key. No unauthenticated project feed."],
    ["Fiverr", "marketplace", "closed_apply", "gig-based", "gated",
     "https://www.fiverr.com/",
     "Seller-posts-gigs model; no buyer-side project API."],
    ["Guru", "marketplace", "scrape", "all freelance", "documented",
     "https://www.guru.com/d/jobs/", "Freelance job board; HTML only."],
    ["PeoplePerHour", "marketplace", "scrape", "all freelance", "documented",
     "https://www.peopleperhour.com/freelance-jobs",
     "Freelance marketplace; React SPA, internal API behind it."],
    ["Twine", "marketplace", "scrape", "all freelance", "documented",
     "https://www.twine.net/jobs",
     "Creative/AI freelance marketplace; SPA."],
    ["Contra", "marketplace", "scrape", "all freelance/independent",
     "documented", "https://contra.com/",
     "Commission-free independent marketplace; SPA/internal API."],
    ["freelancermap", "marketplace", "login_required", "all projects",
     "gated", "https://www.freelancermap.com/",
     "IT/engineering project board (DACH-heavy); project list requires "
     "login."],
    ["Free-Work", "marketplace", "scrape", "contractor/freelance filter",
     "documented", "https://www.free-work.com/en-gb/tech-it/jobs",
     "UK/FR tech contractor board (Nuxt); explicit contractor + freelance "
     "categories."],
    ["Gulp", "marketplace", "scrape", "project type", "documented",
     "https://www.gulp.de/", "DACH IT/engineering project marketplace."],
    ["JobServe", "marketplace", "scrape", "Contract job type", "documented",
     "https://www.jobserve.com/us/en/Job-Search/",
     "One of the oldest contract job boards (IT/tech, US+UK)."],
    ["Dice", "aggregator", "key_required",
     "filters.employmentType=CONTRACTS", "needs_key",
     "https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search",
     "Tech board with a strong CONTRACTS filter; API needs an x-api-key "
     "extracted from the site bundle (403 without it)."],

    # ---------------- vetted talent networks (application-gated) ----------
    ["Toptal", "talent_network", "closed_apply", "all contract", "gated",
     "https://www.toptal.com/",
     "Top-3% vetted network (dev/design/finance/PM). No public job feed; "
     "talent applies to join, roles are matched."],
    ["A.Team", "talent_network", "closed_apply", "all contract/fractional",
     "gated", "https://www.a.team/",
     "Product/eng/design 'teams' network; application-gated, mission-based "
     "contract work."],
    ["Gun.io", "talent_network", "closed_apply", "all contract", "gated",
     "https://gun.io/", "Vetted developer contract network."],
    ["YunoJuno", "talent_network", "login_required", "all contract", "gated",
     "https://www.yunojuno.com/",
     "UK freelancer marketplace/contractor management; login-gated."],
    ["Malt", "talent_network", "login_required", "all freelance", "gated",
     "https://www.malt.com/",
     "Large EU freelance marketplace; profile + login gated."],
    ["Catalant", "talent_network", "closed_apply", "consulting/independent",
     "gated", "https://gocatalant.com/",
     "Independent management-consulting projects; enterprise-side."],
    ["Business Talent Group (BTG)", "talent_network", "closed_apply",
     "independent consulting", "gated", "https://www.businesstalentgroup.com/",
     "High-end independent consultant marketplace (Heidrick-owned)."],
    ["Braintrust (join)", "talent_network", "open_api", "all contract",
     "harvested", "https://app.usebraintrust.com/api/jobs/",
     "Also a vetted network but jobs feed is open — see marketplace row."],
    ["Fiverr Pro / Certified", "talent_network", "closed_apply", "gig",
     "gated", "https://pro.fiverr.com/", "Vetted tier of Fiverr."],
    ["Lemon.io", "talent_network", "closed_apply", "dev contract", "gated",
     "https://lemon.io/", "Vetted startup-focused developer network."],
    ["Andela", "talent_network", "closed_apply", "dev contract", "gated",
     "https://www.andela.com/", "Global engineering talent network."],
    ["Turing", "talent_network", "closed_apply", "dev contract", "gated",
     "https://www.turing.com/", "Remote developer matching network."],

    # ---------------- fractional / executive-on-demand --------------------
    ["Fractional Jobs", "fractional", "scrape", "all fractional",
     "documented", "https://www.fractionaljobs.io/",
     "Curated fractional exec/operator board (Webflow CMS)."],
    ["Go Fractional", "fractional", "scrape", "all fractional", "documented",
     "https://www.gofractional.com/jobs",
     "Fractional exec marketplace; Cloudflare-challenged."],
    ["Continuum", "fractional", "closed_apply", "fractional exec", "gated",
     "https://www.continuum.club/", "Fractional executive network."],
    ["Bolster", "fractional", "login_required", "fractional/advisory",
     "gated", "https://bolster.com/",
     "Executive marketplace (CEO/CxO fractional, board, advisory)."],
    ["Chief of Staff Network", "fractional", "closed_apply", "fractional",
     "gated", "https://www.chiefofstaff.network/", "Niche fractional CoS."],

    # ---------------- expert networks (short-term consulting) -------------
    ["GLG", "expert_network", "closed_apply", "consultation/advisory",
     "gated", "https://glginsights.com/",
     "Largest expert network; short paid consultations + longer advisory."],
    ["AlphaSights", "expert_network", "closed_apply", "advisory", "gated",
     "https://www.alphasights.com/", "Expert network; advisory engagements."],
    ["Guidepoint", "expert_network", "closed_apply", "advisory", "gated",
     "https://www.guidepoint.com/", "Expert network."],
    ["Third Bridge", "expert_network", "closed_apply", "advisory", "gated",
     "https://www.thirdbridge.com/", "Expert network."],

    # ---------------- remote job boards -----------------------------------
    ["We Work Remotely", "remote_board", "rss", "title (Contract)",
     "harvested", "https://weworkremotely.com/remote-jobs.rss",
     "Big remote board; RSS has no type field, contract inferred from title."],
    ["Remotive", "remote_board", "open_api", "job_type in {contract,freelance}",
     "harvested", "https://remotive.com/api/remote-jobs",
     "Open API but returns only a recent slice (~40 jobs) unauthenticated."],
    ["RemoteOK", "remote_board", "open_api", "tags contain contract",
     "harvested", "https://remoteok.com/api",
     "Open API (~100 recent). Attribution required by their ToS."],
    ["Himalayas", "remote_board", "open_api", "employmentType=Contractor",
     "partial", "https://himalayas.app/jobs/api",
     "101k remote jobs; API hard-caps limit=20 and ignores type filters, so "
     "harvested a bounded recent window."],
    ["Jobicy", "remote_board", "open_api", "jobType contract/freelance",
     "harvested", "https://jobicy.com/api/v2/remote-jobs",
     "Open remote-jobs API v2."],
    ["Working Nomads", "remote_board", "open_api", "title/category",
     "harvested", "https://www.workingnomads.com/api/exposed_jobs/",
     "Open feed; no explicit type field."],
    ["Arbeitnow", "remote_board", "open_api", "job_types contain contract",
     "harvested", "https://www.arbeitnow.com/api/job-board-api",
     "Open, paginated; EU-heavy, has job_types array."],
    ["ProBlogger", "remote_board", "rss", "freelance writing", "partial",
     "https://problogger.com/jobs/feed/",
     "Freelance-writing gigs; feed intermittently empty."],
    ["Jobspresso", "remote_board", "scrape", "job type", "documented",
     "https://jobspresso.co/remote-work/", "Curated remote board."],
    ["Wellfound (AngelList)", "remote_board", "scrape", "job type", "gated",
     "https://wellfound.com/jobs",
     "Startup jobs incl. contract; DataDome-protected, needs browser."],
    ["The Muse", "aggregator", "open_api", "title (weak)", "documented",
     "https://www.themuse.com/api/public/jobs",
     "412k jobs, open API, but no reliable contract type field (type = "
     "internal/external); title-filter only."],

    # ---------------- VC / accelerator portfolio boards -------------------
    ["Getro networks (768 boards)", "vc_board", "open_api", "title (no type)",
     "harvested", "https://api.getro.com/api/v2/collections/{id}/search/jobs",
     "VC/accelerator portfolio boards; records lack an employment-type "
     "field so contract detection is title-only. Re-mined from the existing "
     "2.1M-row dump."],
    ["Lever boards (2,842 boards)", "vc_board", "open_api",
     "commitment=Contract/Temp/Freelance", "harvested",
     "https://api.lever.co/v0/postings/{slug}",
     "Public Lever boards; commitment field gives real contract signal. "
     "Re-mined from the existing 76k-posting dump."],
    ["Ashby boards", "ats", "open_api", "employmentType=Contract/Temporary",
     "harvested", "https://api.ashbyhq.com/posting-api/job-board/{slug}",
     "First-party ATS; clean employmentType. Slugs harvested from the Getro "
     "apply-URL dump (2,034 boards)."],
    ["SmartRecruiters boards", "ats", "open_api",
     "typeOfEmployment=Contract/Temporary", "harvested",
     "https://api.smartrecruiters.com/v1/companies/{slug}/postings",
     "Open postings API; 336 boards from the Getro dump."],
    ["Recruitee boards", "ats", "open_api",
     "employment_type_code=contract/freelance/temporary", "harvested",
     "https://{slug}.recruitee.com/api/offers/", "153 boards from Getro."],
    ["Breezy boards", "ats", "open_api", "type=Contract/Temporary/Seasonal",
     "harvested", "https://{slug}.breezy.hr/json", "381 boards from Getro."],
    ["BambooHR boards", "ats", "open_api",
     "employmentStatusLabel=Contract/Temporary/Freelance", "harvested",
     "https://{slug}.bamboohr.com/careers/list", "768 boards from Getro."],
    ["Greenhouse boards", "ats", "open_api", "title (no type field)",
     "documented", "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
     "2,313 boards from Getro; no employment-type field so title-only, low "
     "contract density on startup boards. Not fetched (title-mined via "
     "Getro instead)."],
    ["Workable boards", "ats", "open_api", "employment_type=Contract",
     "documented", "https://apply.workable.com/api/v3/accounts/{slug}/jobs",
     "566 boards from Getro; POST search API with employment_type."],
    ["Teamtailor / JazzHR", "ats", "key_required", "job type",
     "documented", "various",
     "Teamtailor needs an API key for the partner API (though a public "
     "jsonfeed exists at {slug}.teamtailor.com/jobs.json); JazzHR's public "
     "feed needs a key and the board pages are HTML-only."],
    ["Personio boards", "ats", "open_api", "employment_type + schedule",
     "harvested", "https://{slug}.jobs.personio.de/search.json",
     "Verified open 2026-07-28 (the earlier 'key_required' note was wrong): "
     "search.json returns the whole board as JSON with no headers and "
     "carries a job id. Rate-limits hard — 55% HTTP 429 unthrottled — so the "
     "fetcher runs at 2 rps. DACH-heavy and German-language; rows are "
     "country=DE so US-remote cuts exclude them. No posting date, so "
     "personio_posted_ledger.json freezes a first-seen date. "
     "Pipe: personio_discover_boards.py -> personio_fetch_jobs.py "
     "(doubles as probe) -> personio_build_sheet.py."],
    ["Rippling ATS boards", "ats", "open_api", "title (no type field)",
     "harvested",
     "https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs",
     "Unauthenticated, whole board in one call, no pagination. NOT "
     "ats.rippling.com, which is Cloudflare-fronted HTML. Payload carries "
     "only uuid/name/department/workLocation/url — no type, no salary, no "
     "date. ~1/3 of live boards are momentarily empty and must not be "
     "marked dead. Pipe: rippling_discover_boards.py -> "
     "rippling_probe_boards.py -> rippling_fetch_jobs.py -> "
     "rippling_build_sheet.py."],
    ["Y Combinator — Work at a Startup", "startup_board", "html_scrape",
     "type field (Full-time/Contract/Intern)", "harvested",
     "https://www.ycombinator.com/companies/{slug}/jobs (Accept: text/html)",
     "Job list is inline in the Inertia data-page attribute; no login. "
     "Accept: application/json returns 404 on the same URL. Slugs from "
     "yc-oss.github.io/api/companies/hiring.json. WaaS postings are not "
     "syndicated to any ATS, so most are net-new. createdAt is a Rails "
     "relative string, frozen at first sight in yc_posted_ledger.json. "
     "Pipe: yc_discover_boards.py -> yc_fetch_jobs.py -> yc_build_sheet.py."],
    ["Dover careers pages", "ats", "open_api",
     "compensation.employment_type", "harvested",
     "https://app.dover.com/api/v1/careers-page/{uuid}/jobs",
     "Sold almost exclusively to pre-seed->Series-B startups, the segment "
     "the corpus is thinnest on. The list endpoint has no date or salary, so "
     "a per-job detail call to /api/v1/inbound/application-portal-job/{id} "
     "supplies real ISO dates and structured compensation. WARNING: "
     "app.dover.com answers HTTP 200 with a 3,988-byte HTML SPA shell on any "
     "unrecognised path, so responses must be content-type checked. Pipe: "
     "dover_discover_boards.py -> dover_probe_boards.py -> "
     "dover_fetch_jobs.py -> dover_build_sheet.py."],
    ["Notion public careers pages (notion.site)", "ats", "open_api",
     "per-tenant schema property", "harvested",
     "https://www.notion.so/api/v3/loadPageChunk + /queryCollection",
     "DIY careers pages published from Notion. No key, no cookie. Two URL "
     "grammars coexist: /Title-{32hex} carries the page id, /sdr does not and "
     "is resolved from the HTML ('pageId'); a BARE ROOT never exposes one, so "
     "discovery must yield full paths, not subdomains. Discovery needs BOTH "
     "channels — CDX misses 77% of the hosts already in the corpus because "
     "bare slugs contain no job keyword, and the corpus misses the 2,873 CDX "
     "finds. WARNING: A NOTION DATABASE IS NOT A JOB BOARD. "
     "reachcapital.notion.site is the largest notion host in "
     "all_jobs_master.csv (340 rows) and is a VC reading list ('Our Reading "
     "Syllabus', title property 'Publication'); abhishekkaushal's rows are "
     "people and juro's are FAQ entries. Boards must clear a gate on "
     "collection name, schema, and a >=30% role-word rate. Multi-employer "
     "boards emit company per-row or are dropped. Row created_time gives real "
     "posted dates, frozen in notion_posted_ledger.json. Pipe: "
     "notion_discover_boards.py -> notion_probe_boards.py -> "
     "notion_fetch_jobs.py -> notion_build_sheet.py."],
    ["Wayback CDX slug sweep", "ats", "open_api", "n/a (discovery)",
     "harvested",
     "https://web.archive.org/cdx/search/cdx?url={apex}&fl=urlkey&collapse=",
     "Shared board-slug discovery across 9 ATS apexes (slug_common.py + "
     "slug_discover_boards.py), feeding both the new families and "
     "ats_fetch.py via the wayback_ats_slugs.json sidecar. Validates the "
     "RESPONSE BODY, because CDX serves nginx 504/503 HTML pages with HTTP "
     "200 and the older per-family scripts recorded those as completed "
     "pages. Collapse depth is measured per apex, not assumed: at the "
     "default depth the rippling apex lost 45% of its slugs."],

    # ---------------- aggregators needing a free key ----------------------
    ["Adzuna", "aggregator", "key_required", "contract category/keyword",
     "needs_key", "https://api.adzuna.com/v1/api/jobs/{country}/search/1",
     "Free app_id+app_key; supports contract filtering; good US coverage."],
    ["Jooble", "aggregator", "key_required", "type=Contract", "needs_key",
     "https://jooble.org/api/{key}", "Free API key on request."],
    ["Careerjet", "aggregator", "key_required", "contract_type=c", "needs_key",
     "https://www.careerjet.com/partners/api/",
     "Free affiliate key; contract_type=c isolates contract."],
    ["Reed.co.uk", "aggregator", "key_required", "contract flag", "needs_key",
     "https://www.reed.co.uk/api/", "Free key; UK; contract boolean."],
    ["USAJOBS", "aggregator", "key_required",
     "PositionSchedule/Appointment (temp/term)", "needs_key",
     "https://data.usajobs.gov/api/search",
     "Free key (email header). US federal temp/term appointments."],
    ["Findwork", "aggregator", "key_required", "employment_type=contract",
     "needs_key", "https://findwork.dev/api/", "Free-tier key."],
    ["Indeed / LinkedIn / ZipRecruiter / Google Jobs", "aggregator",
     "partner_only", "contract filter", "gated", "various",
     "Publisher/partner programs closed or deprecated; no open contract "
     "feed. Reachable only via their on-site filters."],
]


def rows():
    return [dict(zip(COLS, r)) for r in ROWS]


def main():
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        for r in ROWS:
            w.writerow(dict(zip(COLS, r)))
    print(f"wrote {OUT}: {len(ROWS)} sources", flush=True)


if __name__ == "__main__":
    main()
