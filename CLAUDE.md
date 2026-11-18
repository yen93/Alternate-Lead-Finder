# CLAUDE.md — Alternate Lead Finder

Weekly automation: for each closed/unresponsive lead in `closed_sequence_threads`,
find DIFFERENT people at the same company who hold one of the **target POC roles**
(Apify actor `deepmine~linkedin-company-employees` — names + job titles only, **no
emails**), record them in `alternate_leads` (`email` is NULL), and email Liv a preview of
the leads found that run. (It NO LONGER appends to the Google Sheet — removed in v60.) The
whole thing is one Supabase edge function
plus two pg_cron jobs. (Hunter.io, the old Apify email-finder and Lusha are NOT used
any more — see "Providers history".)

## Where things live (two Supabase projects — don't mix them up)
- **`aivitcomiywiysrfwqxt`** (MAGTestProject) — ALL business tables, the edge
  function `alternate-lead-finder`, and its cron (`alternate-lead-finder-weekly`,
  jobid 3, `55 22 * * 0`). Do the data/function work here.
- **`uwsncuowtzydylfabikj`** (n8n-production) — n8n's own backend DB only. The
  pg_cron job that fires the n8n webhook (`n8n-closed-threads-processing`, jobid 33,
  `0 22 * * 0`) lives here (matches the pattern in
  `../n8n_google_cloud_run_deployment/schedule_webhooks.sql`).
- GCP project **`claudegwscli-502400`** — Cloud Run n8n + Secret Manager.

## The function
- Source: `supabase/functions/alternate-lead-finder/index.ts` (Deno, service role,
  `verify_jwt=false`). Real work runs in `EdgeRuntime.waitUntil` and it returns
  202 immediately — so a normal invocation returns `{"status":"started"}`, not the
  result. Use `?sync=1` to await and see the summary/error.
- Debug params: `?sync=1`, `?limit=N` (can only LOWER the 20-row cap), `?inspect=1`
  (check Sheets/Gmail/AC access, no send), `?append=1` (DISABLED — returns
  `{"status":"disabled"}`, no-op), `?probe=<company name or domain>` (alias `?aprobe=`; raw DeepMine
  search, no writes — shows how many people came back, which match the target roles
  and what the picker would choose; the actor searches by company NAME so a bare
  domain may match poorly), `?orgprobe=<domain>` (raw OpenAI org-resolver check,
  optional `&hint=<text>`; no writes), `?fixorg=1` (maintenance: sync the sheet's Org
  column to the DB, matching rows by **email** — leads with no email are skipped),
  `?to=<email>` (redirect the notification email — ALWAYS use this when testing so Liv
  isn't emailed; honoured by `?sync` runs).
- Deploy: **no supabase CLI is installed.** Deploy via the Management API multipart
  endpoint (reads the file directly, avoids JSON-escaping the backslashes in the
  code):
  ```bash
  TOKEN=$(tr -d ' \t\r\n' < supabase_access_token.txt)
  curl -s -X POST "https://api.supabase.com/v1/projects/aivitcomiywiysrfwqxt/functions/deploy?slug=alternate-lead-finder" \
    -H "Authorization: Bearer $TOKEN" \
    -F 'metadata={"entrypoint_path":"index.ts","name":"alternate-lead-finder","verify_jwt":false};type=application/json' \
    -F 'file=@supabase/functions/alternate-lead-finder/index.ts;type=application/typescript'
  ```
  (The Supabase MCP `deploy_edge_function` also works but needs the content inline,
  which is error-prone with the regex backslashes.) There is no Deno locally; a quick
  syntax check is `npx -y -p typescript tsc --noEmit --skipLibCheck --target es2022
  --module esnext --moduleResolution bundler --strict <file>` (ignore TS2304/2307/2580/
  2584/7006 — they're missing Deno/esm types).

## How a run works
1. **Queue:** up to **20 rows** (`MAX_ROWS_PER_RUN`, hard cap) from `closed_sequence_threads`
   where `email` is not null AND `is_processed` is null/false AND
   `failed_apify_search` is NOT true, oldest `id` first.
2. **Pre-pass (parallel):** group the rows by email domain → one company per distinct
   domain. Resolve each company's NAME (DeepMine searches by name): soc-med context org →
   the ORIGINAL lead's org by email (`loadOrgByEmail`) → OpenAI web-search resolver →
   bare domain. Then run **one DeepMine search per company, all in parallel**
   (`DEEPMINE_CONCURRENCY`, default 20; `maxResults` 100 people each).
3. **Per row:** pick up to 5 people whose **job title** matches the target roles; insert
   into `alternate_leads` (`email` NULL); set the row's flags (below).
4. **Notify:** email Liv a preview (up to 100 rows) of ONLY the `alternate_leads` ids inserted
   this run (`notifyNewLeads`); no email if nothing was inserted. No sheet writes.
   `is_added_to_sheet` is no longer maintained (new rows stay `false`).

- **Target roles** (word-boundary regexes on the job TITLE only — `TARGET_ROLE_PATTERNS`):
  Conference/Events Manager, Marketing Manager, Communication Manager, Learning &
  Development Manager, People & Culture Manager, Executive Assistant. HR terms and
  "personal assistant" are intentionally NOT included. **No fallback:** if nobody matches,
  nothing is inserted (an earlier "closest person" fallback was tried and removed).
  Headlines are NOT matched — they produced false hits ("Retired", taglines).
- **Flags on `closed_sequence_threads`:**
  - lead inserted → `is_processed = true`, `failed_apify_search = false`
  - search ran but no matching lead (nobody in the target roles, or everyone already
    known/excluded) → `failed_apify_search = true`, `is_processed` stays null. These rows
    are **skipped by the queue** so they don't hog the 20-row window; to retry one set
    `failed_apify_search = null`.
  - If the company has **no target-role people at all**, ALL other not-yet-processed rows
    for the same email domain are also flagged `true` (`markCompanyFailed`) so the company
    isn't re-searched. (If target people existed but were all excluded for this row — e.g.
    its own original lead — only this row is flagged.)
  - provider error / no token / per-run cap / wall-clock skip → flag stays NULL, row retries.
- **Dedup (no emails):** by normalised name + org, and name + email-domain (for older leads
  that have emails). The original unresponsive lead is excluded by matching their name
  against the original email's local part. ActiveCampaign dedup is NOT possible without
  an email, so people already in AC can be surfaced.
- **Throughput:** a DeepMine search is ~35–40s and the actor works through companies
  SERIALLY, so a single multi-company call (20 × ~40s) can't fit the isolate — that's why
  searches are parallel single-company calls. Measured full run: 18 companies in 78s,
  90s total. Searches only start while elapsed < `WALL_CLOCK_MS − APPEND_RESERVE_MS −
  APIFY_CALL_RESERVE_MS`; skipped ones retry next run.

## Gotchas learned the hard way
- **Leads have NO email.** `alternate_leads.email` is NULL for DeepMine leads (the table
  allows it). Consequences: (1) **verify the n8n `batch-mailer-…alternate…` workflows skip
  null emails** before relying on this (not yet checked); (2) the queries
  `… where email not in (select email from alternate_leads)` return ZERO rows once any
  email is NULL (`NOT IN` + NULL) — use `NOT EXISTS` or `where email is not null` in the
  subquery; (3) `?fixorg=1` matches by email so it skips these; (4) the sheet (no longer
  written by the function) contains the 69 email-less leads appended before v60.
- **`alternate_leads.sheet_remark`** (text; added 2026-11-18): ONE-TIME copy of the sheet's
  Remark column ("Send intro" / "5A" / null), matched by email. NOT kept in sync — the
  function never writes it. 44 Send intro, 30 5A, 70 null.
- **DeepMine actor facts:** input `companies[]` (names or LinkedIn URLs), `maxResults`,
  `enrichProfiles` (we pass false), `title` (a single EXACT phrase — useless: "Marketing
  Manager" returned 0). Output per person: `firstName`, `lastName`, `jobTitle`, `headline`,
  `searchQuery` (= the company string sent), `currentCompany`, `companyMatch`. Coverage is
  partial (cookie-free public search): ~0–3 target-role people per company, none for some.
  `jobTitle` is sometimes a tagline. It matches by company NAME: the OpenAI resolver
  prompt asks for the common/LinkedIn trading name ("AMCA Australia"), because the legal
  name ("Air Conditioning & Mechanical Contractors' Association of Australia Limited")
  returned 0 people.
- **Wall-clock guard (the notify step must always run).** The isolate is hard-killed at
  ~150s and the email step runs ONCE, after the searches/loop. Env knobs:
  `WALL_CLOCK_MS` (150000), `APPEND_RESERVE_MS` (15000), `APIFY_CALL_RESERVE_MS` (75000),
  `DEEPMINE_TIMEOUT_S` (90, per-search), `DEEPMINE_CONCURRENCY` (20), `DEEPMINE_MAX_RESULTS`
  (100), `APIFY_MAX_DOMAINS` (80, per-run cap), `APIFY_ACTOR`. If the run is killed before
  the email, the leads are still in `alternate_leads` (no resend path exists).
- **Only mark a row processed when a real lead was inserted** (see flags above) — no row
  is burned without a lead.
- **`closed_sequence_threads` has no `source_type_id`** (it's on the follow-up
  tables). Pull carried context from the source table, not the closed table.
- **`alternate_leads.org` must never be null.** Resolution order (pre-pass, per company):
  soc-med context org → carry over the ORIGINAL lead's org by email (`loadOrgByEmail`,
  across the cold detail tables `ai_verified_cold_leads`/`manually_found_cold_leads` and
  the soc-med detail tables `ai_scraped_soc_med_leads`/`manually_found_leads`) → **OpenAI
  web-search resolver** → bare domain (absolute last resort, so never null).
- **OpenAI org resolver (`resolveOrgViaOpenAI`):** used when a lead's original email isn't
  in any detail table. Uses the **Responses API with the `web_search` tool** so the model
  actually looks the domain up — a plain non-browsing model (`gpt-4o-mini`) HALLUCINATED
  (called hia.com.au the "Health Insurance Association"; it's the Housing Industry
  Association), so a web-search model is required. Model via `OPENAI_MODEL` (default
  `gpt-4o`; must support `web_search_preview`). Cached per domain per run. Test with
  `?orgprobe=<domain>`.
- **pg_cron is UTC-only.** Mon PHT = Sunday UTC (`dow 0`, not 1). 6:00 AM PHT =
  `0 22 * * 0`; 6:55 AM PHT = `55 22 * * 0`.
- **Secrets:** the Management API `GET /secrets` returns only hashed digests, so you
  can't read values back. Set them with `POST /secrets`. The edge function reads its
  keys from **Supabase function secrets** (APIFY_API_TOKEN, OPENAI_API_KEY,
  GOOGLE_SERVICE_ACCOUNT_JSON, AC_API_URL, AC_API_TOKEN, ...), which are SEPARATE from
  GCP Secret Manager. `OPENAI_API_KEY` lives in GCP Secret Manager (`OPENAI_API_KEY`,
  project `claudegwscli-502400`) and was copied into the Supabase function secrets from
  there. (`HUNTER_IO_API_KEY` is still set but unused.)
- **Google auth:** SA = `insta-drive-uploader@claudegwscli-502400.iam.gserviceaccount.com`
  (client id 112006793372164249856). Sheets needs the sheet shared with the SA;
  Gmail send needs Workspace domain-wide delegation for scope `.../auth/gmail.send`
  (sends as `NOTIFY_FROM`, default julienne@). Both are already set up.

## Providers history (why only DeepMine)
- **Hunter.io** — FREE plan (~50 domain searches/month, `limit<=10`). On 2026-11-15 every
  call returned **401 "No user found for the API key supplied"** (key invalid/revoked), so
  the run inserted 0 leads and the sheet's last row stayed 2026-11-08. Removed from the
  function; the key in Supabase secrets is unused.
- **Apify `scrapersdelight~decision-maker-email-finder`** — the old reactive fallback
  (~1 min/domain, only ~1–2 domains/run). Replaced by DeepMine.
- **Lusha** — key works on the **V3** API only (V2 returns HTTP 410; V2 sunset 2026-11-30):
  base `https://api.lusha.com`, header `api_key`, `GET /v3/account/usage` (free),
  `POST /v3/contacts/prospecting` (≈1 credit/25 results; email reveal 1 credit each). Free
  plan had 44 credits. Tested once (returned the CEO of amca.com.au); NOT wired in.
- Apify DeepMine pricing is ~$0.79/1,000 employees returned.

## Secrets / files
- `hunter_io_api_key.txt`, `supabase_access_token.txt`, `apify_api_token.txt`,
  `lusha_api_key.txt` — **live secrets, gitignored**, never commit. Hunter/Supabase/Apify
  also in GCP Secret Manager (`hunter-io-api-key`, `supabase-access-token`,
  `apify-api-token`); Lusha is not stored there.
- `manual_invoke.txt` — cmd.exe curl commands (contains only the public anon key).
- `alternate-lead-finder.html` — iMAG end-user guide (still describes the Hunter flow —
  needs updating).

## The wider weekly flow
Mon **6:00 AM PHT** the n8n "Closed Threads Processing" webhook populates
`closed_sequence_threads` (via the `process_unresponsive_lead(thread_id)` SQL function,
which ONLY inserts into `closed_sequence_threads` — it never touches `alternate_leads`);
Mon **6:55 AM PHT** this function processes up to 20 of them.
