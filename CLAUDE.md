# CLAUDE.md — Alternate Lead Finder

Weekly automation: for each closed/unresponsive lead in `closed_sequence_threads`,
find a different contact at the same company (Hunter.io), dedup against
ActiveCampaign, record in `alternate_leads`, append to the sales Google Sheet, and
email Liv a preview. The whole thing is one Supabase edge function plus two
pg_cron jobs.

## Where things live (two Supabase projects — don't mix them up)
- **`aivitcomiywiysrfwqxt`** (MAGTestProject) — ALL business tables, the edge
  function `alternate-lead-finder`, and its cron. Do the data/function work here.
- **`uwsncuowtzydylfabikj`** (n8n-production) — n8n's own backend DB only. The
  pg_cron job that fires the n8n webhook lives here (matches the pattern in
  `../n8n_google_cloud_run_deployment/schedule_webhooks.sql`).
- GCP project **`claudegwscli-502400`** — Cloud Run n8n + Secret Manager.

## The function
- Source: `supabase/functions/alternate-lead-finder/index.ts` (Deno, service role,
  `verify_jwt=false`). Real work runs in `EdgeRuntime.waitUntil` and it returns
  202 immediately — so a normal invocation returns `{"status":"started"}`, not the
  result. Use `?sync=1` to await and see the summary/error.
- Debug params: `?sync=1`, `?limit=N`, `?inspect=1` (check Sheets/Gmail/AC access,
  no send), `?append=1` (only the append+email step, no Hunter cost), `?probe=<domain>`
  (raw Hunter check), `?aprobe=<domain>` (raw Apify fallback check),
  `?orgprobe=<domain>` (raw OpenAI org-resolver check, optional `&hint=<text>`; no writes),
  `?provider=apify` (force the Apify fallback, skipping Hunter — for testing),
  `?fixorg=1` (maintenance: sync the sheet's Org column to the DB, matching rows by
  email — fills blanks/drift, no sends),
  `?to=<email>` (redirect the notification email — use this when testing so Liv
  isn't emailed; now honoured by `?sync` runs too, not just `?append`).
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
  which is error-prone with the regex backslashes.)

## Gotchas learned the hard way
- **Hunter.io is on the FREE plan:** max ~50 domain searches/month AND the `limit`
  param must be `<=10` (higher returns HTTP 400 pagination_error). The code uses
  `limit=10`. One run can only enrich ~50 orgs; a full ~255-row backlog needs a paid
  plan. Over quota, Hunter errors -> the row is left unprocessed (retried), not burned.
- **Apify fallback (Hunter → Apify, reactive):** when Hunter's monthly search
  credits are exhausted it returns HTTP 429 with details "…limit for the number of
  searches per billing period…" (same `too_many_requests` id as a plain rate-limit —
  match on the *details text*, not the id). On that signal the run switches to the
  Apify actor `scrapersdelight~decision-maker-email-finder` (sticky for the rest of
  the run) and maps its records into the Hunter shape. Chosen because it scrapes the
  company's OWN site + LinkedIn (geography-agnostic — it covers `.com.au`, unlike the
  Apollo/US-DB actors which returned 0 for AU domains). Per-run cap `APIFY_MAX_DOMAINS`
  (default 80); env knobs `APIFY_ACTOR`, `APIFY_LIMIT`.
- **Wall-clock guard (append+email must always run).** The isolate is hard-killed at
  ~150s and the append-to-sheet + email step runs ONCE, after the row loop — so a run
  that times out mid-loop inserts leads into the DB but never flushes them to the sheet
  or emails (this actually happened: 2 leads inserted, nothing appended). The Apify
  fallback is ~1 min/domain, so a 20-row Apify run can't finish in 150s. Fix: before
  each *new* provider call (cache misses only), `run()` checks the time budget and, if
  starting one risks the kill, `break`s so the flush still runs; remaining rows stay
  unprocessed and retry. Reserve is provider-aware. Env knobs: `WALL_CLOCK_MS` (150000),
  `APPEND_RESERVE_MS` (15000), `HUNTER_CALL_RESERVE_MS` (8000), `APIFY_CALL_RESERVE_MS`
  (70000). Consequence: while Hunter is exhausted, only ~1–2 Apify domains drain per
  run — the backlog clears slowly until Hunter resets. Recover DB-only leads (inserted
  but not on the sheet, `is_added_to_sheet=false`) with `?append=1`.
- **Only mark a row processed when a real lead was inserted.** `is_processed` is set
  true for a `closed_sequence_threads` row ONLY when ≥1 `alternate_leads` row (with an
  email) was actually inserted for it — for BOTH Hunter and Apify. Provider errors,
  empty results, all-deduped candidates, and no-domain rows all leave `is_processed`
  null/false so the row retries. (Tradeoff: a company with no *new* reachable contact
  is retried every run, re-spending a lookup, until Hunter resets — accepted so no row
  is ever burned without a lead.)
- **`closed_sequence_threads` has no `source_type_id`** (it's on the follow-up
  tables). Pull carried context from the source table, not the closed table.
- **`alternate_leads.org` must never be null.** Resolution order (`run()` per row):
  provider org (Hunter returns one; **the Apify actor does NOT — it has no company
  field**, so on the Apify path org always starts null) → soc-med context org →
  carry over the ORIGINAL lead's org by email (`loadOrgByEmail`, across the cold
  detail tables `ai_verified_cold_leads`/`manually_found_cold_leads` and the soc-med
  detail tables `ai_scraped_soc_med_leads`/`manually_found_leads`) → **OpenAI
  web-search resolver** → bare domain (absolute last resort, so never null).
- **OpenAI org resolver (`resolveOrgViaOpenAI`):** last resort when a lead's original
  email isn't in any detail table (e.g. cold-follow-up rows whose lead was never in
  the cold detail tables — carry-over then finds nothing). Uses the **Responses API
  with the `web_search` tool** so the model actually looks the domain up — a plain
  non-browsing model (`gpt-4o-mini`) HALLUCINATED (called hia.com.au the "Health
  Insurance Association"; it's the Housing Industry Association), so a web-search model
  is required. Model via `OPENAI_MODEL` (default `gpt-4o`; must support
  `web_search_preview`). Cached per domain per run; only fires when org is otherwise
  null AND there's a candidate to insert. Test with `?orgprobe=<domain>`; backfill the
  sheet's Org column with `?fixorg=1`.
- **pg_cron is UTC-only.** Mon PHT = Sunday UTC (`dow 0`, not 1). 6:00 AM PHT =
  `0 22 * * 0`; 6:55 AM PHT = `55 22 * * 0`.
- **Secrets:** the Management API `GET /secrets` returns only hashed digests, so you
  can't read values back. Set them with `POST /secrets`. The edge function reads its
  keys from **Supabase function secrets** (HUNTER_IO_API_KEY, APIFY_API_TOKEN,
  OPENAI_API_KEY, GOOGLE_SERVICE_ACCOUNT_JSON, AC_API_URL, AC_API_TOKEN, ...), which are
  SEPARATE from GCP Secret Manager. `OPENAI_API_KEY` lives in GCP Secret Manager
  (`OPENAI_API_KEY`, project `claudegwscli-502400`) and was copied into the Supabase
  function secrets from there.
- **Google auth:** SA = `insta-drive-uploader@claudegwscli-502400.iam.gserviceaccount.com`
  (client id 112006793372164249856). Sheets needs the sheet shared with the SA;
  Gmail send needs Workspace domain-wide delegation for scope `.../auth/gmail.send`
  (sends as `NOTIFY_FROM`, default julienne@). Both are already set up.

## Secrets / files
- `hunter_io_api_key.txt`, `supabase_access_token.txt`, `apify_api_token.txt` — **live
  secrets, gitignored**, never commit. Also in GCP Secret Manager (`hunter-io-api-key`,
  `supabase-access-token`, `apify-api-token`). (`lusha_api_key.txt` is also gitignored but
  Lusha was evaluated and dropped — not used, not stored in Secret Manager.)
- `manual_invoke.txt` — cmd.exe curl commands (contains only the public anon key).
- `alternate-lead-finder.html` — iMAG end-user guide.

## The wider weekly flow
Mon **6:00 AM PHT** the n8n "Closed Threads Processing" webhook populates
`closed_sequence_threads`; Mon **6:55 AM PHT** this function processes them.
