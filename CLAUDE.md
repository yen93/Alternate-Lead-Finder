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
  no send), `?append=1` (only the append+email step, no Hunter cost), `?probe=<domain>`,
  `?to=<email>` (redirect the notification email — use this when testing so Liv
  isn't emailed).
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
- **Don't mark a row processed on a Hunter failure.** A null Hunter result must
  leave `is_processed` false so it retries; only mark processed when Hunter actually
  responded. (This was a real bug.)
- **`closed_sequence_threads` has no `source_type_id`** (it's on the follow-up
  tables). Pull carried context from the source table, not the closed table.
- **pg_cron is UTC-only.** Mon PHT = Sunday UTC (`dow 0`, not 1). 6:00 AM PHT =
  `0 22 * * 0`; 6:55 AM PHT = `55 22 * * 0`.
- **Secrets:** the Management API `GET /secrets` returns only hashed digests, so you
  can't read values back. Set them with `POST /secrets`. The edge function reads its
  keys from **Supabase function secrets** (HUNTER_IO_API_KEY, GOOGLE_SERVICE_ACCOUNT_JSON,
  AC_API_URL, AC_API_TOKEN, ...), which are SEPARATE from GCP Secret Manager.
- **Google auth:** SA = `insta-drive-uploader@claudegwscli-502400.iam.gserviceaccount.com`
  (client id 112006793372164249856). Sheets needs the sheet shared with the SA;
  Gmail send needs Workspace domain-wide delegation for scope `.../auth/gmail.send`
  (sends as `NOTIFY_FROM`, default julienne@). Both are already set up.

## Secrets / files
- `hunter_io_api_key.txt`, `supabase_access_token.txt` — **live secrets, gitignored**,
  never commit. Also in GCP Secret Manager (`hunter-io-api-key`, `supabase-access-token`).
- `manual_invoke.txt` — cmd.exe curl commands (contains only the public anon key).
- `alternate-lead-finder.html` — iMAG end-user guide.

## The wider weekly flow
Mon **6:00 AM PHT** the n8n "Closed Threads Processing" webhook populates
`closed_sequence_threads`; Mon **6:55 AM PHT** this function processes them.
