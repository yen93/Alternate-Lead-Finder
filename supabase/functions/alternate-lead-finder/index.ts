// alternate-lead-finder
// Weekly (Mon 6:55 AM PHT) automation. For each unresponsive lead in
// closed_sequence_threads, find DIFFERENT people at the same org via the Apify actor
// deepmine/linkedin-company-employees (names + job titles only - NO emails), keep only
// the target POC roles, record them in alternate_leads (email is NULL), mark the
// source row processed, append the new leads to the sales Google Sheet (Sheets API
// via the service account), and email a dynamic preview to Liv (Gmail API via SA
// domain-wide delegation).
//
// Triggered by pg_cron (net.http_post). Kicks the real work into a background
// task and returns 202 immediately so it never hits the request timeout.
// Debug params: ?sync=1, ?limit=N, ?inspect=1, ?probe=<company name or domain>.

import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

// Supabase edge runtime global for background work.
declare const EdgeRuntime: { waitUntil(p: Promise<unknown>): void } | undefined;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const GOOGLE_SA_JSON = Deno.env.get("GOOGLE_SERVICE_ACCOUNT_JSON") ?? "";
const AC_API_URL = (Deno.env.get("AC_API_URL") ?? "").replace(/\/+$/, "");
const AC_API_TOKEN = Deno.env.get("AC_API_TOKEN") ?? "";

// DeepMine (Apify) is the ONLY contact provider. Actor deepmine/linkedin-company-employees
// searches public LinkedIn by company NAME (not domain) and returns people with a job
// title + headline - no emails. We pull a broad list per company and filter to the
// target roles in code (the actor's own `title` filter is a single exact phrase and
// returned ~nothing in testing). API slug uses `~` in the run URL path.
const APIFY_API_TOKEN = Deno.env.get("APIFY_API_TOKEN") ?? "";
const APIFY_ACTOR = Deno.env.get("APIFY_ACTOR") ?? "deepmine~linkedin-company-employees";
const DEEPMINE_MAX_RESULTS = parseInt(Deno.env.get("DEEPMINE_MAX_RESULTS") ?? "100", 10); // people pulled per company
const DEEPMINE_TIMEOUT_S = parseInt(Deno.env.get("DEEPMINE_TIMEOUT_S") ?? "90", 10);
const DEEPMINE_CONCURRENCY = parseInt(Deno.env.get("DEEPMINE_CONCURRENCY") ?? "20", 10); // parallel company searches
const APIFY_MAX_DOMAINS = parseInt(Deno.env.get("APIFY_MAX_DOMAINS") ?? "80", 10);

// Wall-clock guard. The edge isolate is hard-killed at ~150s, and the
// append-to-sheet + email step runs only AFTER the processing loop — so a run that
// times out mid-loop (esp. on the slow Apify fallback, ~1 min/domain) inserts leads
// into the DB but never flushes them to the sheet or emails a preview. We stop
// starting new provider calls before the budget is spent so the flush always runs.
const WALL_CLOCK_MS = parseInt(Deno.env.get("WALL_CLOCK_MS") ?? "150000", 10);
const APPEND_RESERVE_MS = parseInt(Deno.env.get("APPEND_RESERVE_MS") ?? "15000", 10); // headroom for the sheet+email flush
const APIFY_CALL_RESERVE_MS = parseInt(Deno.env.get("APIFY_CALL_RESERVE_MS") ?? "75000", 10); // searches are only started while elapsed < WALL_CLOCK - APPEND_RESERVE - this

// OpenAI org resolver. Last-resort org lookup for alternate_leads.org (which must
// never be null): the Apify actor returns no company name, and some leads have no
// org anywhere in the DB to carry over. When every other source is empty we ask an
// OpenAI model for the company that owns the domain, grounded on the domain + the
// contact's name/title. On any failure/uncertainty we fall back to the bare domain,
// so org is guaranteed non-null.
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY") ?? "";
// Uses the Responses API with the web_search tool, so the model actually looks the
// domain up instead of guessing (a non-browsing model hallucinated e.g. hia.com.au).
// Must be a model that supports web_search_preview.
const OPENAI_MODEL = Deno.env.get("OPENAI_MODEL") ?? "gpt-4o";

// Sales sheet + tab the alternate leads are appended to.
const SPREADSHEET_ID = Deno.env.get("SALES_SHEET_ID") ?? "1svksxHmNBUx9Z20t2kbtHAT1fThcfdCmz5dEp936L7I";
const SHEET_TAB = Deno.env.get("ALT_LEADS_TAB") ?? "alternate_leads_found";
const SHEET_URL = `https://docs.google.com/spreadsheets/d/${SPREADSHEET_ID}/edit`;

// Email notification (Gmail API via SA domain-wide delegation).
const NOTIFY_TO = Deno.env.get("NOTIFY_TO") ?? "liv@myadventuregroup.com.au";
const NOTIFY_FROM = Deno.env.get("NOTIFY_FROM") ?? "julienne@myadventuregroup.com.au";

// Target POC roles, matched (word-boundary regexes) against the person's job TITLE only:
// Conference/Events Manager, Marketing Manager, Communication Manager,
// Learning & Development Manager, People & Culture Manager, Executive Assistant.
const TARGET_ROLE_PATTERNS: RegExp[] = [
  /\bevents?\b/, /\bconferences?\b/,
  /\bmarketing\b/,
  /\bcommunications?\b/,
  /\blearning (&|and) development\b/, /\bl ?& ?d\b/,
  /\bpeople (&|and|\+)? ?culture\b/,
  /\bexecutive assistant\b/,
];
const MAX_TARGET_LEADS = 5; // per org, when target roles are found
const MAX_ROWS_PER_RUN = 20; // hard cap on closed_sequence_threads rows processed per run

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const domainOf = (email: string) => (email.includes("@") ? email.split("@")[1].trim().toLowerCase() : "");

interface Person {
  first_name: string | null;
  last_name: string | null;
  position: string | null; // job title
  headline: string | null;
}
interface PeopleResult { people: Person[] }

// Matches on the job TITLE only. Headlines are free text ("Retired", "Event Coordinator
// with dental background", taglines) and produced false matches in testing.
function isTargetRole(p: Person): boolean {
  return TARGET_ROLE_PATTERNS.some((re) => re.test((p.position ?? "").toLowerCase()));
}

const normName = (s: string | null) => (s ?? "").toLowerCase().replace(/[^a-z\s'-]/g, "").replace(/\s+/g, " ").trim();
const nameKey = (p: Person) => `${normName(p.first_name)} ${normName(p.last_name)}`.trim();
const normOrg = (s: string) => s.toLowerCase().replace(/\s+/g, " ").trim();

// --- DeepMine (Apify) provider ------------------------------------------------
interface RunState { domainsUsed: number }

function deepmineItemsToResult(items: Record<string, unknown>[]): PeopleResult {
  const clean = (s: unknown): string | null => { const t = typeof s === "string" ? s.trim() : ""; return t || null; };
  const people: Person[] = [];
  for (const it of items) {
    const first = clean(it.firstName);
    const last = clean(it.lastName);
    if (!first || !last) continue; // need a real person name
    people.push({ first_name: first, last_name: last, position: clean(it.jobTitle), headline: clean(it.headline) });
  }
  return { people };
}

// Returns null on any provider failure (row stays unprocessed and retries).
async function deepmineCompanySearch(company: string, state: RunState): Promise<PeopleResult | null> {
  if (!APIFY_API_TOKEN) {
    console.warn("APIFY_API_TOKEN not set - cannot search.");
    return null;
  }
  if (state.domainsUsed >= APIFY_MAX_DOMAINS) {
    console.warn(`Per-run cap (${APIFY_MAX_DOMAINS}) reached - leaving "${company}" for next run.`);
    return null;
  }
  const url = `https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?timeout=${DEEPMINE_TIMEOUT_S}`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${APIFY_API_TOKEN}` },
      body: JSON.stringify({ companies: [company], maxResults: DEEPMINE_MAX_RESULTS, enrichProfiles: false }),
    });
    if (!res.ok) {
      console.warn(`DeepMine ${res.status} for "${company}" - skipping. ${(await res.text()).slice(0, 160)}`);
      return null;
    }
    state.domainsUsed++;
    const items = await res.json();
    if (!Array.isArray(items) || items.length === 0) {
      console.log(`DeepMine returned no people for "${company}".`);
      return { people: [] };
    }
    return deepmineItemsToResult(items as Record<string, unknown>[]);
  } catch (err) {
    console.warn(`DeepMine fetch failed for "${company}": ${err}`);
    return null;
  }
}

// Pick up to MAX_TARGET_LEADS target-role people for one org. NO fallback: if nobody
// matches the target roles, nothing is inserted and the row is flagged
// failed_apify_search = true by run().
// `known` holds "name|org" and "name|domain" keys of people already in alternate_leads
// (or inserted this run) - there is no email to dedupe on.
function pickCandidates(
  result: PeopleResult,
  originalEmail: string,
  domain: string,
  org: string,
  known: Set<string>,
): Person[] {
  const orig = originalEmail.split("@")[0].toLowerCase().replace(/[^a-z]/g, "");
  const seen = new Set<string>();
  const eligible = result.people.filter((p) => {
    const f = normName(p.first_name).replace(/[^a-z]/g, "");
    const l = normName(p.last_name).replace(/[^a-z]/g, "");
    if (f && l && orig.includes(f) && orig.includes(l)) return false; // the unresponsive lead themself
    const k = nameKey(p);
    if (!k || seen.has(k)) return false;
    if (known.has(`${k}|${domain}`) || (org && known.has(`${k}|${normOrg(org)}`))) return false;
    seen.add(k);
    return true;
  });

  const targets = eligible.filter(isTargetRole);
  // Prefer people whose job TITLE (not just headline) matches a target role.
  const score = (p: Person) => (p.position && TARGET_ROLE_PATTERNS.some((re) => re.test(p.position!.toLowerCase())) ? 2 : 0);
  targets.sort((a, b) => score(b) - score(a));
  return targets.slice(0, MAX_TARGET_LEADS);
}

// People already in alternate_leads, as "name|org" and (when an email exists) "name|email-domain".
async function loadExistingPeople(db: SupabaseClient): Promise<Set<string>> {
  const set = new Set<string>();
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await db.from("alternate_leads").select("poc_firstname, poc_lastname, org, email").range(from, from + pageSize - 1);
    if (error) { console.warn(`loadExistingPeople error: ${error.message}`); break; }
    if (!data || data.length === 0) break;
    for (const r of data as Record<string, unknown>[]) {
      const k = nameKey({ first_name: (r.poc_firstname as string) ?? null, last_name: (r.poc_lastname as string) ?? null, position: null, headline: null });
      if (!k) continue;
      if (typeof r.org === "string" && r.org.trim()) set.add(`${k}|${normOrg(r.org)}`);
      if (typeof r.email === "string" && r.email) set.add(`${k}|${domainOf(r.email)}`);
    }
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return set;
}

// Parse "Awesome {event} - What's Next for 2026?" -> {event}.
function eventFromSubject(subject: string | null): string | null {
  if (!subject) return null;
  const m = subject.match(/awesome\s+(.+?)\s+-\s+what'?s next/i);
  return m ? m[1].trim() : null;
}

async function loadEmailSet(
  db: SupabaseClient,
  table: string,
  column = "email",
): Promise<Set<string>> {
  const set = new Set<string>();
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await db
      .from(table)
      .select(column)
      .range(from, from + pageSize - 1);
    if (error) {
      console.warn(`loadEmailSet(${table}) error: ${error.message}`);
      break;
    }
    if (!data || data.length === 0) break;
    for (const row of data as Record<string, unknown>[]) {
      const v = row[column];
      if (typeof v === "string" && v) set.add(v.toLowerCase());
    }
    if (data.length < pageSize) break;
    from += pageSize;
  }
  return set;
}

// ---------------------------------------------------------------------------
// Google Service Account auth (RS256 JWT -> access token). `subject` enables
// domain-wide delegation (required for Gmail send-as a Workspace user).
// ---------------------------------------------------------------------------
function base64UrlEncode(input: string | Uint8Array): string {
  const bytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
  let binary = "";
  for (let i = 0; i < bytes.byteLength; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function getGoogleAccessToken(scope: string, subject?: string): Promise<string> {
  if (!GOOGLE_SA_JSON) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON not set");
  const sa = JSON.parse(GOOGLE_SA_JSON);
  const now = Math.floor(Date.now() / 1000);
  const claim: Record<string, unknown> = {
    iss: sa.client_email,
    scope,
    aud: sa.token_uri || "https://oauth2.googleapis.com/token",
    exp: now + 3600,
    iat: now,
  };
  if (subject) claim.sub = subject; // impersonate a Workspace user (delegation)

  const signingInput = `${base64UrlEncode(JSON.stringify({ alg: "RS256", typ: "JWT" }))}.${base64UrlEncode(JSON.stringify(claim))}`;
  const pem = sa.private_key.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace(/\s/g, "");
  const key = await crypto.subtle.importKey(
    "pkcs8",
    Uint8Array.from(atob(pem), (c) => c.charCodeAt(0)),
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", key, new TextEncoder().encode(signingInput));
  const jwt = `${signingInput}.${base64UrlEncode(new Uint8Array(sig))}`;

  const resp = await fetch(sa.token_uri || "https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: jwt }),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(`Google auth failed (${scope}${subject ? " as " + subject : ""}): ${data.error} ${data.error_description ?? ""}`);
  return data.access_token as string;
}

// --- Sheets helpers ---------------------------------------------------------
async function sheetsListTabs(token: string): Promise<string[]> {
  const r = await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}?fields=sheets.properties(title,sheetId)`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`Sheets get meta failed: ${d.error?.message ?? r.status}`);
  return (d.sheets ?? []).map((s: { properties: { title: string } }) => s.properties.title);
}

async function sheetsGetRow1(token: string): Promise<string[]> {
  const r = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(SHEET_TAB)}!1:1`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const d = await r.json();
  if (!r.ok) throw new Error(`Sheets get header failed: ${d.error?.message ?? r.status}`);
  return (d.values?.[0] ?? []) as string[];
}

async function sheetsEnsureTabWithHeader(token: string, header: string[]): Promise<void> {
  const tabs = await sheetsListTabs(token);
  if (!tabs.includes(SHEET_TAB)) {
    await fetch(`https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}:batchUpdate`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ requests: [{ addSheet: { properties: { title: SHEET_TAB } } }] }),
    });
  }
  const existing = await sheetsGetRow1(token);
  if (existing.length === 0) {
    await fetch(
      `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(SHEET_TAB)}!A1?valueInputOption=RAW`,
      {
        method: "PUT",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ values: [header] }),
      },
    );
  }
}

async function sheetsAppend(token: string, rows: (string | number | null)[][]): Promise<void> {
  const r = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(SHEET_TAB)}!A1:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ values: rows }),
    },
  );
  const d = await r.json();
  if (!r.ok) throw new Error(`Sheets append failed: ${d.error?.message ?? r.status}`);
}

// 0-based column index -> A1 letter (0 -> A, 26 -> AA).
function colLetter(idx0: number): string {
  let n = idx0 + 1, s = "";
  while (n > 0) { const m = (n - 1) % 26; s = String.fromCharCode(65 + m) + s; n = Math.floor((n - 1) / 26); }
  return s;
}

async function sheetsGetAllValues(token: string): Promise<string[][]> {
  const r = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values/${encodeURIComponent(SHEET_TAB)}`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  const d = await r.json();
  if (!r.ok) throw new Error(`Sheets get values failed: ${d.error?.message ?? r.status}`);
  return (d.values ?? []) as string[][];
}

async function sheetsBatchUpdateCells(token: string, data: { range: string; values: string[][] }[]): Promise<void> {
  if (data.length === 0) return;
  const r = await fetch(
    `https://sheets.googleapis.com/v4/spreadsheets/${SPREADSHEET_ID}/values:batchUpdate`,
    {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ valueInputOption: "USER_ENTERED", data }),
    },
  );
  const d = await r.json();
  if (!r.ok) throw new Error(`Sheets batchUpdate failed: ${d.error?.message ?? r.status}`);
}

// Maintenance: sync the sheet's Org column to the DB (org must never be blank once
// the DB has it). Matches sheet rows to alternate_leads by email. Used by ?fixorg=1.
async function fixSheetOrgs(db: SupabaseClient): Promise<Record<string, unknown>> {
  const token = await getGoogleAccessToken("https://www.googleapis.com/auth/spreadsheets");
  const values = await sheetsGetAllValues(token);
  if (values.length < 2) return { updated: 0, note: "no data rows" };
  const header = values[0].map((h) => (h ?? "").trim().toLowerCase());
  const emailCol = header.findIndex((h) => /email/.test(h));
  const orgCol = header.findIndex((h) => /(org|company|organi[sz]ation)/.test(h));
  if (emailCol < 0 || orgCol < 0) return { error: `email/org column not found (email=${emailCol}, org=${orgCol})` };

  // DB org keyed by lowercased email.
  const orgByEmail = new Map<string, string>();
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await db.from("alternate_leads").select("email, org").range(from, from + pageSize - 1);
    if (error) { console.warn(`fixSheetOrgs load: ${error.message}`); break; }
    if (!data || data.length === 0) break;
    for (const r of data as Record<string, unknown>[]) {
      const e = typeof r.email === "string" ? r.email.toLowerCase() : "";
      const o = typeof r.org === "string" ? r.org.trim() : "";
      if (e && o) orgByEmail.set(e, o);
    }
    if (data.length < pageSize) break;
    from += pageSize;
  }

  const orgA1 = colLetter(orgCol);
  const updates: { range: string; values: string[][] }[] = [];
  for (let i = 1; i < values.length; i++) {
    const email = (values[i][emailCol] ?? "").trim().toLowerCase();
    if (!email) continue;
    const dbOrg = orgByEmail.get(email);
    if (!dbOrg) continue;
    const cur = (values[i][orgCol] ?? "").trim();
    if (cur !== dbOrg) updates.push({ range: `'${SHEET_TAB}'!${orgA1}${i + 1}`, values: [[dbOrg]] });
  }
  await sheetsBatchUpdateCells(token, updates);
  return { updated: updates.length, cells: updates.map((u) => u.range) };
}

// --- Gmail helper -----------------------------------------------------------
async function gmailSend(token: string, from: string, to: string, subject: string, html: string): Promise<void> {
  const mime = [
    `From: ${from}`,
    `To: ${to}`,
    `Subject: ${subject}`,
    "MIME-Version: 1.0",
    'Content-Type: text/html; charset="UTF-8"',
    "",
    html,
  ].join("\r\n");
  const raw = base64UrlEncode(mime);
  const r = await fetch("https://gmail.googleapis.com/gmail/v1/users/me/messages/send", {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ raw }),
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`Gmail send failed: ${d.error?.message ?? r.status}`);
}

// --- ActiveCampaign live lookup (fallback when the cached table is empty) ----
async function acContactExists(email: string): Promise<boolean> {
  if (!AC_API_URL || !AC_API_TOKEN) return false;
  try {
    const r = await fetch(`${AC_API_URL}/api/3/contacts?email=${encodeURIComponent(email)}`, {
      headers: { "Api-Token": AC_API_TOKEN, "Content-Type": "application/json" },
    });
    if (!r.ok) {
      console.warn(`AC lookup ${r.status} for ${email}`);
      return false;
    }
    const d = await r.json();
    return Array.isArray(d.contacts) && d.contacts.length > 0;
  } catch (err) {
    console.warn(`AC lookup failed for ${email}: ${err}`);
    return false;
  }
}

async function run(limit?: number, opts?: { notifyTo?: string }): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
  const state: RunState = { domainsUsed: 0 };
  const runStarted = Date.now();

  // 1. Unresponsive leads to process. HARD cap of MAX_ROWS_PER_RUN (20) rows per run;
  //    `?limit=N` can only lower it (testing), never raise it above the cap.
  const rowLimit = limit && limit > 0 ? Math.min(limit, MAX_ROWS_PER_RUN) : MAX_ROWS_PER_RUN;
  const query = db
    .from("closed_sequence_threads")
    .select("id, thread_id, source_table, email")
    .not("email", "is", null)
    .or("is_processed.is.null,is_processed.eq.false")
    // Skip rows whose search already ran and found no target-role lead, so they don't
    // hog the 20-row queue every week. To retry one: set failed_apify_search = null.
    .not("failed_apify_search", "is", true)
    .order("id", { ascending: true })
    .limit(rowLimit);
  const { data: rows, error: qErr } = await query;
  if (qErr) {
    console.error(`Query closed_sequence_threads failed: ${qErr.message}`);
    return { error: qErr.message };
  }
  console.log(`Processing ${rows?.length ?? 0} unresponsive lead(s).`);
  if (!rows || rows.length === 0) return { rows: 0, processed: 0, inserted: 0 };

  // Dedup: DeepMine gives no emails, so people are deduped by name + org / email domain.
  const knownPeople = await loadExistingPeople(db);

  // Context maps for carrying speaker/subject/event onto soc-med leads.
  const threadIds = [...new Set(rows.map((r) => r.thread_id).filter(Boolean))] as string[];
  const emails = [...new Set(rows.map((r) => r.email).filter(Boolean))] as string[];
  const followCtx = await loadContext(db, "follow_up_sequence_threads", threadIds, ["speaker", "subject", "poc_firstname", "source_type_id"]);
  const coldCtx = await loadContext(db, "cold_leads_follow_up_sequence_threads", threadIds, ["speaker", "subject", "poc_firstname", "source_type_id"]);
  const socMedCtx = await loadSocMed(db, threadIds, emails);
  // org must never be null on an alternate lead AND DeepMine searches by company NAME,
  // so org is resolved BEFORE the search: soc-med context -> the ORIGINAL lead's org
  // by email (cold + soc-med detail tables) -> OpenAI web-search resolver -> bare domain.
  const origOrgByEmail = await loadOrgByEmail(db, emails);

  const domainCache = new Map<string, PeopleResult | null>();
  const orgCache = new Map<string, string>(); // OpenAI-resolved org per domain
  const resolveOrg = async (domain: string, hint?: string): Promise<string> => {
    if (!orgCache.has(domain)) orgCache.set(domain, await resolveOrgViaOpenAI(domain, hint));
    return orgCache.get(domain)!;
  };
  let inserted = 0;
  let processed = 0;
  let stoppedEarly = false;
  const newLeadIds: number[] = []; // alternate_leads ids inserted this run (for the email preview)

  // Run `fn` over `items` with at most `limit` in flight at once.
  const mapLimit = async <T>(items: T[], limit: number, fn: (item: T) => Promise<void>): Promise<void> => {
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) await fn(items[next++]);
    }));
  };

  // ---- Pre-pass: ONE org + ONE DeepMine search per DISTINCT company, run in parallel.
  // The actor works through companies serially (~40s each), so a single multi-company
  // call can't fit the ~150s isolate; parallel single-company calls can.
  const domainOrg = new Map<string, string>();
  const domains: string[] = [];
  for (const row of rows) {
    const e = row.email as string;
    const d = domainOf(e);
    if (!d) continue;
    if (!domains.includes(d)) domains.push(d);
    if (!domainOrg.has(d)) {
      const sm0 = socMedCtx.get(row.thread_id) ?? socMedCtx.get(e.toLowerCase());
      const o = (sm0?.org ?? origOrgByEmail.get(e.toLowerCase()) ?? "").trim();
      if (o) domainOrg.set(d, o);
    }
  }
  // Companies with no org in the DB: resolve via OpenAI (parallel).
  await mapLimit(domains.filter((d) => !domainOrg.has(d)), DEEPMINE_CONCURRENCY, async (d) => {
    domainOrg.set(d, await resolveOrg(d));
  });
  // Search every company in parallel (bounded by DEEPMINE_CONCURRENCY).
  await mapLimit(domains, DEEPMINE_CONCURRENCY, async (d) => {
    // Wall-clock guard: don't start a search that can't finish before the sheet flush;
    // those rows are left unprocessed and retry next run.
    if (Date.now() - runStarted > WALL_CLOCK_MS - APPEND_RESERVE_MS - APIFY_CALL_RESERVE_MS) {
      stoppedEarly = true;
      console.warn(`Wall-clock budget nearly spent - skipping search for ${d}; its rows retry next run.`);
      domainCache.set(d, null);
      return;
    }
    domainCache.set(d, await deepmineCompanySearch(domainOrg.get(d)!, state));
  });
  console.log(`Searched ${domains.length} company(ies) in ${Math.round((Date.now() - runStarted) / 1000)}s.`);

  for (const row of rows) {
    const email = row.email as string;
    const domain = domainOf(email);
    if (!domain) {
      // No parseable domain -> no lead possible. Per the strict rule we do NOT
      // mark it processed (only a real inserted lead does that); the skip is free.
      console.warn(`Row ${row.id} has no parseable domain from "${email}" - skipping (left unprocessed).`);
      continue;
    }

    const leadType = row.source_table === "follow_up_sequence_threads" ? "soc med"
      : row.source_table === "cold_leads_follow_up_sequence_threads" ? "cold"
      : null;
    const ctx = followCtx.get(row.thread_id) ?? coldCtx.get(row.thread_id) ?? {};
    const sm = socMedCtx.get(row.thread_id) ?? socMedCtx.get(email.toLowerCase());
    const org = (sm?.org ?? origOrgByEmail.get(email.toLowerCase()) ?? "").trim() || domainOrg.get(domain) || domain;

    const result = domainCache.get(domain) ?? null;
    if (result === null) {
      // Provider errored / no token / cap hit / skipped by the wall-clock guard - leave
      // unprocessed so it retries next run rather than silently burning the row.
      console.warn(`No provider result for ${domain} - leaving row ${row.id} unprocessed for retry.`);
      continue;
    }

    let insertedThisRow = 0;
    {
      const candidates = pickCandidates(result, email, domain, org, knownPeople);

      const subject = ctx.subject ?? null;
      const speaker = (leadType === "soc med") ? (sm?.speaker_name ?? ctx.speaker ?? null) : (ctx.speaker ?? null);
      const event = (leadType === "soc med")
        ? (sm?.event_name ?? eventFromSubject(subject))
        : null;

      for (const c of candidates) {
        const { data: insRow, error: insErr } = await db.from("alternate_leads").insert({
          source_table: row.source_table,
          thread_id: row.thread_id,
          source_type_id: ctx.source_type_id ?? null,
          email: null, // DeepMine provides no emails
          poc_firstname: c.first_name,
          poc_lastname: c.last_name,
          poc_role: c.position ?? c.headline,
          org,
          speaker,
          event,
          subject,
          lead_type: leadType,
          is_added_to_sheet: false, // sheet append is disabled; column left at its default
        }).select("id").single();
        if (insErr) {
          console.warn(`Insert failed for ${c.first_name} ${c.last_name}: ${insErr.message}`);
          continue;
        }
        if (insRow?.id != null) newLeadIds.push(insRow.id as number);
        const k = nameKey(c);
        knownPeople.add(`${k}|${domain}`);
        if (org) knownPeople.add(`${k}|${normOrg(org)}`);
        inserted++;
        insertedThisRow++;
      }
    }

    // 4. Mark the source row processed ONLY when a real alternate lead was actually
    //    inserted for it. If the provider found nothing, nobody matched the target
    //    roles, or every candidate was already known, leave the row unprocessed so it
    //    retries next run - no row is burned without a lead.
    //    The search itself succeeded here (provider errors `continue`d above), so:
    //    lead inserted -> is_processed = true, failed_apify_search = false;
    //    no matching lead -> failed_apify_search = true (is_processed stays null).
    if (insertedThisRow > 0) {
      await markProcessed(db, row.id);
      processed++;
    } else {
      await markFailedSearch(db, row.id);
      // If the company has NO target-role people at all, every other unprocessed row
      // for the same company (same email domain) would fail identically - flag them all
      // now so we don't re-search the company. (If target people existed but were all
      // excluded - e.g. this row's own original lead - other rows may still match, so
      // only this row is flagged.)
      let siblings = 0;
      if (!result.people.some(isTargetRole)) siblings = await markCompanyFailed(db, domain, row.id);
      console.log(`No matching lead for row ${row.id} (${domain}) - flagged failed_apify_search=true${siblings ? ` (+${siblings} other row(s) for the same company)` : ""}.`);
    }
  }

  console.log(`Inserted ${inserted} alternate lead(s) across ${domainCache.size} domain(s).${stoppedEarly ? " (stopped early on wall-clock budget - remaining rows retry next run)" : ""}`);

  // 5. Email Liv a preview of the leads inserted this run (NO sheet append any more).
  //    (?to= redirects the notification during testing so Liv isn't emailed).
  const sheetResult = await notifyNewLeads(db, newLeadIds, opts?.notifyTo);

  return {
    rows: rows.length, processed, inserted, domains: domainCache.size, stoppedEarly,
    provider: "deepmine", deepmineSearches: state.domainsUsed, ...sheetResult,
  };
}

interface AltRow {
  id: number; created_at: string | null; lead_type: string | null; org: string | null;
  poc_firstname: string | null; poc_lastname: string | null; poc_role: string | null;
  email: string | null; speaker: string | null; event: string | null; subject: string | null;
  source_table: string | null; thread_id: string | null;
}

// Canonical header used when the tab has to be created from scratch.
const SHEET_HEADER = [
  "Date Found", "Lead Type", "Org", "POC First Name", "POC Last Name", "POC Role",
  "Email", "Speaker", "Event", "Subject", "Source Table", "Original Thread ID",
];

// Map a lead's value to a sheet header cell (dynamic to whatever columns exist).
function valueForHeader(h: string, r: AltRow): string {
  const k = h.trim().toLowerCase();
  const date = (r.created_at ?? "").slice(0, 10);
  if (/(date|found|created)/.test(k)) return date;
  if (/(lead\s*type|^type$)/.test(k)) return r.lead_type ?? "";
  if (/(org|company|organi[sz]ation)/.test(k)) return r.org ?? "";
  if (/(first)/.test(k)) return r.poc_firstname ?? "";
  if (/(last)/.test(k)) return r.poc_lastname ?? "";
  if (/(role|position|title)/.test(k)) return r.poc_role ?? "";
  if (/(email)/.test(k)) return r.email ?? "";
  if (/(speaker)/.test(k)) return r.speaker ?? "";
  if (/(event)/.test(k)) return r.event ?? "";
  if (/(subject)/.test(k)) return r.subject ?? "";
  if (/(source)/.test(k)) return r.source_table ?? "";
  if (/(thread)/.test(k)) return r.thread_id ?? "";
  return "";
}

function esc(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function buildEmailHtml(leads: AltRow[]): string {
  const previewRows = leads.slice(0, 100).map((r) => {
    const name = [r.poc_firstname, r.poc_lastname].filter(Boolean).join(" ") || "—";
    const ctx = r.lead_type === "soc med" && (r.event || r.speaker)
      ? `${esc(r.event ?? "")}${r.speaker ? " · " + esc(r.speaker) : ""}` : "";
    return `<tr>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(r.org ?? "—")}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(name)}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(r.poc_role ?? "—")}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${r.email ? `<a href="mailto:${esc(r.email)}">${esc(r.email)}</a>` : "—"}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(r.lead_type ?? "—")}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${ctx || "—"}</td>
    </tr>`;
  }).join("");
  const more = leads.length > 100 ? `<p style="color:#666;font-size:13px">…and ${leads.length - 100} more in the alternate_leads table.</p>` : "";
  return `<div style="font-family:Arial,Helvetica,sans-serif;color:#222;max-width:760px">
    <p>Hi Liv,</p>
    <p><strong>${leads.length} new alternate lead${leads.length === 1 ? "" : "s"}</strong> ${leads.length === 1 ? "is" : "are"} ready for your review.</p>
    <p>These are <strong>alternate contacts at the same companies as our closed / unresponsive leads</strong> —
       people we can reach out to instead, prioritising the target POC roles.</p>
    <table style="border-collapse:collapse;font-size:14px;width:100%;margin:12px 0">
      <thead><tr style="background:#f4f4f6;text-align:left">
        <th style="padding:8px 10px">Org</th><th style="padding:8px 10px">Name</th>
        <th style="padding:8px 10px">Role</th><th style="padding:8px 10px">Email</th>
        <th style="padding:8px 10px">Type</th><th style="padding:8px 10px">Event · Speaker</th>
      </tr></thead>
      <tbody>${previewRows}</tbody>
    </table>
    ${more}
    <p style="color:#888;font-size:12px">Automated by the Alternate Lead Finder. Leads are stored in the alternate_leads table.</p>
  </div>`;
}

// The function NO LONGER appends to the Google Sheet (the sheet helpers above are kept
// for ?inspect / ?fixorg only). It just emails a preview of the leads inserted THIS run.
async function notifyNewLeads(db: SupabaseClient, ids: number[], toOverride?: string): Promise<Record<string, unknown>> {
  const result: Record<string, unknown> = { emailed: false };
  const recipient = toOverride || NOTIFY_TO;
  if (ids.length === 0) { console.log("No new leads - no email sent."); return result; }

  const { data: pending, error } = await db
    .from("alternate_leads")
    .select("id, created_at, lead_type, org, poc_firstname, poc_lastname, poc_role, email, speaker, event, subject, source_table, thread_id")
    .in("id", ids)
    .order("id", { ascending: true });
  if (error) { result.notifyError = error.message; return result; }
  const leads = (pending ?? []) as AltRow[];
  if (leads.length === 0) return result;

  // --- Email Liv a preview ---
  try {
    const gToken = await getGoogleAccessToken("https://www.googleapis.com/auth/gmail.send", NOTIFY_FROM);
    const subject = `${leads.length} new alternate lead${leads.length === 1 ? "" : "s"} ready for review`;
    await gmailSend(gToken, NOTIFY_FROM, recipient, subject, buildEmailHtml(leads));
    result.emailed = true;
    result.emailedTo = recipient;
    console.log(`Notification email sent to ${recipient}.`);
  } catch (err) {
    console.error(`Email notification failed: ${err}`);
    result.emailError = String(err);
  }
  return result;
}

// Original lead's org, keyed by lowercased original email, unioned across the four
// source-detail tables. ai_* sources take precedence over manually_* (first write
// wins). Used to guarantee alternate_leads.org is never null.
async function loadOrgByEmail(db: SupabaseClient, emails: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  if (emails.length === 0) return map;
  const tables = [
    "ai_verified_cold_leads", "manually_found_cold_leads", // cold sources
    "ai_scraped_soc_med_leads", "manually_found_leads",    // soc-med sources
  ];
  for (const t of tables) {
    const { data, error } = await db.from(t).select("email, org").in("email", emails);
    if (error) { console.warn(`loadOrgByEmail(${t}) error: ${error.message}`); continue; }
    for (const r of (data ?? []) as Record<string, unknown>[]) {
      const e = typeof r.email === "string" ? r.email.toLowerCase() : "";
      const org = typeof r.org === "string" ? r.org.trim() : "";
      if (e && org && !map.has(e)) map.set(e, org);
    }
  }
  return map;
}

// Last-resort org lookup via an OpenAI model. Returns a company name, or the bare
// domain when OpenAI is unavailable / errors / is unsure — never null/blank. `hint`
// is light grounding (the contact's name + title) to disambiguate the domain.
async function resolveOrgViaOpenAI(domain: string, hint?: string): Promise<string> {
  if (!OPENAI_API_KEY) return domain; // not configured — fall back to the domain
  try {
    const input = `What is the common trading name of the organization that owns the website `
      + `domain ${domain}, as it would appear on its LinkedIn company page?` + (hint ? ` A known contact there: ${hint}.` : "")
      + ` Use the short everyday name (e.g. "AMCA Australia", not "Air Conditioning & Mechanical `
      + `Contractors' Association of Australia Limited") and drop legal suffixes such as Pty Ltd, `
      + `Limited, Inc. Reply with ONLY the organization name and nothing else. If you cannot `
      + `determine it with confidence, reply with exactly: ${domain}`;
    const res = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        tools: [{ type: "web_search_preview" }],
        input,
      }),
    });
    if (!res.ok) {
      console.warn(`OpenAI org lookup ${res.status} for ${domain} — using domain. ${(await res.text()).slice(0, 160)}`);
      return domain;
    }
    const data = await res.json();
    // Responses API: output is an array of items; the assistant `message` item holds
    // the answer as an `output_text` content part.
    let text = "";
    for (const o of (data?.output ?? [])) {
      if (o?.type !== "message") continue;
      for (const c of (o?.content ?? [])) {
        if (c?.type === "output_text" && typeof c.text === "string") text = c.text;
      }
    }
    const org = text.trim().replace(/^["']|["']$/g, "").trim();
    if (!org) return domain;
    console.log(`OpenAI resolved org for ${domain}: "${org}".`);
    return org;
  } catch (err) {
    console.warn(`OpenAI org lookup failed for ${domain}: ${err} — using domain.`);
    return domain;
  }
}

interface Ctx { speaker?: string | null; subject?: string | null; poc_firstname?: string | null; source_type_id?: number | null }
async function loadContext(
  db: SupabaseClient,
  table: string,
  threadIds: string[],
  cols: string[],
): Promise<Map<string, Ctx>> {
  const map = new Map<string, Ctx>();
  if (threadIds.length === 0) return map;
  const { data, error } = await db
    .from(table)
    .select(["thread_id", ...cols].join(","))
    .in("thread_id", threadIds);
  if (error) {
    console.warn(`loadContext(${table}) error: ${error.message}`);
    return map;
  }
  for (const r of (data ?? []) as Record<string, unknown>[]) {
    const tid = r.thread_id as string;
    if (tid && !map.has(tid)) {
      map.set(tid, { speaker: r.speaker as string, subject: r.subject as string, poc_firstname: r.poc_firstname as string, source_type_id: r.source_type_id as number });
    }
  }
  return map;
}

interface SocMed { event_name?: string | null; speaker_name?: string | null; org?: string | null }
async function loadSocMed(
  db: SupabaseClient,
  threadIds: string[],
  emails: string[],
): Promise<Map<string, SocMed>> {
  const map = new Map<string, SocMed>();
  const push = (key: string | null | undefined, v: SocMed) => {
    if (key && !map.has(key)) map.set(key, v);
  };
  const cols = "thread_id, email, event_name, speaker_name, org";
  if (threadIds.length > 0) {
    const { data } = await db.from("ai_scraped_soc_med_leads").select(cols).in("thread_id", threadIds);
    for (const r of (data ?? []) as Record<string, string>[]) {
      const v = { event_name: r.event_name, speaker_name: r.speaker_name, org: r.org };
      push(r.thread_id, v);
      push(r.email?.toLowerCase(), v);
    }
  }
  if (emails.length > 0) {
    const { data } = await db.from("ai_scraped_soc_med_leads").select(cols).in("email", emails);
    for (const r of (data ?? []) as Record<string, string>[]) {
      const v = { event_name: r.event_name, speaker_name: r.speaker_name, org: r.org };
      push(r.email?.toLowerCase(), v);
      push(r.thread_id, v);
    }
  }
  return map;
}

async function markProcessed(db: SupabaseClient, id: number): Promise<void> {
  const { error } = await db.from("closed_sequence_threads").update({ is_processed: true, failed_apify_search: false }).eq("id", id);
  if (error) console.warn(`markProcessed(${id}) failed: ${error.message}`);
}

// Flag every OTHER not-yet-processed row for the same company (email domain) as a
// failed search. Never touches processed rows or rows already flagged. Returns the count.
async function markCompanyFailed(db: SupabaseClient, domain: string, exceptId: number): Promise<number> {
  const pattern = `%@${domain.replace(/[\\%_]/g, "\\$&")}`;
  const { data, error } = await db
    .from("closed_sequence_threads")
    .update({ failed_apify_search: true })
    .ilike("email", pattern)
    .neq("id", exceptId)
    .or("is_processed.is.null,is_processed.eq.false")
    .not("failed_apify_search", "is", true)
    .select("id");
  if (error) { console.warn(`markCompanyFailed(${domain}) failed: ${error.message}`); return 0; }
  return data?.length ?? 0;
}

// The DeepMine search ran but produced no lead within the target roles.
async function markFailedSearch(db: SupabaseClient, id: number): Promise<void> {
  const { error } = await db.from("closed_sequence_threads").update({ failed_apify_search: true }).eq("id", id);
  if (error) console.warn(`markFailedSearch(${id}) failed: ${error.message}`);
}

Deno.serve(async (req: Request) => {
  const params = new URL(req.url).searchParams;
  const limitParam = params.get("limit");
  const limit = limitParam ? parseInt(limitParam, 10) : undefined;

  // ?append=1 used to run the sheet-append + email step. The sheet append was removed,
  // so it is now a no-op that says so (nothing is written or sent).
  if (params.get("append") === "1") {
    return Response.json({ status: "disabled", note: "The function no longer appends to the sheet." });
  }

  // Maintenance: ?fixorg=1 syncs the sheet's Org column to the DB (fills blanks /
  // corrects drift) by matching sheet rows to alternate_leads on email. No sends.
  if (params.get("fixorg") === "1") {
    try {
      const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });
      const res = await fixSheetOrgs(db);
      return Response.json({ status: "done", ...res });
    } catch (err) {
      return Response.json({ status: "error", error: String(err) }, { status: 500 });
    }
  }

  // Debug: ?inspect=1 verifies Sheets access (tabs + header) and Gmail
  // delegation (token only, no send), so we can confirm setup before going live.
  if (params.get("inspect") === "1") {
    const out: Record<string, unknown> = {};
    try {
      const t = await getGoogleAccessToken("https://www.googleapis.com/auth/spreadsheets");
      out.sheetsTokenOk = true;
      try {
        out.tabs = await sheetsListTabs(t);
        out.header = await sheetsGetRow1(t);
      } catch (e) {
        out.sheetsReadError = String(e);
      }
    } catch (e) {
      out.sheetsTokenError = String(e);
    }
    try {
      await getGoogleAccessToken("https://www.googleapis.com/auth/gmail.send", NOTIFY_FROM);
      out.gmailDelegationOk = true;
    } catch (e) {
      out.gmailDelegationError = String(e);
    }
    out.acConfigured = Boolean(AC_API_URL && AC_API_TOKEN);
    return Response.json(out);
  }

  // Debug: ?probe=<company name or domain> runs the DeepMine actor directly (no writes)
  // and reports how many people came back and which pass the target-role filter.
  // The actor searches by company NAME, so a bare domain may match poorly.
  if (params.get("probe") || params.get("aprobe")) {
    const q = (params.get("probe") ?? params.get("aprobe"))!;
    const info: Record<string, unknown> = { apifyKeyLen: APIFY_API_TOKEN.length, actor: APIFY_ACTOR, query: q };
    try {
      const r = await deepmineCompanySearch(q, { domainsUsed: 0 });
      info.returned = r ? r.people.length : null; // null = provider failure (see logs)
      info.targets = r ? r.people.filter(isTargetRole).map((p) => ({ name: `${p.first_name} ${p.last_name}`, role: p.position ?? p.headline })) : null;
      // What the picker would choose (target-role people only).
      info.picked = r ? pickCandidates(r, "", "", "", new Set()).map((p) => ({ name: `${p.first_name} ${p.last_name}`, role: p.position ?? p.headline })) : null;
    } catch (err) {
      info.fetchError = String(err);
    }
    return Response.json(info);
  }

  // Debug: ?orgprobe=<domain> runs the OpenAI last-resort org resolver directly and
  // returns what it would store, without writing anything. Optional &hint=<text>.
  if (params.get("orgprobe")) {
    const dom = params.get("orgprobe")!;
    const org = await resolveOrgViaOpenAI(dom, params.get("hint") ?? undefined);
    return Response.json({ domain: dom, model: OPENAI_MODEL, openaiKeyLen: OPENAI_API_KEY.length, org });
  }

  // ?to= redirects the notification email (use when testing so Liv isn't emailed).
  const notifyTo = params.get("to") ?? undefined;

  // Debug/testing: ?sync=1 awaits the work and returns the outcome (or error)
  // in the response, so failures are visible instead of vanishing in the bg task.
  if (params.get("sync") === "1") {
    try {
      const result = await run(limit, { notifyTo });
      return Response.json({ status: "done", ...result });
    } catch (err) {
      console.error(`run() crashed: ${err}`);
      return Response.json({ status: "error", error: String(err), stack: (err as Error)?.stack }, { status: 500 });
    }
  }

  const work = run(limit, { notifyTo }).catch((err) => console.error(`run() crashed: ${err}`));
  if (typeof EdgeRuntime !== "undefined") {
    EdgeRuntime.waitUntil(work);
  }
  return new Response(JSON.stringify({ status: "started" }), {
    status: 202,
    headers: { "Content-Type": "application/json" },
  });
});
