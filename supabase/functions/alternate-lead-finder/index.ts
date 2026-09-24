// alternate-lead-finder
// Weekly (Mon 6:55 AM PHT) automation. For each unresponsive lead in
// closed_sequence_threads, find a DIFFERENT contact at the same org via
// Hunter.io (prioritising target POC roles), dedup against ActiveCampaign
// (cached table, with live AC API fallback when the table is empty), record it
// in alternate_leads, mark the source row processed, append the new leads to the
// sales Google Sheet (Sheets API via the service account), and email a dynamic
// preview to Liv (Gmail API via SA domain-wide delegation).
//
// Triggered by pg_cron (net.http_post). Kicks the real work into a background
// task and returns 202 immediately so it never hits the request timeout.
// Debug params: ?sync=1, ?limit=N, ?inspect=1, ?probe=<domain>.

import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

// Supabase edge runtime global for background work.
declare const EdgeRuntime: { waitUntil(p: Promise<unknown>): void } | undefined;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const HUNTER_API_KEY = Deno.env.get("HUNTER_IO_API_KEY") ?? "";
const GOOGLE_SA_JSON = Deno.env.get("GOOGLE_SERVICE_ACCOUNT_JSON") ?? "";
const AC_API_URL = (Deno.env.get("AC_API_URL") ?? "").replace(/\/+$/, "");
const AC_API_TOKEN = Deno.env.get("AC_API_TOKEN") ?? "";

// Apify fallback (used only when Hunter's plan credits are exhausted). Actor
// scrapersdelight/decision-maker-email-finder scrapes the company's OWN site +
// LinkedIn (geography-agnostic, like Hunter) and returns decision-maker records
// with name, title, best-guess work email, pattern, confidence and MX validation.
// API slug uses `~` in the run URL path.
const APIFY_API_TOKEN = Deno.env.get("APIFY_API_TOKEN") ?? "";
const APIFY_ACTOR = Deno.env.get("APIFY_ACTOR") ?? "scrapersdelight~decision-maker-email-finder";
const APIFY_LIMIT = parseInt(Deno.env.get("APIFY_LIMIT") ?? "10", 10); // contacts per domain
const APIFY_MAX_DOMAINS = parseInt(Deno.env.get("APIFY_MAX_DOMAINS") ?? "80", 10);

// Wall-clock guard. The edge isolate is hard-killed at ~150s, and the
// append-to-sheet + email step runs only AFTER the processing loop — so a run that
// times out mid-loop (esp. on the slow Apify fallback, ~1 min/domain) inserts leads
// into the DB but never flushes them to the sheet or emails a preview. We stop
// starting new provider calls before the budget is spent so the flush always runs.
const WALL_CLOCK_MS = parseInt(Deno.env.get("WALL_CLOCK_MS") ?? "150000", 10);
const APPEND_RESERVE_MS = parseInt(Deno.env.get("APPEND_RESERVE_MS") ?? "15000", 10); // headroom for the sheet+email flush
const HUNTER_CALL_RESERVE_MS = parseInt(Deno.env.get("HUNTER_CALL_RESERVE_MS") ?? "8000", 10); // worst-case one Hunter call
const APIFY_CALL_RESERVE_MS = parseInt(Deno.env.get("APIFY_CALL_RESERVE_MS") ?? "70000", 10); // worst-case one Apify actor run

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

// Roles we prioritise (keyword groups matched against Hunter position/department).
const TARGET_ROLE_KEYWORDS: string[] = [
  "human resources", "hr ", " hr", "hr manager", "head of hr", "chief people",
  "people & culture", "people and culture", "people culture",
  "executive assistant", "personal assistant", "assistant to", "ea to",
  "events", "event manager", "events manager",
  "conference",
  "marketing",
  "communication", "communications",
  "learning & development", "learning and development", "l&d", "l & d",
];
const MAX_TARGET_LEADS = 5; // per org, when target roles are found
const HUNTER_DELAY_MS = 300; // politeness between distinct domain calls

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const domainOf = (email: string) => (email.includes("@") ? email.split("@")[1].trim().toLowerCase() : "");

interface HunterEmail {
  value: string;
  first_name: string | null;
  last_name: string | null;
  position: string | null;
  department: string | null;
  seniority: string | null;
  type: string | null; // personal | generic
}
interface HunterResult {
  organization: string | null;
  emails: HunterEmail[];
}

function isTargetRole(e: HunterEmail): boolean {
  const hay = `${e.position ?? ""} ${e.department ?? ""} ${e.seniority ?? ""}`.toLowerCase();
  return TARGET_ROLE_KEYWORDS.some((kw) => hay.includes(kw));
}

// Outcome of a Hunter lookup. `exhausted` means the plan's search credits are
// used up (switch to the Apify fallback); `error` is transient (retry next run).
type HunterOutcome =
  | { status: "ok"; result: HunterResult }
  | { status: "exhausted" }
  | { status: "error" };

// A 429 from Hunter is ambiguous: it fires both for per-second rate limiting AND
// for the monthly plan quota being used up. They share id "too_many_requests";
// only the details text distinguishes them ("...limit for the number of searches
// per billing period..."). Treat the plan-quota variant as `exhausted`.
function hunterQuotaExhausted(body: string): boolean {
  return /billing period|number of searches|per your plan|upgrade your plan/i.test(body);
}

async function hunterDomainSearch(domain: string): Promise<HunterOutcome> {
  if (!HUNTER_API_KEY) {
    console.warn("HUNTER_IO_API_KEY not set — cannot search domains.");
    return { status: "error" };
  }
  // limit=10 is the Free plan's max page size (higher values return HTTP 400).
  const url = `https://api.hunter.io/v2/domain-search?domain=${encodeURIComponent(domain)}&limit=10&api_key=${HUNTER_API_KEY}`;
  try {
    const res = await fetch(url);
    if (!res.ok) {
      const body = await res.text();
      if ((res.status === 429 || res.status === 403) && hunterQuotaExhausted(body)) {
        console.warn(`Hunter plan credits exhausted (${res.status}) for ${domain}.`);
        return { status: "exhausted" };
      }
      console.warn(`Hunter ${res.status} for ${domain} — skipping. ${body.slice(0, 160)}`);
      return { status: "error" };
    }
    const json = await res.json();
    const data = json?.data ?? {};
    return {
      status: "ok",
      result: {
        organization: data.organization ?? null,
        emails: Array.isArray(data.emails) ? data.emails : [],
      },
    };
  } catch (err) {
    console.warn(`Hunter fetch failed for ${domain}: ${err}`);
    return { status: "error" };
  }
}

// --- Apify fallback ---------------------------------------------------------
interface RunState { useApify: boolean; apifyDomainsUsed: number }

// Map the Apify decision-maker-email-finder dataset (a flat array of records:
// { email, firstName, lastName, title, seniority, confidence, mxValid, ... }) to
// the Hunter shape so the rest of the pipeline (pickCandidates / isTargetRole /
// scoreContact) is unchanged. This actor guesses emails from the company's site,
// so we drop records with no email or an undeliverable domain (mxValid === false).
function apifyItemsToResult(items: Record<string, unknown>[]): HunterResult {
  const cap = (s: unknown): string | null => {
    const t = typeof s === "string" ? s.trim() : "";
    return t ? t.charAt(0).toUpperCase() + t.slice(1) : null;
  };
  const emails: HunterEmail[] = [];
  for (const it of items) {
    const val = (it.email ?? "") as string;
    if (!val) continue;                 // no usable email
    if (it.mxValid === false) continue; // domain can't receive mail
    emails.push({
      value: val,
      first_name: cap(it.firstName),
      last_name: cap(it.lastName),
      position: (it.title ?? null) as string | null,
      department: null,
      seniority: (it.seniority ?? null) as string | null,
      type: "personal", // named decision-makers — treat as personal (scored/kept)
    });
  }
  // This actor has no reliable per-domain org name field; org is left to the
  // soc-med context fallback (or null for cold leads).
  return { organization: null, emails };
}

async function apifyDomainSearch(domain: string, state: RunState): Promise<HunterResult | null> {
  if (!APIFY_API_TOKEN) {
    console.warn("APIFY_API_TOKEN not set — cannot run Apify fallback.");
    return null;
  }
  if (state.apifyDomainsUsed >= APIFY_MAX_DOMAINS) {
    console.warn(`Apify per-run cap (${APIFY_MAX_DOMAINS}) reached — leaving ${domain} for next run.`);
    return null;
  }
  const url = `https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?token=${APIFY_API_TOKEN}`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ domains: [domain], maxContactsPerDomain: APIFY_LIMIT, maxItems: APIFY_LIMIT, useGoogleFallback: true }),
    });
    if (!res.ok) {
      console.warn(`Apify ${res.status} for ${domain} — skipping. ${(await res.text()).slice(0, 160)}`);
      return null;
    }
    state.apifyDomainsUsed++;
    const items = await res.json();
    if (!Array.isArray(items) || items.length === 0) {
      console.log(`Apify returned no data for ${domain}.`);
      return { organization: null, emails: [] };
    }
    return apifyItemsToResult(items as Record<string, unknown>[]);
  } catch (err) {
    console.warn(`Apify fetch failed for ${domain}: ${err}`);
    return null;
  }
}

// Provider dispatcher: Hunter is primary; switch to Apify (sticky for the rest of
// the run) the first time Hunter reports its plan credits are exhausted.
async function findDomainContacts(domain: string, state: RunState): Promise<HunterResult | null> {
  if (!state.useApify) {
    const h = await hunterDomainSearch(domain);
    if (h.status === "ok") return h.result;
    if (h.status === "exhausted") {
      console.warn("Hunter credits exhausted — switching to Apify fallback for the rest of this run.");
      state.useApify = true;
      // fall through to Apify for this same domain
    } else {
      return null; // transient — leave the row unprocessed so it retries
    }
  }
  return await apifyDomainSearch(domain, state);
}

// Pick candidates for one org: up to 5 target-role contacts, else 1 fallback.
function pickCandidates(
  result: HunterResult,
  originalEmail: string,
  excluded: Set<string>, // lowercased emails already in AC / alternate_leads / this run
): HunterEmail[] {
  const origLower = originalEmail.toLowerCase();
  const usable = result.emails.filter((e) => {
    if (!e.value) return false;
    const v = e.value.toLowerCase();
    return v !== origLower && !excluded.has(v);
  });

  const targets = usable.filter(isTargetRole);
  // Prefer personal, named contacts within targets.
  targets.sort((a, b) => scoreContact(b) - scoreContact(a));
  if (targets.length > 0) return targets.slice(0, MAX_TARGET_LEADS);

  // Fallback: max 1 of any role, prefer personal + named.
  const fallback = usable.filter((e) => e.type !== "generic");
  fallback.sort((a, b) => scoreContact(b) - scoreContact(a));
  const chosen = fallback.length > 0 ? fallback[0] : usable[0];
  return chosen ? [chosen] : [];
}

function scoreContact(e: HunterEmail): number {
  let s = 0;
  if (e.type === "personal") s += 2;
  if (e.first_name) s += 1;
  if (e.last_name) s += 1;
  if (e.position) s += 1;
  return s;
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

async function run(limit?: number, opts?: { forceApify?: boolean; notifyTo?: string }): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
  const state: RunState = { useApify: opts?.forceApify ?? false, apifyDomainsUsed: 0 };
  const runStarted = Date.now();

  // 1. Unresponsive leads to process. Capped at 20 rows per run by default; the
  //    `?limit=N` param overrides it (testing).
  const rowLimit = limit && limit > 0 ? limit : 20;
  const query = db
    .from("closed_sequence_threads")
    .select("id, thread_id, source_table, email")
    .not("email", "is", null)
    .or("is_processed.is.null,is_processed.eq.false")
    .order("id", { ascending: true })
    .limit(rowLimit);
  const { data: rows, error: qErr } = await query;
  if (qErr) {
    console.error(`Query closed_sequence_threads failed: ${qErr.message}`);
    return { error: qErr.message };
  }
  console.log(`Processing ${rows?.length ?? 0} unresponsive lead(s).`);
  if (!rows || rows.length === 0) return { rows: 0, processed: 0, inserted: 0 };

  // Dedup sets: AC contacts (cached table) + existing alternate_leads.
  const acEmails = await loadEmailSet(db, "activecampaign_contacts");
  const acTableEmpty = acEmails.size === 0;
  if (acTableEmpty) {
    console.warn("activecampaign_contacts is EMPTY — falling back to live ActiveCampaign API for dedup.");
  }
  const existingAlt = await loadEmailSet(db, "alternate_leads");

  // Context maps for carrying speaker/subject/event onto soc-med leads.
  const threadIds = [...new Set(rows.map((r) => r.thread_id).filter(Boolean))] as string[];
  const emails = [...new Set(rows.map((r) => r.email).filter(Boolean))] as string[];
  const followCtx = await loadContext(db, "follow_up_sequence_threads", threadIds, ["speaker", "subject", "poc_firstname", "source_type_id"]);
  const coldCtx = await loadContext(db, "cold_leads_follow_up_sequence_threads", threadIds, ["speaker", "subject", "poc_firstname", "source_type_id"]);
  const socMedCtx = await loadSocMed(db, threadIds, emails);
  // org must never be null on an alternate lead: when Hunter/Apify return no org,
  // carry over the ORIGINAL lead's org, looked up by the original email across the
  // cold detail tables (ai_verified_cold_leads, manually_found_cold_leads) and the
  // soc-med detail tables (ai_scraped_soc_med_leads, manually_found_leads).
  const origOrgByEmail = await loadOrgByEmail(db, emails);

  const domainCache = new Map<string, HunterResult | null>();
  const orgCache = new Map<string, string>(); // resolved org per domain (incl. OpenAI last-resort)
  const insertedThisRun = new Set<string>();
  let inserted = 0;
  let processed = 0;
  let stoppedEarly = false;

  for (const row of rows) {
    const email = row.email as string;
    const domain = domainOf(email);
    if (!domain) {
      // No parseable domain -> no lead possible. Per the strict rule we do NOT
      // mark it processed (only a real inserted lead does that); the skip is free.
      console.warn(`Row ${row.id} has no parseable domain from "${email}" — skipping (left unprocessed).`);
      continue;
    }

    let result = domainCache.get(domain);
    if (result === undefined) {
      // Wall-clock guard — only before an actual provider call (cache misses are
      // free). If starting one risks overrunning the isolate's hard kill, stop the
      // loop now so the append+email flush below still runs and this run's inserts
      // reach the sheet. Remaining rows are left unprocessed and retry next run.
      const callReserve = state.useApify ? APIFY_CALL_RESERVE_MS : HUNTER_CALL_RESERVE_MS;
      if (Date.now() - runStarted > WALL_CLOCK_MS - APPEND_RESERVE_MS - callReserve) {
        stoppedEarly = true;
        console.warn(`Wall-clock budget nearly spent — stopping before ${domain} (row ${row.id}); ${inserted} lead(s) inserted so far. Remaining rows left unprocessed for retry; flushing to sheet.`);
        break;
      }
      result = await findDomainContacts(domain, state);
      domainCache.set(domain, result);
      await sleep(HUNTER_DELAY_MS);
    }

    if (result === null) {
      // Provider errored / rate-limited / no key / cap hit — leave unprocessed so
      // it retries next run rather than silently burning the row.
      console.warn(`No provider result for ${domain} — leaving row ${row.id} unprocessed for retry.`);
      continue;
    }

    let insertedThisRow = 0;
    {
      const excluded = new Set<string>([...acEmails, ...existingAlt, ...insertedThisRun]);
      const candidates = pickCandidates(result, email, excluded);

      const leadType = row.source_table === "follow_up_sequence_threads" ? "soc med"
        : row.source_table === "cold_leads_follow_up_sequence_threads" ? "cold"
        : null;

      const ctx = followCtx.get(row.thread_id) ?? coldCtx.get(row.thread_id) ?? {};
      const sm = socMedCtx.get(row.thread_id) ?? socMedCtx.get(email.toLowerCase());
      const speaker = (leadType === "soc med") ? (sm?.speaker_name ?? ctx.speaker ?? null) : (ctx.speaker ?? null);
      const subject = ctx.subject ?? null;
      const event = (leadType === "soc med")
        ? (sm?.event_name ?? eventFromSubject(subject))
        : null;
      // org must never be null. Order: provider org (Hunter) -> soc-med context ->
      // carry over the original lead's org by email -> OpenAI resolver (domain +
      // contact hint, cached per domain) -> bare domain. Only resolved when there
      // is at least one candidate to insert, so we don't spend an OpenAI call on a
      // row that inserts nothing.
      let org: string | null = result.organization ?? sm?.org ?? origOrgByEmail.get(email.toLowerCase()) ?? null;
      if ((!org || !org.trim()) && candidates.length > 0) {
        if (!orgCache.has(domain)) {
          const c0 = candidates[0];
          const hint = [[c0.first_name, c0.last_name].filter(Boolean).join(" "), c0.position]
            .filter(Boolean).join(", ");
          orgCache.set(domain, await resolveOrgViaOpenAI(domain, hint || undefined));
        }
        org = orgCache.get(domain)!;
      }

      for (const c of candidates) {
        const vLower = c.value.toLowerCase();
        // Live AC fallback: when the cached table is empty, verify against AC directly.
        if (acTableEmpty && await acContactExists(c.value)) {
          console.log(`Skipping ${c.value} — already in ActiveCampaign (live check).`);
          continue;
        }
        const { error: insErr } = await db.from("alternate_leads").insert({
          source_table: row.source_table,
          thread_id: row.thread_id,
          source_type_id: ctx.source_type_id ?? null,
          email: c.value,
          poc_firstname: c.first_name,
          poc_lastname: c.last_name,
          poc_role: c.position,
          org,
          speaker,
          event,
          subject,
          lead_type: leadType,
          is_added_to_sheet: false,
        });
        if (insErr) {
          console.warn(`Insert failed for ${c.value}: ${insErr.message}`);
          continue;
        }
        insertedThisRun.add(vLower);
        inserted++;
        insertedThisRow++;
      }
    }

    // 4. Mark the source row processed ONLY when a real alternate lead (with an
    //    email) was actually inserted for it. If the provider found nothing, or
    //    every candidate was already known (deduped away), leave the row
    //    unprocessed so it retries next run — no row is burned without a lead.
    if (insertedThisRow > 0) {
      await markProcessed(db, row.id);
      processed++;
    } else {
      console.log(`No new lead inserted for row ${row.id} (${domain}) — leaving unprocessed to retry.`);
    }
  }

  console.log(`Inserted ${inserted} alternate lead(s) across ${domainCache.size} domain(s).${stoppedEarly ? " (stopped early on wall-clock budget — remaining rows retry next run)" : ""}`);

  // 5. Append newly found leads to the sales sheet, then email Liv a preview
  //    (?to= redirects the notification during testing so Liv isn't emailed).
  const sheetResult = await appendToSheetAndNotify(db, opts?.notifyTo);

  return {
    rows: rows.length, processed, inserted, domains: domainCache.size, stoppedEarly,
    provider: state.useApify ? "apify" : "hunter", apifyDomains: state.apifyDomainsUsed,
    acEmails: acEmails.size, acLiveFallback: acTableEmpty, ...sheetResult,
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
  const previewRows = leads.slice(0, 20).map((r) => {
    const name = [r.poc_firstname, r.poc_lastname].filter(Boolean).join(" ") || "—";
    const ctx = r.lead_type === "soc med" && (r.event || r.speaker)
      ? `${esc(r.event ?? "")}${r.speaker ? " · " + esc(r.speaker) : ""}` : "";
    return `<tr>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(r.org ?? "—")}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(name)}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(r.poc_role ?? "—")}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee"><a href="mailto:${esc(r.email ?? "")}">${esc(r.email ?? "—")}</a></td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${esc(r.lead_type ?? "—")}</td>
      <td style="padding:6px 10px;border-bottom:1px solid #eee">${ctx || "—"}</td>
    </tr>`;
  }).join("");
  const more = leads.length > 20 ? `<p style="color:#666;font-size:13px">…and ${leads.length - 20} more in the sheet.</p>` : "";
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
    <p style="margin:18px 0">
      <a href="${SHEET_URL}" style="background:#1a73e8;color:#fff;text-decoration:none;padding:10px 18px;border-radius:6px;display:inline-block">
        Open the sheet for review →</a>
    </p>
    <p style="color:#888;font-size:12px">Automated by the Alternate Lead Finder · tab: ${esc(SHEET_TAB)}</p>
  </div>`;
}

async function appendToSheetAndNotify(db: SupabaseClient, toOverride?: string): Promise<Record<string, unknown>> {
  const result: Record<string, unknown> = { appended: 0, emailed: false };
  const recipient = toOverride || NOTIFY_TO;

  // Rows queued for the sheet (this run's inserts + any prior unadded).
  const { data: pending, error } = await db
    .from("alternate_leads")
    .select("id, created_at, lead_type, org, poc_firstname, poc_lastname, poc_role, email, speaker, event, subject, source_table, thread_id")
    .or("is_added_to_sheet.is.null,is_added_to_sheet.eq.false")
    .order("id", { ascending: true });
  if (error) { result.sheetError = error.message; return result; }
  const leads = (pending ?? []) as AltRow[];
  if (leads.length === 0) { console.log("No new leads to append."); return result; }

  // --- Append to Google Sheet ---
  try {
    const token = await getGoogleAccessToken("https://www.googleapis.com/auth/spreadsheets");
    await sheetsEnsureTabWithHeader(token, SHEET_HEADER);
    const header = await sheetsGetRow1(token);
    const rows = leads.map((r) => header.map((h) => valueForHeader(h, r)));
    await sheetsAppend(token, rows);

    const ids = leads.map((r) => r.id);
    const { error: updErr } = await db.from("alternate_leads").update({ is_added_to_sheet: true }).in("id", ids);
    if (updErr) console.warn(`Failed to flag is_added_to_sheet: ${updErr.message}`);
    result.appended = leads.length;
    console.log(`Appended ${leads.length} lead(s) to ${SHEET_TAB}.`);
  } catch (err) {
    console.error(`Sheet append failed (leads left queued): ${err}`);
    result.sheetError = String(err);
    return result; // don't email if the sheet write didn't happen
  }

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
    const input = `What is the official name of the organization that owns the website `
      + `domain ${domain}?` + (hint ? ` A known contact there: ${hint}.` : "")
      + ` Reply with ONLY the organization name and nothing else. If you cannot `
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
  const { error } = await db.from("closed_sequence_threads").update({ is_processed: true }).eq("id", id);
  if (error) console.warn(`markProcessed(${id}) failed: ${error.message}`);
}

Deno.serve(async (req: Request) => {
  const params = new URL(req.url).searchParams;
  const limitParam = params.get("limit");
  const limit = limitParam ? parseInt(limitParam, 10) : undefined;

  // Debug: ?append=1 runs ONLY the sheet-append + email step against already
  // queued alternate_leads (is_added_to_sheet false/null). No Hunter calls.
  if (params.get("append") === "1") {
    try {
      const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });
      const res = await appendToSheetAndNotify(db, params.get("to") ?? undefined);
      return Response.json({ status: "done", ...res });
    } catch (err) {
      return Response.json({ status: "error", error: String(err) }, { status: 500 });
    }
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

  // Debug: ?probe=<domain> reports what the runtime sees (key length + raw
  // Hunter HTTP status/body snippet), to diagnose credential/egress issues.
  if (params.get("probe")) {
    const dom = params.get("probe")!;
    const info: Record<string, unknown> = { hunterKeyLen: HUNTER_API_KEY.length };
    try {
      const r = await fetch(`https://api.hunter.io/v2/domain-search?domain=${encodeURIComponent(dom)}&limit=3&api_key=${HUNTER_API_KEY}`);
      const body = await r.text();
      info.status = r.status;
      info.bodySnippet = body.slice(0, 200);
    } catch (err) {
      info.fetchError = String(err);
    }
    return Response.json(info);
  }

  // Debug: ?aprobe=<domain> runs the Apify fallback actor directly and reports the
  // raw HTTP status + a body snippet, to confirm the actor's field names/mapping.
  if (params.get("aprobe")) {
    const dom = params.get("aprobe")!;
    const info: Record<string, unknown> = { apifyKeyLen: APIFY_API_TOKEN.length, actor: APIFY_ACTOR };
    try {
      const r = await fetch(`https://api.apify.com/v2/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?token=${APIFY_API_TOKEN}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ domains: [dom], maxContactsPerDomain: APIFY_LIMIT, maxItems: APIFY_LIMIT, useGoogleFallback: true }),
      });
      const body = await r.text();
      info.status = r.status;
      info.bodySnippet = body.slice(0, 800);
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

  // ?provider=apify forces the Apify fallback path (skips Hunter) for testing.
  // ?to= redirects the notification email (use when testing so Liv isn't emailed).
  const forceApify = params.get("provider") === "apify";
  const notifyTo = params.get("to") ?? undefined;

  // Debug/testing: ?sync=1 awaits the work and returns the outcome (or error)
  // in the response, so failures are visible instead of vanishing in the bg task.
  if (params.get("sync") === "1") {
    try {
      const result = await run(limit, { forceApify, notifyTo });
      return Response.json({ status: "done", ...result });
    } catch (err) {
      console.error(`run() crashed: ${err}`);
      return Response.json({ status: "error", error: String(err), stack: (err as Error)?.stack }, { status: 500 });
    }
  }

  const work = run(limit, { forceApify, notifyTo }).catch((err) => console.error(`run() crashed: ${err}`));
  if (typeof EdgeRuntime !== "undefined") {
    EdgeRuntime.waitUntil(work);
  }
  return new Response(JSON.stringify({ status: "started" }), {
    status: 202,
    headers: { "Content-Type": "application/json" },
  });
});
