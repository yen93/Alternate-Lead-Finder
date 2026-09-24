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

async function hunterDomainSearch(domain: string): Promise<HunterResult | null> {
  if (!HUNTER_API_KEY) {
    console.warn("HUNTER_IO_API_KEY not set — cannot search domains.");
    return null;
  }
  // limit=10 is the Free plan's max page size (higher values return HTTP 400).
  const url = `https://api.hunter.io/v2/domain-search?domain=${encodeURIComponent(domain)}&limit=10&api_key=${HUNTER_API_KEY}`;
  try {
    const res = await fetch(url);
    if (res.status === 429) {
      console.warn(`Hunter 429 (rate limited) for ${domain} — skipping.`);
      return null;
    }
    if (!res.ok) {
      console.warn(`Hunter ${res.status} for ${domain} — skipping.`);
      return null;
    }
    const json = await res.json();
    const data = json?.data ?? {};
    return {
      organization: data.organization ?? null,
      emails: Array.isArray(data.emails) ? data.emails : [],
    };
  } catch (err) {
    console.warn(`Hunter fetch failed for ${domain}: ${err}`);
    return null;
  }
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

async function run(limit?: number): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });

  // 1. Unresponsive leads to process. Optional `limit` caps the batch (testing).
  let query = db
    .from("closed_sequence_threads")
    .select("id, thread_id, source_table, email")
    .not("email", "is", null)
    .or("is_processed.is.null,is_processed.eq.false")
    .order("id", { ascending: true });
  if (limit && limit > 0) query = query.limit(limit);
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

  const domainCache = new Map<string, HunterResult | null>();
  const insertedThisRun = new Set<string>();
  let inserted = 0;
  let processed = 0;

  for (const row of rows) {
    const email = row.email as string;
    const domain = domainOf(email);
    if (!domain) {
      await markProcessed(db, row.id);
      continue;
    }

    let result = domainCache.get(domain);
    if (result === undefined) {
      result = await hunterDomainSearch(domain);
      domainCache.set(domain, result);
      await sleep(HUNTER_DELAY_MS);
    }

    if (result === null) {
      // Hunter errored / rate-limited / no key — leave unprocessed so it retries
      // next run rather than silently burning the row.
      console.warn(`No Hunter result for ${domain} — leaving row ${row.id} unprocessed for retry.`);
      continue;
    }

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
      const org = result.organization ?? sm?.org ?? null;

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
      }
    }

    // 4. Mark the source row processed (Hunter responded; nothing more to retry).
    await markProcessed(db, row.id);
    processed++;
  }

  console.log(`Inserted ${inserted} alternate lead(s) across ${domainCache.size} domain(s).`);

  // 5. Append newly found leads to the sales sheet, then email Liv a preview.
  const sheetResult = await appendToSheetAndNotify(db);

  return {
    rows: rows.length, processed, inserted, domains: domainCache.size,
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

  // Debug/testing: ?sync=1 awaits the work and returns the outcome (or error)
  // in the response, so failures are visible instead of vanishing in the bg task.
  if (params.get("sync") === "1") {
    try {
      const result = await run(limit);
      return Response.json({ status: "done", ...result });
    } catch (err) {
      console.error(`run() crashed: ${err}`);
      return Response.json({ status: "error", error: String(err), stack: (err as Error)?.stack }, { status: 500 });
    }
  }

  const work = run(limit).catch((err) => console.error(`run() crashed: ${err}`));
  if (typeof EdgeRuntime !== "undefined") {
    EdgeRuntime.waitUntil(work);
  }
  return new Response(JSON.stringify({ status: "started" }), {
    status: 202,
    headers: { "Content-Type": "application/json" },
  });
});
