// alternate-lead-enrichment
// Weekday (Mon-Fri 2:00 PM PHT) automation. Finds the EMAIL (and, when it comes free in
// the same lookup, the phone number) for alternate_leads that Liv has approved for intro
// but that still have no email.
//
// Queue (kept EXACTLY as specified by the enrichment prompt):
//   select * from alternate_leads
//   where status = 'Send intro' and is_processed is null and email is null
// The `status` column is populated by Liv's review step, so this function correctly
// processes NOTHING until a lead is approved - that is by design, not a bug.
//
// Providers: Hunter.io (main) -> Lusha V3 (backup), per lead. EMAIL is the priority.
//   - Hunter returns email + phone_number in one response, so the phone is saved for free.
//   - Lusha bills phone reveal as a separate field, so we request reveal:["emails"] only
//     (prio email; skip phone when it would cost extra - per the prompt).
//
// Mark semantics (no row burned without a real check):
//   - email found (either provider)            -> email (+ phone if free), is_processed = true
//   - BOTH providers answered "nothing found"  -> is_processed = true (one-shot; reset to
//                                                 null to retry) so we don't re-burn credits
//   - a provider we relied on ERRORED / quota  -> is_processed stays null, lead retries next run
//   - lead has no first/last name              -> skipped, left unprocessed (no provider call)
//
// Triggered by pg_cron (net.http_post); kicks the real work into a background task and
// returns 202 immediately. Debug params:
//   ?sync=1      await the work and return the summary/error in the response
//   ?limit=N     lower the per-run cap (never raises it above ENRICH_MAX_PER_RUN)
//   ?dry=1       list which leads WOULD be enriched + resolved domain/name, make ZERO
//                provider calls (safe pre-flight that does not touch the creds)

import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

declare const EdgeRuntime: { waitUntil(p: Promise<unknown>): void } | undefined;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

// Providers. HUNTER_IO_API_KEY already lives in the function secrets; LUSHA_API_KEY must
// be added (storing a secret does not call the API).
const HUNTER_API_KEY = Deno.env.get("HUNTER_IO_API_KEY") ?? "";
const LUSHA_API_KEY = Deno.env.get("LUSHA_API_KEY") ?? "";

// Per-run cap. Hunter's FREE plan is ~50 domain searches/MONTH; at ~22 weekday runs/month
// a cap of 10 can exceed the free tier if every run is full. Lower ENRICH_MAX_PER_RUN (or
// upgrade the Hunter plan) to stay within quota. `?limit=N` can only LOWER this.
const ENRICH_MAX_PER_RUN = parseInt(Deno.env.get("ENRICH_MAX_PER_RUN") ?? "10", 10);

// Accept a Hunter email only when its deliverability score is >= this (0 = accept any
// email Hunter returns). Lusha emails are accepted as-is.
const HUNTER_MIN_SCORE = parseInt(Deno.env.get("HUNTER_MIN_SCORE") ?? "0", 10);

// Wall-clock guard. The isolate is hard-killed ~150s; each lead's DB write happens inline
// (no final flush), but we still stop starting new leads near the budget.
const WALL_CLOCK_MS = parseInt(Deno.env.get("WALL_CLOCK_MS") ?? "150000", 10);
const WALL_CLOCK_RESERVE_MS = parseInt(Deno.env.get("WALL_CLOCK_RESERVE_MS") ?? "8000", 10);

// Per-call network timeout for a single provider request.
const PROVIDER_TIMEOUT_MS = parseInt(Deno.env.get("PROVIDER_TIMEOUT_MS") ?? "20000", 10);

// Free / generic mailbox domains: for these the original lead's email domain is NOT the
// company domain, so Hunter is searched by company NAME instead and Lusha gets companyName only.
const FREEMAIL = new Set([
  "gmail.com", "googlemail.com", "yahoo.com", "yahoo.com.au", "ymail.com", "hotmail.com",
  "hotmail.com.au", "outlook.com", "outlook.com.au", "live.com", "live.com.au", "icloud.com",
  "me.com", "aol.com", "bigpond.com", "bigpond.net.au", "optusnet.com.au", "iinet.net.au",
  "protonmail.com", "proton.me", "gmx.com", "mail.com", "msn.com",
]);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const domainOf = (email: string) => (email.includes("@") ? email.split("@")[1].trim().toLowerCase() : "");

interface Lead {
  id: number;
  poc_firstname: string | null;
  poc_lastname: string | null;
  org: string | null;
  thread_id: string | null;
}

interface Found { email: string | null; phone: string | null; score?: number | null }
// A provider attempt is either: a found result, a definitive "nothing" (ok:true,email:null),
// or an error (ok:false) meaning we could NOT check (leave the lead for retry).
interface Attempt { ok: boolean; email: string | null; phone: string | null; score?: number | null; note?: string }

// fetch with an AbortController timeout.
async function fetchT(url: string, init: RequestInit): Promise<Response> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), PROVIDER_TIMEOUT_MS);
  try {
    return await fetch(url, { ...init, signal: ctl.signal });
  } finally {
    clearTimeout(t);
  }
}

// --- Hunter.io (main) -------------------------------------------------------
// GET /v2/email-finder with a domain (or company) + first/last name. Returns the email,
// a deliverability `score`, and `phone_number` (null unless Hunter holds one) in ONE
// response, so the phone costs nothing extra. Auth via X-API-KEY header.
// Returns ok:false on any transport/auth error so the lead is retried, not burned.
async function hunterFind(domain: string | null, company: string | null, first: string, last: string): Promise<Attempt> {
  if (!HUNTER_API_KEY) return { ok: false, email: null, phone: null, note: "no HUNTER_IO_API_KEY" };
  const qs = new URLSearchParams({ first_name: first, last_name: last });
  if (domain) qs.set("domain", domain);
  else if (company) qs.set("company", company);
  else return { ok: false, email: null, phone: null, note: "no domain/company" };
  try {
    const res = await fetchT(`https://api.hunter.io/v2/email-finder?${qs.toString()}`, {
      headers: { "X-API-KEY": HUNTER_API_KEY, Accept: "application/json" },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const code = body?.errors?.[0]?.code ?? res.status;
      const details = body?.errors?.[0]?.details ?? "";
      return { ok: false, email: null, phone: null, note: `hunter ${code} ${String(details).slice(0, 120)}` };
    }
    const d = body?.data ?? {};
    const email = typeof d.email === "string" && d.email.trim() ? d.email.trim() : null;
    const score = typeof d.score === "number" ? d.score : null;
    const phone = typeof d.phone_number === "string" && d.phone_number.trim() ? d.phone_number.trim() : null;
    if (email && (score ?? 0) < HUNTER_MIN_SCORE) {
      return { ok: true, email: null, phone, score, note: `below min score (${score})` };
    }
    return { ok: true, email, phone, score };
  } catch (err) {
    return { ok: false, email: null, phone: null, note: `hunter fetch failed: ${err}` };
  }
}

// --- Lusha V3 (backup) ------------------------------------------------------
// POST /v3/contacts/search-and-enrich with reveal:["emails"] (email only - phone is billed
// as a separate field, so we skip it per the prompt). Auth via api_key header.
async function lushaFind(domain: string | null, company: string | null, first: string, last: string): Promise<Attempt> {
  if (!LUSHA_API_KEY) return { ok: true, email: null, phone: null, note: "no LUSHA_API_KEY (backup not configured)" };
  const contact: Record<string, string> = { clientReferenceId: "1", firstName: first, lastName: last };
  if (company) contact.companyName = company;
  if (domain) contact.companyDomain = domain;
  if (!company && !domain) return { ok: false, email: null, phone: null, note: "no domain/company" };
  try {
    const res = await fetchT("https://api.lusha.com/v3/contacts/search-and-enrich", {
      method: "POST",
      headers: { api_key: LUSHA_API_KEY, "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ contacts: [contact], reveal: ["emails"] }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      return { ok: false, email: null, phone: null, note: `lusha ${res.status} ${JSON.stringify(body).slice(0, 120)}` };
    }
    const r = (body?.results ?? [])[0] ?? {};
    if (r?.error?.code) {
      // A per-record NOT_FOUND/COMPLIANCE_RESTRICTED is a definitive "nothing" (ok:true),
      // not a transport failure - we DID get an answer.
      return { ok: true, email: null, phone: null, note: `lusha result ${r.error.code}` };
    }
    const emails: Record<string, unknown>[] = Array.isArray(r.emails) ? r.emails : [];
    const work = emails.find((e) => (e.type as string) === "work" && typeof e.email === "string" && (e.email as string).trim());
    const any = emails.find((e) => typeof e.email === "string" && (e.email as string).trim());
    const email = ((work ?? any)?.email as string | undefined)?.trim() ?? null;
    // We requested emails only, but if a phone rides along for free, keep it.
    const phones: Record<string, unknown>[] = Array.isArray(r.phones) ? r.phones : [];
    const phone = (phones.find((p) => typeof p.number === "string" && (p.number as string).trim())?.number as string | undefined)?.trim() ?? null;
    return { ok: true, email, phone };
  } catch (err) {
    return { ok: false, email: null, phone: null, note: `lusha fetch failed: ${err}` };
  }
}

// Original company domain for each alternate lead, via thread_id -> closed_sequence_threads.email.
async function loadDomains(db: SupabaseClient, threadIds: string[]): Promise<Map<string, string>> {
  const map = new Map<string, string>();
  if (threadIds.length === 0) return map;
  const { data, error } = await db
    .from("closed_sequence_threads")
    .select("thread_id, email")
    .in("thread_id", threadIds);
  if (error) { console.warn(`loadDomains error: ${error.message}`); return map; }
  for (const r of (data ?? []) as Record<string, unknown>[]) {
    const tid = r.thread_id as string;
    const d = domainOf((r.email as string) ?? "");
    if (tid && d && !map.has(tid)) map.set(tid, d);
  }
  return map;
}

interface RunState {
  hunterDown: boolean; // 401/429 etc. -> stop calling Hunter for the rest of the run
  lushaDown: boolean;
}

async function run(limit?: number, opts?: { dry?: boolean }): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });
  const runStarted = Date.now();
  const dry = Boolean(opts?.dry);

  const cap = limit && limit > 0 ? Math.min(limit, ENRICH_MAX_PER_RUN) : ENRICH_MAX_PER_RUN;

  // 1. Queue - EXACTLY as specified. Do not change these predicates.
  const { data: leads, error: qErr } = await db
    .from("alternate_leads")
    .select("id, poc_firstname, poc_lastname, org, thread_id")
    .eq("status", "Send intro")
    .is("is_processed", null)
    .is("email", null)
    .order("id", { ascending: true })
    .limit(cap);
  if (qErr) { console.error(`Query alternate_leads failed: ${qErr.message}`); return { error: qErr.message }; }
  const rows = (leads ?? []) as Lead[];
  console.log(`${dry ? "[dry] " : ""}Enrichment queue: ${rows.length} lead(s).`);
  if (rows.length === 0) return { queue: 0, enriched: 0, dry };

  const threadIds = [...new Set(rows.map((r) => r.thread_id).filter(Boolean))] as string[];
  const domainByThread = await loadDomains(db, threadIds);

  // Resolve per-lead search inputs (domain vs company) up front.
  const prepared = rows.map((lead) => {
    const first = (lead.poc_firstname ?? "").trim();
    const last = (lead.poc_lastname ?? "").trim();
    const company = (lead.org ?? "").trim() || null;
    const rawDomain = lead.thread_id ? (domainByThread.get(lead.thread_id) ?? null) : null;
    // Freemail domains are personal, not the company - search by company name instead.
    const domain = rawDomain && !FREEMAIL.has(rawDomain) ? rawDomain : null;
    return { lead, first, last, company, domain, rawDomain, hasName: Boolean(first && last) };
  });

  if (dry) {
    return {
      dry: true,
      queue: rows.length,
      wouldEnrich: prepared.filter((p) => p.hasName).length,
      skippedNoName: prepared.filter((p) => !p.hasName).length,
      leads: prepared.map((p) => ({
        id: p.lead.id,
        name: `${p.first} ${p.last}`.trim() || null,
        company: p.company,
        searchDomain: p.domain,
        originalDomain: p.rawDomain,
        hasName: p.hasName,
      })),
      hunterConfigured: Boolean(HUNTER_API_KEY),
      lushaConfigured: Boolean(LUSHA_API_KEY),
    };
  }

  const state: RunState = { hunterDown: false, lushaDown: false };
  let enriched = 0, phones = 0, noResult = 0, errored = 0, skipped = 0;

  for (const p of prepared) {
    if (Date.now() - runStarted > WALL_CLOCK_MS - WALL_CLOCK_RESERVE_MS) {
      console.warn("Wall-clock budget nearly spent - remaining leads retry next run.");
      break;
    }
    if (!p.hasName) {
      console.warn(`Lead ${p.lead.id} has no first/last name - skipping (left unprocessed, no provider call).`);
      skipped++;
      continue;
    }

    // --- Hunter (main) ---
    let result: Found | null = null;
    let anyProviderError = false;

    if (!state.hunterDown) {
      const h = await hunterFind(p.domain, p.company, p.first, p.last);
      if (h.ok) {
        if (h.email) result = { email: h.email, phone: h.phone, score: h.score };
        else if (h.phone && !result) result = { email: null, phone: h.phone }; // keep a free phone even if no email yet
      } else {
        anyProviderError = true;
        // Systemic auth/quota failures: stop hammering Hunter for the rest of the run.
        if (/hunter (401|403|429)/.test(h.note ?? "")) {
          state.hunterDown = true;
          console.warn(`Hunter disabled for this run: ${h.note}`);
        } else {
          console.warn(`Hunter error for lead ${p.lead.id}: ${h.note}`);
        }
      }
    }

    // --- Lusha (backup) - only if we still have no email ---
    if (!result?.email && !state.lushaDown) {
      const l = await lushaFind(p.domain, p.company, p.first, p.last);
      if (l.ok) {
        if (l.email) result = { email: l.email, phone: l.phone ?? result?.phone ?? null };
      } else {
        anyProviderError = true;
        if (/lusha (401|402|403)/.test(l.note ?? "")) {
          state.lushaDown = true;
          console.warn(`Lusha disabled for this run: ${l.note}`);
        } else {
          console.warn(`Lusha error for lead ${p.lead.id}: ${l.note}`);
        }
      }
    }

    // --- Decide & write ---
    if (result?.email) {
      const update: Record<string, unknown> = { email: result.email, is_processed: true };
      if (result.phone) update.phone_number = result.phone;
      const { error: uErr } = await db.from("alternate_leads").update(update).eq("id", p.lead.id);
      if (uErr) { console.warn(`Update failed for lead ${p.lead.id}: ${uErr.message}`); errored++; continue; }
      enriched++;
      if (result.phone) phones++;
      console.log(`Lead ${p.lead.id}: email found${result.phone ? " + phone" : ""}.`);
    } else if (anyProviderError) {
      // We could NOT fully check (a provider we relied on errored) - leave unprocessed to retry.
      errored++;
      console.log(`Lead ${p.lead.id}: left unprocessed (provider error) - will retry.`);
    } else {
      // Both providers answered "nothing" - one-shot: mark processed so we don't re-burn credits.
      const update: Record<string, unknown> = { is_processed: true };
      if (result?.phone) update.phone_number = result.phone; // free phone with no email - still keep it
      const { error: uErr } = await db.from("alternate_leads").update(update).eq("id", p.lead.id);
      if (uErr) { console.warn(`Mark-processed failed for lead ${p.lead.id}: ${uErr.message}`); errored++; continue; }
      noResult++;
      if (result?.phone) phones++;
      console.log(`Lead ${p.lead.id}: no email found - marked processed (reset is_processed=null to retry).`);
    }
  }

  const summary = { queue: rows.length, enriched, phones, noResult, errored, skipped, hunterDown: state.hunterDown, lushaDown: state.lushaDown, dry: false };
  console.log(`Done: ${JSON.stringify(summary)}`);
  return summary;
}

Deno.serve(async (req: Request) => {
  const params = new URL(req.url).searchParams;
  const limitParam = params.get("limit");
  const limit = limitParam ? parseInt(limitParam, 10) : undefined;
  const dry = params.get("dry") === "1";

  if (dry || params.get("sync") === "1") {
    try {
      const result = await run(limit, { dry });
      return Response.json({ status: "done", ...result });
    } catch (err) {
      console.error(`run() crashed: ${err}`);
      return Response.json({ status: "error", error: String(err), stack: (err as Error)?.stack }, { status: 500 });
    }
  }

  const work = run(limit).catch((err) => console.error(`run() crashed: ${err}`));
  if (typeof EdgeRuntime !== "undefined") EdgeRuntime.waitUntil(work);
  return new Response(JSON.stringify({ status: "started" }), {
    status: 202,
    headers: { "Content-Type": "application/json" },
  });
});
