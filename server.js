const express = require("express");
const cors = require("cors");
const BetterSqlite3 = require('better-sqlite3');
const fs = require('fs');

function saveDb() {} // no-op: better-sqlite3 writes to disk automatically

function initDb(dbPath) {
  // Ensure directory exists
  const dir = require('path').dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  let db;
  try {
    db = new BetterSqlite3(dbPath);
    db.pragma('journal_mode = WAL');  // WAL mode: safe concurrent reads + writes
    db.pragma('integrity_check');
    console.log("[db] Database loaded and verified OK.");
  } catch(e) {
    console.error(`[db] Database corrupt (${e.message}) — deleting and starting fresh.`);
    try { fs.unlinkSync(dbPath); } catch(_) {}
    db = new BetterSqlite3(dbPath);
    db.pragma('journal_mode = WAL');
    console.log("[db] Fresh database created — full TV sync will start.");
  }
  return db;
}

class Database {
  constructor(db) { this.db = db; }
  prepare(sql) { return this.db.prepare(sql); }
  exec(sql) { this.db.exec(sql); }
  transaction(fn) {
    const t = this.db.transaction(fn);
    return (items) => t(items);
  }
}
const fetch = require("node-fetch");
const cron = require("node-cron");
const path = require("path");

function lookupMunicipality() { return null; }


const app = express();
const PORT = process.env.PORT || 3001;
const ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || "";
const FRONTEND_URL = process.env.FRONTEND_URL || "*";
const AI_STALE_DAYS = parseInt(process.env.AI_STALE_DAYS || "100");

// -- DATABASE -----------------------------------------------------------------
const DB_PATH = process.env.DB_PATH || "/data/yourdomi.db";
let db; // initialized async below

// DB initialized in startServer()
const DB_SCHEMA = `
  CREATE TABLE IF NOT EXISTS properties (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    fetched_at INTEGER NOT NULL,
    name TEXT,
    municipality TEXT,
    province TEXT,
    status TEXT,
    slaapplaatsen INTEGER,
    phone TEXT,
    email TEXT,
    website TEXT,
    type TEXT,
    regio TEXT,
    date_online TEXT,
    postal_code TEXT,
    street TEXT
  );

  CREATE TABLE IF NOT EXISTS enrichment (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    enriched_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS outcomes (
    id TEXT PRIMARY KEY,
    outcome TEXT,
    note TEXT,
    contact_naam TEXT,
    updated_at INTEGER NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_enrichment_age ON enrichment(enriched_at);

  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT,
    created_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    name TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
  );
`;

const corsOptions = {
  origin: function(origin, callback) {
    callback(null, origin || "https://yourdomi-bellist.vercel.app");
  },
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "x-auth-token", "Authorization"],
  optionsSuccessStatus: 204,
};
app.use(cors(corsOptions));
app.options("*", cors(corsOptions)); // handle all preflight requests
app.use(express.json({ limit: "2mb" }));

// -- AUTH HELPERS --------------------------------------------------------------
const crypto = require("crypto");

function hashPassword(password) {
  return crypto.createHash("sha256").update(password + "yourdomi_salt_2025").digest("hex");
}

function generateToken() {
  return crypto.randomBytes(32).toString("hex");
}

function requireAuth(req, res, next) {
  const token = req.headers["x-auth-token"];
  if (!token) return res.status(401).json({ error: "Niet ingelogd" });
  const session = db.prepare("SELECT * FROM sessions WHERE token = ? AND expires_at > ?").get(token, Date.now());
  if (!session) return res.status(401).json({ error: "Sessie verlopen" });
  req.user = { id: session.user_id, username: session.username, name: session.name };
  next();
}

function ensureDefaultUsers() {
  const now = Date.now();
  const defaults = [
    { username: "aaron", name: "Aaron" },
    { username: "ruben", name: "Ruben" },
    { username: "vic",   name: "Vic"   },
  ];
  for (const u of defaults) {
    const existing = db.prepare("SELECT id FROM users WHERE username = ?").get(u.username);
    if (!existing) {
      db.prepare("INSERT INTO users (username, password_hash, name, created_at) VALUES (?, ?, ?, ?)").run(
        u.username, hashPassword("yourdomi2026"), u.name, now
      );
      console.log(`[auth] Created user: ${u.username}`);
    } else {
      db.prepare("UPDATE users SET password_hash = ?, name = ? WHERE username = ?").run(
        hashPassword("yourdomi2026"), u.name, u.username
      );
      console.log(`[auth] Updated user: ${u.username}`);
    }
  }
  // persist user records so login works after restart
}

// -- TOERISME VLAANDEREN FETCH -------------------------------------------------
const TV_BASE = "https://linked.toerismevlaanderen.be/lodgings";

async function fetchPageFromTV(page = 1, size = 100) {
  const url = `https://linked.toerismevlaanderen.be/lodgings?page[size]=${size}&page[number]=${page}`;
  const res = await fetch(url, {
    headers: { Accept: "application/vnd.api+json", "User-Agent": "YourdomiServer/1.0" },
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`TV API ${res.status}: ${await res.text()}`);
  return res.json();
}

async function syncPropertiesFromTV() {
  console.log("[sync] Starting fast parallel sync from Toerisme Vlaanderen...");
  const PAGE_SIZE = 50;   // smaller pages = less memory per batch
  const CONCURRENCY = 2;  // low concurrency to avoid OOM on Railway
  const now = Date.now();

  const insert = db.prepare(
    "INSERT OR REPLACE INTO properties (id, data, fetched_at, name, municipality, province, status, slaapplaatsen, phone, email, website, type, regio, date_online, postal_code, street) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );
  // Process items one at a time — never hold entire batch in memory
  function insertItem(item) {
    const parsed = parseLodging(item.raw, item.included);
    // Only store minimal raw data — drop included to save space
    const minimal = { raw: item.raw, included: [] };
    insert.run(
      item.id,
      JSON.stringify(minimal),
      now,
      parsed.name || "",
      parsed.municipality || "",
      parsed.province || "",
      parsed.status || "",
      parsed.slaapplaatsen || 0,
      parsed.phone || "",
      parsed.email || "",
      parsed.website || "",
      parsed.type || "",
      parsed.toeristischeRegio || "",
      parsed.dateOnline || "",
      parsed.postalCode || "",
      parsed.street || ""
    );
  }
  const insertMany = db.transaction((items) => {
    for (const item of items) insertItem(item);
  });

  try {
    // Step 1: fetch page 1 to get total count
    const firstPage = await fetchPageFromTV(1, PAGE_SIZE);
    const total = firstPage.meta?.count || firstPage.meta?.total || 0;
    if (!total) { console.log("[sync] No properties found."); return; }
    const totalPages = Math.ceil(total / PAGE_SIZE);
    console.log(`[sync] Total: ${total} properties across ${totalPages} pages — ${CONCURRENCY} parallel`);

    // Store first page
    const firstItems = (firstPage.data || []).map(item => ({ id: item.id, raw: item, included: firstPage.included || [] }));
    insertMany(firstItems);
    let synced = firstItems.length;

    // Step 2: remaining pages in parallel batches
    const remainingPages = Array.from({ length: totalPages - 1 }, (_, i) => i + 2);

    for (let i = 0; i < remainingPages.length; i += CONCURRENCY) {
      const batch = remainingPages.slice(i, i + CONCURRENCY);
      const results = await Promise.allSettled(batch.map(p => fetchPageFromTV(p, PAGE_SIZE)));

      const toStore = [];
      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        if (r.status === "fulfilled") {
          (r.value.data || []).forEach(item => toStore.push({ id: item.id, raw: item, included: r.value.included || [] }));
        } else {
          console.warn(`[sync] Page ${batch[j]} failed: ${r.reason?.message} — retrying`);
          try {
            const retry = await fetchPageFromTV(batch[j], PAGE_SIZE);
            (retry.data || []).forEach(item => toStore.push({ id: item.id, raw: item, included: retry.included || [] }));
          } catch(e) { console.error(`[sync] Page ${batch[j]} retry failed: ${e.message}`); }
        }
      }

      insertMany(toStore);
      synced += toStore.length;
      const pct = Math.round(synced / total * 100);
      console.log(`[sync] Pages ${batch[0]}-${batch[batch.length-1]}: ${synced}/${total} (${pct}%)`);

      // Release memory and yield between batches
      if (global.gc) global.gc();
      await new Promise(r => setTimeout(r, 500));
    }

    console.log(`[sync] Done. Synced ${synced} properties.`);
  } catch (e) {
    console.error("[sync] Error:", e.message);
  }
}

// -- PARSE LODGING (same logic as frontend) -----------------------------------
const s = (v) => {
  if (!v && v !== 0) return "";
  if (typeof v === "string") return v;
  if (Array.isArray(v)) {
    const first = v[0];
    if (!first) return "";
    if (typeof first === "string") return first;
    if (typeof first === "object") return first.content || first.value || first.text || first.name || "";
    return String(first);
  }
  if (typeof v === "object") return v.content || v.value || v.text || v.name || "";
  return String(v);
};
const n = (v) => isNaN(parseInt(v)) ? 0 : parseInt(v);

function parseLodging(raw, included = []) {
  const attr = raw.attributes || {};
  const rel = raw.relationships || {};

  // Name
  let name = attr["name"] || attr["schema:name"] || attr["alternative-name"] || attr["dcterms:title"] || "";

  let municipality = "", province = "", postalCode = "", toeristischeRegio = "";

  // --- MUNICIPALITY / PROVINCE / POSTALCODE ---
  // 1. Direct attributes (TV API v2 style)
  municipality = s(
    attr["municipality-name"] || attr["hoofdgemeente"] || attr["address-municipality"] ||
    attr["schema:address"]?.["schema:addressLocality"] || ""
  );
  province = s(
    attr["province"] || attr["provincie"] || attr["Provincie"] ||
    attr["schema:address"]?.["schema:addressRegion"] || ""
  );
  postalCode = s(
    attr["postal-code"] || attr["postcode"] || attr["postalCode"] ||
    attr["schema:address"]?.["schema:postalCode"] || ""
  );
  toeristischeRegio = s(attr["touristRegion"] || attr["toeristischeRegio"] || attr["tourist-region"] || "");

  // 2. Included relationships (TV API v1 style - municipality as related resource)
  if (!municipality && included.length) {
    const muniRef = rel.municipality?.data || rel["hoofdgemeente"]?.data;
    if (muniRef) {
      const muni = included.find((i) => i.type === muniRef.type && i.id === muniRef.id);
      if (muni) {
        municipality = s(muni?.attributes?.name || muni?.attributes?.["schema:name"] || "");
        const provRef = muni?.relationships?.province?.data;
        if (provRef) {
          const prov = included.find((i) => i.type === provRef.type && i.id === provRef.id);
          province = s(prov?.attributes?.name || "");
        }
        postalCode = s(muni?.attributes?.["postal-code"] || muni?.attributes?.postcode || "");
      }
    }
  }

  // --- STREET ---
  const street = s(
    attr["street"] || attr["straat"] || attr["address-street"] ||
    attr["schema:address"]?.["schema:streetAddress"] ||
    attr["schema:streetAddress"] || ""
  );

  // --- CONTACT: phone, email, website ---
  // TV API stores contacts in contact-points relationship (included resources)
  const contactPoints = rel["contact-points"]?.data || [];
  const phones = [], emails = [], websites = [];
  for (const cp of contactPoints) {
    const c = included.find(i => i.type === cp.type && i.id === cp.id);
    if (!c) continue;
    const ca = c.attributes || {};
    if (ca["schema:telephone"]) phones.push(s(ca["schema:telephone"]));
    if (ca["schema:email"])     emails.push(s(ca["schema:email"]));
    if (ca["schema:url"])       websites.push(s(ca["schema:url"]));
  }
  // Also try direct attributes (some TV API versions put contact info directly)
  const directPhones = [
    attr["schema:telephone"], attr["phone"], attr["telefoon"],
    attr["contact-phone"], attr["contactPhone"],
  ].filter(Boolean);
  for (const p of directPhones) { const v = s(p); if (v && !phones.includes(v)) phones.push(v); }

  if (!emails.length)   { const v = s(attr["schema:email"] || attr["email"] || attr["contact-email"] || ""); if (v) emails.push(v); }
  if (!websites.length) { const v = s(attr["schema:url"] || attr["website"] || attr["contact-website"] || ""); if (v) websites.push(v); }

  const cleanPhone = (p) => s(p).replace(/\s/g, "").replace(/^00/, "+").replace(/^\+?0032/, "+32");
  const phone = phones[0] ? cleanPhone(phones[0]) : null;
  const phone2 = phones[1] ? cleanPhone(phones[1]) : null;
  const phoneNorm = phone ? phone.replace(/[^0-9+]/g, "") : null;

  // Images
  const mediaRefs = rel.media?.data || rel["main-media"]?.data ? [rel["main-media"]?.data].filter(Boolean) : [];
  const allMediaRefs = (rel.media?.data || []).concat(mediaRefs).filter(Boolean);
  const images = allMediaRefs
    .map((m) => included.find(i => i.type === m.type && i.id === m.id)?.attributes?.contentUrl)
    .filter(Boolean)
    .slice(0, 5);

  // Slaapplaatsen
  const slaapplaatsen = parseInt(attr["number-of-sleeping-places"] || attr["schema:numberOfRooms"] || 0);

  // Status
  const status = attr["registration-status"] || attr.status || "";

  // Type
  const type = attr["category"] || attr["dcterms:type"] || attr["schema:additionalType"] || attr.type || "";

  // Date online
  const dateOnline = attr["modified"] || attr["registration-date"] || attr["dcterms:created"] || "";

  return {
    id: s(raw.id),
    name: s(name) || [s(street), s(municipality)].filter(Boolean).join(", ") || `Pand ${s(raw.id).slice(-8)}`,
    municipality: s(municipality),
    province: s(province),
    toeristischeRegio: s(toeristischeRegio),
    type: s(type),
    postalCode: s(postalCode),
    street: s(street),
    sleepPlaces: n(attr["number-of-sleeping-places"] || attr["schema:numberOfRooms"] || 0),
    slaapplaatsen: n(attr["number-of-sleeping-places"] || attr["schema:numberOfRooms"] || 0),
    units: n(attr["number-of-rental-units"] || 1),
    phone: s(phone) || null,
    phone2: s(phone2) || null,
    phoneNorm: s(phoneNorm) || null,
    email: s(emails[0]) || null,
    website: s(websites[0]) || null,
    images: images.map(s).filter(Boolean),
    status: s(status) || "aangemeld",
    starRating: null,
    onlineSince: s(attr["modified"] || attr["registration-date"] || ""),
    dateOnline: s(attr["modified"] || attr["registration-date"] || ""),
    registrationNumber: s(raw.id),
    category: s(attr["category"] || type || "vakantiewoning"),
    rawUrl: `https://linked.toerismevlaanderen.be/id/lodgings/${raw.id}`,
  };
}

// -- API ROUTES ----------------------------------------------------------------

// GET /api/panden
app.get("/api/panden", requireAuth, (req, res) => {
  try {
    const page = parseInt(req.query.page || "1");
    const size = Math.min(parseInt(req.query.size || "50"), 200);
    const { zoek, gemeente, provincie, status, minSlaap, maxSlaap, heeftTelefoon, heeftEmail, heeftWebsite } = req.query;

    const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    if (total === 0) return res.json({ data: [], meta: { total: 0, page, size }, _needsSync: true });

    // All filters use indexed columns only — no JSON_EXTRACT
    const conditions = [];
    const params = [];

    if (zoek) {
      const q = `%${zoek.toLowerCase()}%`;
      conditions.push("(LOWER(name) LIKE ? OR LOWER(municipality) LIKE ? OR postal_code LIKE ?)");
      params.push(q, q, `%${zoek}%`);
    }
    if (gemeente) {
      conditions.push("LOWER(municipality) LIKE ?");
      params.push(`%${gemeente.toLowerCase()}%`);
    }
    if (provincie) { conditions.push("province = ?"); params.push(provincie); }
    if (status)    { conditions.push("status = ?");   params.push(status); }
    if (minSlaap)  { conditions.push("slaapplaatsen >= ?"); params.push(parseInt(minSlaap)); }
    if (maxSlaap)  { conditions.push("slaapplaatsen <= ?"); params.push(parseInt(maxSlaap)); }
    if (req.query.type)  { conditions.push("type = ?");  params.push(req.query.type); }
    if (req.query.regio) { conditions.push("regio = ?"); params.push(req.query.regio); }
    if (heeftTelefoon === "1") { conditions.push("phone != '' AND phone IS NOT NULL"); }
    if (heeftEmail    === "1") { conditions.push("email != '' AND email IS NOT NULL"); }
    if (heeftWebsite  === "1") { conditions.push("website != '' AND website IS NOT NULL"); }

    const where   = conditions.length ? "WHERE " + conditions.join(" AND ") : "";
    const orderBy = req.query.sorteer === "nieuwste" ? "ORDER BY date_online DESC" : "";

    const filteredTotal = db.prepare(`SELECT COUNT(*) as c FROM properties ${where}`).get(...params).c;
    const offset = (page - 1) * size;
    const rows = db.prepare(`SELECT data FROM properties ${where} ${orderBy} LIMIT ? OFFSET ?`).all(...params, size, offset);

    const properties = rows.map(r => {
      try {
        const stored = JSON.parse(r.data);
        return parseLodging(stored.raw || stored, stored.included || []);
      } catch(e) {
        console.error("[panden] parseLodging error:", e.message);
        return null;
      }
    }).filter(Boolean);

    res.json({
      data: properties,
      meta: { total: filteredTotal, dbTotal: total, page, size, pages: Math.ceil(filteredTotal / size) },
    });
  } catch (e) {
    console.error("[/api/panden] error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/panden/count
app.get("/api/panden/count", requireAuth, (req, res) => {
  const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
  const lastFetch = db.prepare("SELECT MAX(fetched_at) as t FROM properties").get().t;
  res.json({ total, lastFetch });
});

// POST /api/panden/enrich-address — fetch full TV detail for a batch of IDs
// Body: { ids: ["id1","id2",...] }  (max 20 at a time)
// Fetches each property's individual URL, extracts address+phone, updates DB
app.post("/api/panden/enrich-address", requireAuth, async (req, res) => {
  const ids = (req.body?.ids || []).slice(0, 20);
  if (!ids.length) return res.json({ updated: [] });

  const results = [];
  await Promise.allSettled(ids.map(async (id) => {
    try {
      // Check if already enriched (phone or municipality filled)
      const existing = db.prepare("SELECT phone, municipality, data FROM properties WHERE id = ?").get(id);
      if (!existing) return;
      if (existing.phone || existing.municipality) {
        results.push({ id, skipped: true });
        return;
      }

      // Fetch individual property from TV API
      const url = `https://linked.toerismevlaanderen.be/id/lodgings/${id}`;
      const r = await fetch(url, {
        headers: { Accept: "application/vnd.api+json", "User-Agent": "YourdomiServer/1.0" },
        signal: AbortSignal.timeout(15000),
      });
      if (!r.ok) { results.push({ id, error: `TV ${r.status}` }); return; }
      const json = await r.json();

      // TV individual endpoint returns { data: {...}, included: [...] }
      const raw = json.data || json;
      const included = json.included || [];

      const parsed = parseLodging(raw, included);

      // Update the stored raw data with the richer individual response
      const enrichedStored = { raw, included };
      db.prepare(
        "UPDATE properties SET data=?, municipality=?, province=?, phone=?, email=?, website=?, postal_code=?, street=?, name=? WHERE id=?"
      ).run(
        JSON.stringify(enrichedStored),
        parsed.municipality || existing.municipality || "",
        parsed.province || "",
        parsed.phone || "",
        parsed.email || "",
        parsed.website || "",
        parsed.postalCode || "",
        parsed.street || "",
        parsed.name || "",
        id
      );
      results.push({
        id,
        name:         parsed.name,
        municipality: parsed.municipality,
        province:     parsed.province,
        postalCode:   parsed.postalCode,
        street:       parsed.street,
        phone:        parsed.phone,
        phone2:       parsed.phone2,
        email:        parsed.email,
        website:      parsed.website,
      });
    } catch(e) {
      results.push({ id, error: e.message });
    }
  }));

  res.json({ updated: results });
});

// GET /api/debug/raw — NO AUTH — shows raw stored TV API attributes
app.get("/api/debug/raw", (req, res) => {
  try {
    const rows = db.prepare("SELECT data FROM properties LIMIT 3").all();
    const samples = rows.map(r => {
      const stored = JSON.parse(r.data);
      const raw = stored.raw || stored;
      return { id: raw.id, attributeKeys: Object.keys(raw.attributes||{}), relationshipKeys: Object.keys(raw.relationships||{}), includedTypes: [...new Set((stored.included||[]).map(i=>i.type))], includedCount:(stored.included||[]).length, attributes: raw.attributes||{}, firstIncluded:(stored.included||[])[0]||null, parsed: parseLodging(raw, stored.included||[]) };
    });
    res.json(samples);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/debug/tv/:id — fetch live from TV and show raw response (no auth, for testing)
app.get("/api/debug/tv/:id", async (req, res) => {
  try {
    const url = `https://linked.toerismevlaanderen.be/id/lodgings/${req.params.id}`;
    const r = await fetch(url, {
      headers: { Accept: "application/vnd.api+json", "User-Agent": "YourdomiServer/1.0" },
      signal: AbortSignal.timeout(15000),
    });
    const json = await r.json();
    const raw = json.data || json;
    const included = json.included || [];
    res.json({
      attributeKeys: Object.keys(raw.attributes || {}),
      attributes: raw.attributes || {},
      relationshipKeys: Object.keys(raw.relationships || {}),
      includedCount: included.length,
      includedTypes: [...new Set(included.map(i => i.type))],
      included: included,
      parsed: parseLodging(raw, included),
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/backfill — re-parse all stored raw data and update indexed columns
app.post("/api/backfill", async (req, res) => {
  res.json({ ok: true, message: "Backfill started in background" });
  setTimeout(async () => {
    try {
      console.log("[backfill] Re-parsing all stored properties...");
      const rows = db.prepare("SELECT id, data FROM properties").all();
      const update = db.prepare("UPDATE properties SET name=?,municipality=?,province=?,status=?,slaapplaatsen=?,phone=?,email=?,website=?,type=?,regio=?,date_online=?,postal_code=? WHERE id=?");
      let count = 0;
      for (const row of rows) {
        try {
          const stored = JSON.parse(row.data);
          const p = parseLodging(stored.raw||stored, stored.included||[]);
          update.run(p.name,p.municipality,p.province,p.status,p.slaapplaatsen,p.phone,p.email,p.website,p.type,p.toeristischeRegio,p.dateOnline,p.postalCode,row.id);
          count++;
          if (count%1000===0){console.log("[backfill]",count,"/",rows.length);}
        } catch(e) { console.warn("[backfill] Error on",row.id,e.message); }
      }
            console.log("[backfill] Done. Updated",count,"properties.");
    } catch(e) { console.error("[backfill] Error:", e.message); }
  }, 100);
});

// GET /api/debug — NO AUTH — shows raw stored data so we can fix field names
app.get("/api/debug", (req, res) => {
  try {
    const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    const emptyMuni = db.prepare("SELECT COUNT(*) as c FROM properties WHERE municipality IS NULL OR municipality = ''").get().c;
    const filledMuni = total - emptyMuni;

    // Show raw attributes of first 3 properties — this tells us the exact TV API field names
    const rawRows = db.prepare("SELECT data FROM properties LIMIT 3").all();
    const rawSamples = rawRows.map(r => {
      const stored = JSON.parse(r.data);
      const raw = stored.raw || stored;
      const attr = raw.attributes || {};
      return {
        id: raw.id,
        allAttributeKeys: Object.keys(attr),
        schemaAddress: attr["schema:address"] || null,
        relationshipKeys: Object.keys(raw.relationships || {}),
        includedCount: (stored.included || []).length,
        includedTypes: [...new Set((stored.included || []).map(i => i.type))],
        firstIncluded: (stored.included || [])[0] || null,
        // Show all values for key candidates
        "attr.hoofdgemeente": attr["hoofdgemeente"],
        "attr.municipality-name": attr["municipality-name"],
        "attr.address-municipality": attr["address-municipality"],
        "attr.schema:address": attr["schema:address"],
        "rel.municipality": raw.relationships?.municipality || null,
      };
    });

    // Also test: does gemeente filter return anything?
    const testGent = db.prepare("SELECT COUNT(*) as c FROM properties WHERE LOWER(municipality) LIKE '%gent%'").get().c;
    const testKoksijde = db.prepare("SELECT COUNT(*) as c FROM properties WHERE LOWER(municipality) LIKE '%koksijde%'").get().c;
    const sampleMunicipalities = db.prepare("SELECT municipality, COUNT(*) as c FROM properties WHERE municipality != '' GROUP BY municipality ORDER BY c DESC LIMIT 20").all();

    res.json({ total, filledMuni, emptyMuni, testGent, testKoksijde, sampleMunicipalities, rawSamples });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// POST or GET /api/sync - trigger manual sync
app.post("/api/sync", async (req, res) => {
  res.json({ ok: true, message: "Sync started in background" });
  syncPropertiesFromTV().catch(console.error);
});
app.get("/api/sync", async (req, res) => {
  res.json({ ok: true, message: "Sync started in background" });
  syncPropertiesFromTV().catch(console.error);
});

// GET /api/enrichment/:id
app.get("/api/enrichment/:id", requireAuth, (req, res) => {
  const row = db.prepare("SELECT data, enriched_at FROM enrichment WHERE id = ?").get(req.params.id);
  if (!row) return res.json(null);
  res.json({ ...JSON.parse(row.data), _enrichedAt: row.enriched_at });
});

// GET /api/enrichment - get all (for bulk load)
app.get("/api/enrichment", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT id, data, enriched_at FROM enrichment").all();
  const result = {};
  for (const row of rows) {
    result[row.id] = { ...JSON.parse(row.data), _enrichedAt: row.enriched_at };
  }
  res.json(result);
});

// POST /api/enrichment/:id - save enrichment result
app.post("/api/enrichment/:id", requireAuth, (req, res) => {
  const data = req.body;
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Invalid data" });
  db.prepare("INSERT OR REPLACE INTO enrichment (id, data, enriched_at) VALUES (?, ?, ?)").run(
    req.params.id,
    JSON.stringify(data),
    Date.now()
  );
  res.json({ ok: true });
});

// GET /api/enrichment/stale - get IDs that need re-enrichment
app.get("/api/enrichment/stale", requireAuth, (req, res) => {
  const cutoff = Date.now() - AI_STALE_DAYS * 24 * 60 * 60 * 1000;
  // Properties with no enrichment or enrichment older than AI_STALE_DAYS
  const staleRows = db.prepare("SELECT id FROM enrichment WHERE enriched_at < ?").all(cutoff);
  const enrichedIds = new Set(db.prepare("SELECT id FROM enrichment").all().map((r) => r.id));
  const allIds = db.prepare("SELECT id FROM properties").all().map((r) => r.id);
  const unenriched = allIds.filter((id) => !enrichedIds.has(id));
  const stale = staleRows.map((r) => r.id);
  res.json({ stale: [...stale, ...unenriched].slice(0, 200) });
});

// GET /api/outcomes - all outcomes + notes
app.get("/api/outcomes", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT * FROM outcomes").all();
  const result = {};
  for (const row of rows) {
    result[row.id] = {
      outcome: row.outcome,
      note: row.note,
      contactNaam: row.contact_naam,
      updatedAt: row.updated_at,
    };
  }
  res.json(result);
});

// POST /api/outcomes/:id
app.post("/api/outcomes/:id", requireAuth, (req, res) => {
  const { outcome, note, contactNaam } = req.body;
  db.prepare(
    "INSERT OR REPLACE INTO outcomes (id, outcome, note, contact_naam, updated_at) VALUES (?, ?, ?, ?, ?)"
  ).run(req.params.id, outcome || null, note || null, contactNaam || null, Date.now());
  res.json({ ok: true });
});

// GET /api/health
app.get("/api/health", requireAuth, (req, res) => {
  const propCount = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
  const enrichCount = db.prepare("SELECT COUNT(*) as c FROM enrichment").get().c;
  const outcomeCount = db.prepare("SELECT COUNT(*) as c FROM outcomes").get().c;
  const lastFetch = db.prepare("SELECT MAX(fetched_at) as t FROM properties").get()?.t;
  res.json({
    ok: true,
    properties: propCount,
    enrichments: enrichCount,
    outcomes: outcomeCount,
    staleAfterDays: AI_STALE_DAYS,
    lastSync: lastFetch ? new Date(lastFetch).toISOString() : null,
  });
});

// GET /api/meta - unique provinces, types for filter dropdowns
app.get("/api/meta", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT data FROM properties").all();
  const provinces = new Set();
  const types = new Set();
  const regios = new Set();
  for (const r of rows) {
    try {
      const p = JSON.parse(r.data);
      const parsed = parseLodging(p.raw || p, p.included || []);
      if (parsed.province) provinces.add(parsed.province);
      if (parsed.type) types.add(parsed.type);
      if (parsed.toeristischeRegio) regios.add(parsed.toeristischeRegio);
    } catch {}
  }
  res.json({
    provinces: [...provinces].sort(),
    types: [...types].sort(),
    regios: [...regios].sort(),
  });
});

// -- CRON: weekly property sync, daily stale re-enrichment check ---------------
// Every Sunday at 03:00 - re-sync all properties from TV
cron.schedule("0 3 * * 0", () => {
  console.log("[cron] Weekly property sync starting...");
  syncPropertiesFromTV().catch(console.error);
});

// -- AUTH ENDPOINTS -----------------------------------------------------------
// Emergency reset - only works if ADMIN_PASSWORD env var is set in Railway
app.post("/api/admin/reset-password", (req, res) => {
  const adminPw = process.env.ADMIN_PASSWORD;
  if (!adminPw) return res.status(403).json({ error: "ADMIN_PASSWORD not configured" });
  const { adminPassword, username, newPassword } = req.body;
  if (adminPassword !== adminPw) return res.status(403).json({ error: "Wrong admin password" });
  if (!username || !newPassword) return res.status(400).json({ error: "username and newPassword required" });
  const user = db.prepare("SELECT id FROM users WHERE username = ?").get(username);
  if (!user) return res.status(404).json({ error: "User not found" });
  db.prepare("UPDATE users SET password_hash = ? WHERE username = ?").run(hashPassword(newPassword), username);
  db.prepare("DELETE FROM sessions WHERE username = ?").run(username); // invalidate old sessions
  res.json({ ok: true, message: `Password reset for ${username}` });
});


app.post("/api/login", (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Gebruikersnaam en wachtwoord vereist" });
  const user = db.prepare("SELECT * FROM users WHERE username = ?").get(username.toLowerCase().trim());
  if (!user || user.password_hash !== hashPassword(password)) {
    return res.status(401).json({ error: "Ongeldige gebruikersnaam of wachtwoord" });
  }
  const token = generateToken();
  const now = Date.now();
  const expires = now + 30 * 24 * 60 * 60 * 1000; // 30 days
  db.prepare("INSERT INTO sessions (token, user_id, username, name, created_at, expires_at) VALUES (?,?,?,?,?,?)").run(
    token, user.id, user.username, user.name || user.username, now, expires
  );
  res.json({ token, username: user.username, name: user.name || user.username });
});

app.post("/api/logout", (req, res) => {
  const token = req.headers["x-auth-token"];
  if (token) db.prepare("DELETE FROM sessions WHERE token = ?").run(token);
  res.json({ ok: true });
});

app.get("/api/me", requireAuth, (req, res) => {
  res.json({ username: req.user.username, name: req.user.name });
});

app.get("/api/users", requireAuth, (req, res) => {
  const users = db.prepare("SELECT id, username, name, created_at FROM users").all();
  res.json(users);
});

app.post("/api/users", requireAuth, (req, res) => {
  const { username, password, name } = req.body;
  if (!username || !password) return res.status(400).json({ error: "username en password vereist" });
  try {
    db.prepare("INSERT INTO users (username, password_hash, name, created_at) VALUES (?,?,?,?)").run(
      username.toLowerCase().trim(), hashPassword(password), name || username, Date.now()
    );
    res.json({ ok: true });
  } catch(e) {
    res.status(400).json({ error: "Gebruikersnaam bestaat al" });
  }
});

app.delete("/api/users/:username", requireAuth, (req, res) => {
  db.prepare("DELETE FROM users WHERE username = ?").run(req.params.username);
  res.json({ ok: true });
});

app.post("/api/users/:username/password", requireAuth, (req, res) => {
  const { password } = req.body;
  if (!password) return res.status(400).json({ error: "password vereist" });
  db.prepare("UPDATE users SET password_hash = ? WHERE username = ?").run(
    hashPassword(password), req.params.username
  );
  res.json({ ok: true });
});

// -- MONDAY PROXY -------------------------------------------------------------
// Browser can't call Monday API directly (CORS). Proxy it through the server.
app.post("/api/ai", requireAuth, async (req, res) => {
  const apiKey = ANTHROPIC_KEY;
  if (!apiKey) return res.status(500).json({ error: "ANTHROPIC_KEY not configured on server" });
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify(req.body),
    });
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/api/monday", requireAuth, async (req, res) => {
  const apiKey = process.env.MONDAY_API_KEY;
  const { query, variables } = req.body;
  if (!apiKey) return res.status(500).json({ error: "MONDAY_API_KEY not configured on server" });
  if (!query) return res.status(400).json({ error: "query required" });
  try {
    const r = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": apiKey,
        "API-Version": "2024-01",
      },
      body: JSON.stringify({ query, variables: variables || {} }),
    });
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// -- START ---------------------------------------------------------------------
async function startServer() {
  const sqlDb = initDb(DB_PATH);
  db = new Database(sqlDb);
  db.exec(DB_SCHEMA);

  // Migrations: add columns to existing tables if they don't exist yet
  const existingCols = db.prepare("PRAGMA table_info(properties)").all().map(r => r.name);
  const newCols = [
    ["name", "TEXT"],
    ["municipality", "TEXT"],
    ["province", "TEXT"],
    ["status", "TEXT"],
    ["slaapplaatsen", "INTEGER"],
    ["phone", "TEXT"],
    ["email", "TEXT"],
    ["website", "TEXT"],
    ["type", "TEXT"],
    ["regio", "TEXT"],
    ["date_online", "TEXT"],
    ["postal_code", "TEXT"],
    ["street", "TEXT"],
  ];
  for (const [col, type] of newCols) {
    if (!existingCols.includes(col)) {
      db.exec(`ALTER TABLE properties ADD COLUMN ${col} ${type}`);
      console.log(`[migration] Added column: ${col}`);
    }
  }
  // Create indexes now that columns are guaranteed to exist
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_name ON properties(name)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_municipality ON properties(municipality)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_province ON properties(province)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_status ON properties(status)`);

  // Backfill disabled - was OOM killing the container on 25k rows
  // Municipality data comes from TV API included array per property

  ensureDefaultUsers();
  app.listen(PORT, () => {
    console.log(`YourDomi server running on port ${PORT}`);
    console.log(`DB: ${DB_PATH}`);
    console.log(`AI stale after: ${AI_STALE_DAYS} days`);

    const count = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    if (count === 0) {
      console.log("[startup] Empty DB - starting initial TV sync...");
      syncPropertiesFromTV().catch(console.error);
    } else {
      console.log(`[startup] DB has ${count} properties.`);
    }
  });
}

startServer().catch(console.error);
