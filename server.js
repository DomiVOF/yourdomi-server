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

// -- VISITFLANDERS OPEN DATA SYNC ----------------------------------------------
// Primary data source: opendata.visitflanders.org/sector/accommodation/base_registry
// This API returns flat JSON with name, address, phone, email in one bulk call.
// Much better than the linked.toerismevlaanderen.be API which only has 6 fields per record.

const VF_BASE = "https://opendata.visitflanders.org/sector/accommodation/base_registry.json";

// Map visitflanders promotional_region to province
const REGIO_TO_PROVINCE = {
  kust: "West-Vlaanderen",
  westhoek: "West-Vlaanderen",
  brugge: "West-Vlaanderen",
  leieStreek: "West-Vlaanderen",
  roeselare: "West-Vlaanderen",
  kortrijk: "West-Vlaanderen",
  gent: "Oost-Vlaanderen",
  meetjesland: "Oost-Vlaanderen",
  waasland: "Oost-Vlaanderen",
  denderstreek: "Oost-Vlaanderen",
  vlaamseArdennen: "Oost-Vlaanderen",
  antwerpen: "Antwerpen",
  mechelen: "Antwerpen",
  kempen: "Antwerpen",
  antwerpseKempen: "Antwerpen",
  kunststadAntwerpen: "Antwerpen",
  hasselt: "Limburg",
  limburgseKempen: "Limburg",
  haspengouw: "Limburg",
  leuven: "Vlaams-Brabant",
  vlaamsBrabant: "Vlaams-Brabant",
  pajottenland: "Vlaams-Brabant",
  hageland: "Vlaams-Brabant",
  brussel: "Brussel",
};

// Map visitflanders discriminator to a clean Dutch type label
const DISC_TO_TYPE = {
  HOLIDAY_COTTAGE: "Vakantiewoning",
  BED_AND_BREAKFAST: "B&B",
  HOTEL: "Hotel",
  HOSTEL: "Hostel",
  CAMPING: "Camping",
  HOLIDAY_PARK: "Vakantiepark",
  GENERIC_ROOMS: "Kamers",
  GENERIC_TERRAIN: "Terrein",
  YOUTH_ACCOMMODATION: "Jeugdlogies",
};

// Map visitflanders status to display label
const STATUS_MAP = {
  NOTIFIED: "aangemeld",
  ACKNOWLEDGED: "erkend",
  LICENSED: "vergund",
  STOPPED: "gestopt",
};

async function fetchVFPage(offset = 0, limit = 500) {
  const url = `${VF_BASE}?offset=${offset}&limit=${limit}`;
  const res = await fetch(url, {
    headers: { "User-Agent": "YourdomiServer/1.0", "Accept": "application/json" },
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`VF API ${res.status} at offset ${offset}: ${await res.text()}`);
  return res.json(); // returns array directly
}

function parseVFRecord(rec) {
  const trim = (v) => (typeof v === "string" ? v.trim() : "") || "";
  const regio = trim(rec.promotional_region).toLowerCase();
  const disc = trim(rec.discriminator).toUpperCase();

  // Build a stable ID: use business_product_id prefixed so it doesn't clash with old UUIDs
  const id = `vf_${rec.business_product_id}`;

  // Name: prefer name_or_number, fallback to name, fallback to street+city
  const rawName = trim(rec.name_or_number) || trim(rec.name);
  const street = trim(rec.street);
  const houseNum = trim(rec.house_number);
  const box = trim(rec.box_number);
  const city = trim(rec.main_city_name) || trim(rec.city_name);
  const postal = trim(rec.postal_code);

  const streetFull = [street, houseNum, box].filter(Boolean).join(" ");
  const name = rawName || (streetFull && city ? `${streetFull}, ${city}` : city || id);

  // Phone: phone1 is primary, phone2/phone3 secondary
  const phone = trim(rec.phone1);
  const phone2 = trim(rec.phone2) || trim(rec.phone3);

  const province = REGIO_TO_PROVINCE[regio] || REGIO_TO_PROVINCE[rec.promotional_region] || "";
  const type = DISC_TO_TYPE[disc] || trim(rec.discriminator);
  const status = STATUS_MAP[trim(rec.status).toUpperCase()] || trim(rec.status).toLowerCase();

  // Date online: notification_date or last_status_change_date
  const dateOnline = trim(rec.notification_date || rec.last_status_change_date || rec.changed_time || "").substring(0, 10);

  return {
    id,
    businessProductId: trim(String(rec.business_product_id || "")),
    name,
    street: streetFull,
    municipality: city,
    postalCode: postal,
    province,
    phone,
    phone2,
    email: trim(rec.email),
    website: trim(rec.website),
    status,
    slaapplaatsen: parseInt(rec.maximum_capacity) || 0,
    aantalUnits: parseInt(rec.number_of_units) || 0,
    type,
    toeristischeRegio: regio,
    dateOnline,
    lat: parseFloat(rec.lat) || null,
    lng: parseFloat(rec.long) || null,
    rawUrl: `https://opendata.visitflanders.org/sector/accommodation/base_registry.json?business_product_id=${rec.business_product_id}`,
  };
}

async function syncPropertiesFromTV() {
  console.log("[sync] Starting sync from VisitFlanders open data API...");
  const LIMIT = 500;
  const CONCURRENCY = 3;
  const now = Date.now();

  const insert = db.prepare(
    "INSERT OR REPLACE INTO properties (id, data, fetched_at, name, municipality, province, status, slaapplaatsen, phone, email, website, type, regio, date_online, postal_code, street) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );

  function insertItem(parsed) {
    insert.run(
      parsed.id,
      JSON.stringify(parsed),
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

  const insertBatch = db.transaction((records) => {
    for (const r of records) insertItem(r);
  });

  try {
    // Step 1: fetch first page to gauge total
    console.log("[sync] Fetching first page...");
    const firstPage = await fetchVFPage(0, LIMIT);
    if (!Array.isArray(firstPage) || firstPage.length === 0) {
      console.log("[sync] No records returned from VF API.");
      return;
    }

    // Parse and store first batch
    const firstParsed = firstPage.map(parseVFRecord);
    insertBatch(firstParsed);
    let synced = firstParsed.length;
    console.log(`[sync] First batch: ${synced} records`);

    // If we got a full page, there are more
    if (firstPage.length < LIMIT) {
      console.log(`[sync] Done. Total: ${synced} properties (single page).`);
      return;
    }

    // Step 2: keep fetching until we get a partial page
    let offset = LIMIT;
    let hasMore = true;

    while (hasMore) {
      // Fetch CONCURRENCY pages in parallel
      const offsets = Array.from({ length: CONCURRENCY }, (_, i) => offset + i * LIMIT);
      const results = await Promise.allSettled(offsets.map(o => fetchVFPage(o, LIMIT)));

      let gotAny = false;
      for (let i = 0; i < results.length; i++) {
        const r = results[i];
        if (r.status === "fulfilled" && Array.isArray(r.value) && r.value.length > 0) {
          const parsed = r.value.map(parseVFRecord);
          insertBatch(parsed);
          synced += parsed.length;
          gotAny = true;
          if (r.value.length < LIMIT) {
            hasMore = false; // last page
          }
        } else if (r.status === "rejected") {
          console.warn(`[sync] Offset ${offsets[i]} failed: ${r.reason?.message}`);
        } else {
          hasMore = false; // empty page = done
        }
      }

      offset += CONCURRENCY * LIMIT;
      const pct = Math.round((synced / Math.max(synced, 1)) * 100);
      console.log(`[sync] Synced ${synced} records so far...`);

      if (!gotAny) hasMore = false;
      if (global.gc) global.gc();
      await new Promise(r => setTimeout(r, 300));
    }

    console.log(`[sync] Done. Total synced: ${synced} properties from VisitFlanders.`);
  } catch (e) {
    console.error("[sync] Error:", e.message);
  }
}

// -- PARSE LODGING — now just reads already-parsed VF data from DB -----------
// The DB stores the output of parseVFRecord() directly as JSON.
// This function provides a consistent interface for the API routes.
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

// parseLodging: reads VF-parsed data from DB. Data is already clean from parseVFRecord().
// Keeps the same output shape so App.jsx doesn't need changes.
function parseLodging(stored) {
  // stored IS already the parseVFRecord() output — just normalize + ensure all fields present
  if (!stored || typeof stored !== "object") return null;

  const t = (v) => (typeof v === "string" ? v.trim() : "") || "";
  const phone = t(stored.phone);
  const phone2 = t(stored.phone2);
  const phoneNorm = phone ? phone.replace(/[^0-9+]/g, "") : null;

  return {
    id: t(stored.id),
    businessProductId: t(stored.businessProductId),
    name: t(stored.name) || t(stored.id),
    municipality: t(stored.municipality),
    province: t(stored.province),
    toeristischeRegio: t(stored.toeristischeRegio),
    type: t(stored.type),
    postalCode: t(stored.postalCode),
    street: t(stored.street),
    sleepPlaces: n(stored.slaapplaatsen),
    slaapplaatsen: n(stored.slaapplaatsen),
    units: n(stored.aantalUnits) || 1,
    phone: phone || null,
    phone2: phone2 || null,
    phoneNorm,
    email: t(stored.email) || null,
    website: t(stored.website) || null,
    images: [],
    status: t(stored.status) || "aangemeld",
    starRating: null,
    onlineSince: t(stored.dateOnline),
    dateOnline: t(stored.dateOnline),
    registrationNumber: t(stored.id),
    category: t(stored.type) || "vakantiewoning",
    rawUrl: t(stored.rawUrl),
    lat: stored.lat || null,
    lng: stored.lng || null,
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
        return parseLodging(stored);
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

// POST /api/panden/enrich-address — no-op, VF API provides all data in sync
app.post("/api/panden/enrich-address", requireAuth, async (req, res) => {
  res.json({ updated: [], message: "No enrichment needed — VF API provides all data." });
});

// GET /api/debug/raw — shows sample stored VF data
app.get("/api/debug/raw", (req, res) => {
  try {
    const rows = db.prepare("SELECT data FROM properties LIMIT 3").all();
    const samples = rows.map(r => {
      const stored = JSON.parse(r.data);
      return { stored, parsed: parseLodging(stored) };
    });
    res.json(samples);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/debug/tv/:id — show stored record
app.get("/api/debug/tv/:id", async (req, res) => {
  try {
    const row = db.prepare("SELECT data FROM properties WHERE id = ? OR id = ?").get(
      req.params.id, `vf_${req.params.id}`
    );
    if (!row) return res.status(404).json({ error: "Not found" });
    const stored = JSON.parse(row.data);
    res.json({ stored, parsed: parseLodging(stored) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/backfill — re-index all stored VF data into indexed columns
app.post("/api/backfill", async (req, res) => {
  res.json({ ok: true, message: "Backfill started in background" });
  setTimeout(async () => {
    try {
      console.log("[backfill] Re-indexing all stored VF properties...");
      const rows = db.prepare("SELECT id, data FROM properties").all();
      const update = db.prepare("UPDATE properties SET name=?,municipality=?,province=?,status=?,slaapplaatsen=?,phone=?,email=?,website=?,type=?,regio=?,date_online=?,postal_code=?,street=? WHERE id=?");
      let count = 0;
      for (const row of rows) {
        try {
          const stored = JSON.parse(row.data);
          const p = parseLodging(stored);
          update.run(p.name,p.municipality,p.province,p.status,p.slaapplaatsen,p.phone,p.email,p.website,p.type,p.toeristischeRegio,p.dateOnline,p.postalCode,p.street,row.id);
          count++;
          if (count % 5000 === 0) { console.log("[backfill]", count, "/", rows.length); }
        } catch(e) { console.warn("[backfill] Error on", row.id, e.message); }
      }
      console.log("[backfill] Done. Updated", count, "properties.");
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
      const parsed = parseLodging(p);
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
