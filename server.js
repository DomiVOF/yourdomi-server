const express = require("express");
const cors = require("cors");
const BetterSqlite3 = require("better-sqlite3");
const fetch = require("node-fetch");
const cron = require("node-cron");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

// =============================================================================
// CONFIG
// =============================================================================
const app = express();
const PORT = process.env.PORT || 3001;
const ANTHROPIC_KEY = process.env.ANTHROPIC_KEY || "";
const AI_STALE_DAYS = parseInt(process.env.AI_STALE_DAYS || "100");
const DB_PATH = process.env.DB_PATH || "/data/yourdomi.db";

let db;

// =============================================================================
// DATABASE
// =============================================================================
function initDb(dbPath) {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  let sqlDb;
  try {
    sqlDb = new BetterSqlite3(dbPath);
    sqlDb.pragma("journal_mode = WAL");
    sqlDb.pragma("integrity_check");
    console.log("[db] Database OK");
  } catch (e) {
    console.error(`[db] Corrupt (${e.message}) — resetting`);
    try { fs.unlinkSync(dbPath); } catch (_) {}
    sqlDb = new BetterSqlite3(dbPath);
    sqlDb.pragma("journal_mode = WAL");
    console.log("[db] Fresh database created");
  }
  return sqlDb;
}

const DB_SCHEMA = `
  CREATE TABLE IF NOT EXISTS properties (
    id          TEXT PRIMARY KEY,
    data        TEXT NOT NULL,
    fetched_at  INTEGER NOT NULL,
    name        TEXT,
    municipality TEXT,
    province    TEXT,
    status      TEXT,
    slaapplaatsen INTEGER,
    phone       TEXT,
    email       TEXT,
    website     TEXT,
    type        TEXT,
    regio       TEXT,
    date_online TEXT,
    postal_code TEXT,
    street      TEXT
  );

  CREATE TABLE IF NOT EXISTS enrichment (
    id          TEXT PRIMARY KEY,
    data        TEXT NOT NULL,
    enriched_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS outcomes (
    id          TEXT PRIMARY KEY,
    outcome     TEXT,
    note        TEXT,
    contact_naam TEXT,
    updated_at  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    username    TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name        TEXT,
    created_at  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS sessions (
    token       TEXT PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    username    TEXT NOT NULL,
    name        TEXT,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_enrichment_age ON enrichment(enriched_at);
`;

// =============================================================================
// VISITFLANDERS SYNC
// Field contract — exactly what the frontend expects on each property object:
//   id, name, street, municipality, province, postalCode,
//   status, slaapplaatsen, sleepPlaces, units,
//   phone, phone2, phoneNorm, email, website,
//   type, category, toeristischeRegio, onlineSince, dateOnline,
//   registrationNumber, rawUrl, starRating
// =============================================================================

const VF_URL = "https://opendata.visitflanders.org/sector/accommodation/base_registry.json";

// promotional_region → province
const REGIO_PROVINCE = {
  kust: "West-Vlaanderen", westhoek: "West-Vlaanderen", brugge: "West-Vlaanderen",
  leieStreek: "West-Vlaanderen", roeselare: "West-Vlaanderen", kortrijk: "West-Vlaanderen",
  gent: "Oost-Vlaanderen", meetjesland: "Oost-Vlaanderen", waasland: "Oost-Vlaanderen",
  denderstreek: "Oost-Vlaanderen", vlaamseArdennen: "Oost-Vlaanderen",
  antwerpen: "Antwerpen", mechelen: "Antwerpen", kempen: "Antwerpen",
  antwerpseKempen: "Antwerpen", kunststadAntwerpen: "Antwerpen",
  hasselt: "Limburg", limburgseKempen: "Limburg", haspengouw: "Limburg",
  leuven: "Vlaams-Brabant", vlaamsBrabant: "Vlaams-Brabant",
  pajottenland: "Vlaams-Brabant", hageland: "Vlaams-Brabant",
  brussel: "Brussel",
};

// discriminator → Dutch type label
const DISC_TYPE = {
  HOLIDAY_COTTAGE: "Vakantiewoning", BED_AND_BREAKFAST: "B&B", HOTEL: "Hotel",
  HOSTEL: "Hostel", CAMPING: "Camping", HOLIDAY_PARK: "Vakantiepark",
  GENERIC_ROOMS: "Kamers", GENERIC_TERRAIN: "Terrein",
  YOUTH_ACCOMMODATION: "Jeugdlogies",
};

// status → Dutch label
const STATUS_NL = {
  NOTIFIED: "aangemeld", ACKNOWLEDGED: "erkend",
  LICENSED: "vergund", STOPPED: "gestopt",
};

function normalizePhone(p) {
  if (!p || typeof p !== "string") return null;
  return p.trim().replace(/[\s\-().]/g, "").replace(/^00/, "+").replace(/^\+?0032/, "+32") || null;
}

function trim(v) {
  return (typeof v === "string" ? v.trim() : "") || "";
}

// Convert one raw VF record into the exact shape the frontend expects
function vfToProperty(rec) {
  const bpid = String(rec.business_product_id || "");
  const id = `vf_${bpid}`;

  const rawName = trim(rec.name_or_number) || trim(rec.name);
  const street = [trim(rec.street), trim(rec.house_number), trim(rec.box_number)].filter(Boolean).join(" ");
  const municipality = trim(rec.main_city_name) || trim(rec.city_name);
  const postalCode = trim(rec.postal_code);
  const regio = trim(rec.promotional_region).toLowerCase();
  const province = REGIO_PROVINCE[regio] || REGIO_PROVINCE[trim(rec.promotional_region)] || "";
  const disc = trim(rec.discriminator).toUpperCase();
  const type = DISC_TYPE[disc] || trim(rec.discriminator) || "Vakantiewoning";
  const statusRaw = trim(rec.status).toUpperCase();
  const status = STATUS_NL[statusRaw] || trim(rec.status).toLowerCase() || "aangemeld";

  // Name fallback: name_or_number → name → street+city → id
  const name = rawName
    || (street && municipality ? `${street}, ${municipality}` : "")
    || municipality
    || id;

  const phone = normalizePhone(rec.phone1);
  const phone2 = normalizePhone(rec.phone2) || normalizePhone(rec.phone3);
  const phoneNorm = phone ? phone.replace(/[^0-9+]/g, "") : null;

  // Date: prefer notification_date, then changed_time
  const dateRaw = trim(rec.notification_date || rec.last_status_change_date || rec.changed_time || "");
  const dateOnline = dateRaw.substring(0, 10); // "YYYY-MM-DD"

  const slaapplaatsen = parseInt(rec.maximum_capacity) || 0;
  const units = parseInt(rec.number_of_units) || 1;

  return {
    // Core identity
    id,
    businessProductId: bpid,
    registrationNumber: bpid,
    rawUrl: `https://www.toerismevlaanderen.be/logiesdecreet/basisregister?product=${bpid}`,

    // Name & address
    name,
    street,
    municipality,
    postalCode,
    province,

    // Contact
    phone,
    phone2,
    phoneNorm,
    email: trim(rec.email) || null,
    website: trim(rec.website) || null,

    // Classification
    status,
    type,
    category: type,
    toeristischeRegio: regio,
    starRating: null,

    // Capacity
    slaapplaatsen,
    sleepPlaces: slaapplaatsen,
    units,

    // Date
    onlineSince: dateOnline || null,
    dateOnline: dateOnline || null,

    // Coordinates (for future use)
    lat: parseFloat(rec.lat) || null,
    lng: parseFloat(rec.long) || null,
  };
}

async function fetchVFPage(offset, limit = 500) {
  const url = `${VF_URL}?offset=${offset}&limit=${limit}`;
  const res = await fetch(url, {
    headers: { "User-Agent": "YourdomiServer/1.0", Accept: "application/json" },
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`VF API ${res.status} at offset ${offset}`);
  return res.json();
}

async function syncFromVF() {
  console.log("[sync] Starting VisitFlanders sync...");
  const LIMIT = 500;
  const now = Date.now();

  const insert = db.prepare(
    `INSERT OR REPLACE INTO properties
      (id, data, fetched_at, name, municipality, province, status, slaapplaatsen,
       phone, email, website, type, regio, date_online, postal_code, street)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
  );

  const insertBatch = db.transaction((records) => {
    for (const p of records) {
      insert.run(
        p.id, JSON.stringify(p), now,
        p.name, p.municipality, p.province, p.status, p.slaapplaatsen,
        p.phone, p.email, p.website, p.type, p.toeristischeRegio,
        p.dateOnline, p.postalCode, p.street
      );
    }
  });

  let offset = 0;
  let totalSynced = 0;

  try {
    while (true) {
      const batch = await fetchVFPage(offset, LIMIT);
      if (!Array.isArray(batch) || batch.length === 0) break;

      const parsed = batch.map(vfToProperty);
      insertBatch(parsed);
      totalSynced += parsed.length;
      console.log(`[sync] ${totalSynced} records...`);

      if (batch.length < LIMIT) break; // last page
      offset += LIMIT;

      if (global.gc) global.gc();
      await new Promise(r => setTimeout(r, 200));
    }

    console.log(`[sync] Done — ${totalSynced} properties from VisitFlanders.`);
  } catch (e) {
    console.error("[sync] Error:", e.message);
  }
}

// =============================================================================
// CORS & MIDDLEWARE
// =============================================================================
const corsOptions = {
  origin: (origin, cb) => cb(null, origin || "*"),
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "x-auth-token", "Authorization"],
  optionsSuccessStatus: 204,
};
app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json({ limit: "2mb" }));

// =============================================================================
// AUTH
// =============================================================================
function hashPassword(pw) {
  return crypto.createHash("sha256").update(pw + "yourdomi_salt_2025").digest("hex");
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
    const exists = db.prepare("SELECT id FROM users WHERE username = ?").get(u.username);
    if (!exists) {
      db.prepare("INSERT INTO users (username, password_hash, name, created_at) VALUES (?,?,?,?)").run(
        u.username, hashPassword("yourdomi2026"), u.name, now
      );
    } else {
      db.prepare("UPDATE users SET password_hash=?, name=? WHERE username=?").run(
        hashPassword("yourdomi2026"), u.name, u.username
      );
    }
  }
}

// =============================================================================
// API ROUTES — PROPERTIES
// =============================================================================

// GET /api/panden
app.get("/api/panden", requireAuth, (req, res) => {
  try {
    const page = parseInt(req.query.page || "1");
    const size = Math.min(parseInt(req.query.size || "50"), 200);
    const { zoek, gemeente, provincie, status, minSlaap, maxSlaap, heeftTelefoon, heeftEmail, heeftWebsite } = req.query;

    const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    if (total === 0) return res.json({ data: [], meta: { total: 0, page, size }, _needsSync: true });

    const conditions = [];
    const params = [];

    if (zoek) {
      const q = `%${zoek.toLowerCase()}%`;
      conditions.push("(LOWER(name) LIKE ? OR LOWER(municipality) LIKE ? OR postal_code LIKE ? OR LOWER(street) LIKE ?)");
      params.push(q, q, `%${zoek}%`, q);
    }
    if (gemeente)   { conditions.push("LOWER(municipality) LIKE ?"); params.push(`%${gemeente.toLowerCase()}%`); }
    if (provincie)  { conditions.push("province = ?");    params.push(provincie); }
    if (status)     { conditions.push("status = ?");      params.push(status); }
    if (minSlaap)   { conditions.push("slaapplaatsen >= ?"); params.push(parseInt(minSlaap)); }
    if (maxSlaap)   { conditions.push("slaapplaatsen <= ?"); params.push(parseInt(maxSlaap)); }
    if (req.query.type)  { conditions.push("type = ?");  params.push(req.query.type); }
    if (req.query.regio) { conditions.push("regio = ?"); params.push(req.query.regio); }
    if (heeftTelefoon === "1") conditions.push("phone IS NOT NULL AND phone != ''");
    if (heeftEmail    === "1") conditions.push("email IS NOT NULL AND email != ''");
    if (heeftWebsite  === "1") conditions.push("website IS NOT NULL AND website != ''");

    const where   = conditions.length ? "WHERE " + conditions.join(" AND ") : "";
    const orderBy = req.query.sorteer === "nieuwste" ? "ORDER BY date_online DESC" : "";

    const filteredTotal = db.prepare(`SELECT COUNT(*) as c FROM properties ${where}`).get(...params).c;
    const offset = (page - 1) * size;
    const rows = db.prepare(`SELECT data FROM properties ${where} ${orderBy} LIMIT ? OFFSET ?`).all(...params, size, offset);

    const properties = rows.map(r => {
      try { return JSON.parse(r.data); }
      catch { return null; }
    }).filter(Boolean);

    res.json({
      data: properties,
      meta: { total: filteredTotal, dbTotal: total, page, size, pages: Math.ceil(filteredTotal / size) },
    });
  } catch (e) {
    console.error("[/api/panden]", e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/panden/count
app.get("/api/panden/count", requireAuth, (req, res) => {
  const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
  const lastFetch = db.prepare("SELECT MAX(fetched_at) as t FROM properties").get().t;
  res.json({ total, lastFetch });
});

// POST /api/panden/enrich-address — no-op, all data comes from VF sync
app.post("/api/panden/enrich-address", requireAuth, (req, res) => {
  res.json({ updated: [], message: "All data already available from VF sync." });
});

// =============================================================================
// API ROUTES — META
// =============================================================================

// GET /api/meta
app.get("/api/meta", requireAuth, (req, res) => {
  const provinces = db.prepare("SELECT DISTINCT province FROM properties WHERE province IS NOT NULL AND province != '' ORDER BY province").all().map(r => r.province);
  const types     = db.prepare("SELECT DISTINCT type FROM properties WHERE type IS NOT NULL AND type != '' ORDER BY type").all().map(r => r.type);
  const regios    = db.prepare("SELECT DISTINCT regio FROM properties WHERE regio IS NOT NULL AND regio != '' ORDER BY regio").all().map(r => r.regio);
  res.json({ provinces, types, regios });
});

// GET /api/health
app.get("/api/health", requireAuth, (req, res) => {
  const props    = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
  const enriched = db.prepare("SELECT COUNT(*) as c FROM enrichment").get().c;
  const outcomes = db.prepare("SELECT COUNT(*) as c FROM outcomes").get().c;
  const lastSync = db.prepare("SELECT MAX(fetched_at) as t FROM properties").get()?.t;
  const withPhone = db.prepare("SELECT COUNT(*) as c FROM properties WHERE phone IS NOT NULL AND phone != ''").get().c;
  res.json({
    ok: true, properties: props, enrichments: enriched, outcomes,
    withPhone, lastSync: lastSync ? new Date(lastSync).toISOString() : null,
    staleAfterDays: AI_STALE_DAYS,
  });
});

// =============================================================================
// API ROUTES — SYNC
// =============================================================================

app.post("/api/sync", (req, res) => {
  res.json({ ok: true, message: "Sync started" });
  syncFromVF().catch(console.error);
});
app.get("/api/sync", (req, res) => {
  res.json({ ok: true, message: "Sync started" });
  syncFromVF().catch(console.error);
});

// POST /api/backfill — re-index all stored data into indexed columns
app.post("/api/backfill", requireAuth, (req, res) => {
  res.json({ ok: true, message: "Backfill started" });
  setTimeout(() => {
    try {
      const rows = db.prepare("SELECT id, data FROM properties").all();
      const upd = db.prepare(
        "UPDATE properties SET name=?,municipality=?,province=?,status=?,slaapplaatsen=?,phone=?,email=?,website=?,type=?,regio=?,date_online=?,postal_code=?,street=? WHERE id=?"
      );
      let n = 0;
      for (const row of rows) {
        try {
          const p = JSON.parse(row.data);
          upd.run(p.name,p.municipality,p.province,p.status,p.slaapplaatsen,p.phone,p.email,p.website,p.type,p.toeristischeRegio,p.dateOnline,p.postalCode,p.street,row.id);
          n++;
        } catch {}
      }
      console.log(`[backfill] Done — ${n} records re-indexed`);
    } catch (e) { console.error("[backfill]", e.message); }
  }, 100);
});

// POST /api/admin/cleanup-old-records — remove non-vf_ records
app.post("/api/admin/cleanup-old-records", requireAuth, (req, res) => {
  try {
    const old = db.prepare("SELECT COUNT(*) as c FROM properties WHERE id NOT LIKE 'vf_%'").get().c;
    const vf  = db.prepare("SELECT COUNT(*) as c FROM properties WHERE id LIKE 'vf_%'").get().c;
    if (vf === 0) return res.json({ ok: false, message: "Geen VF records — sync eerst" });
    db.prepare("DELETE FROM properties WHERE id NOT LIKE 'vf_%'").run();
    db.prepare("DELETE FROM enrichment WHERE id NOT LIKE 'vf_%'").run();
    db.prepare("DELETE FROM outcomes  WHERE id NOT LIKE 'vf_%'").run();
    res.json({ ok: true, deleted: old, kept: vf });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// API ROUTES — DEBUG
// =============================================================================

app.get("/api/debug/raw", (req, res) => {
  try {
    const rows = db.prepare("SELECT data FROM properties LIMIT 3").all();
    res.json(rows.map(r => JSON.parse(r.data)));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/debug", (req, res) => {
  try {
    const total   = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    const withMuni = db.prepare("SELECT COUNT(*) as c FROM properties WHERE municipality != ''").get().c;
    const withPhone = db.prepare("SELECT COUNT(*) as c FROM properties WHERE phone IS NOT NULL AND phone != ''").get().c;
    const sample  = db.prepare("SELECT data FROM properties WHERE phone IS NOT NULL AND phone != '' LIMIT 3").all().map(r => JSON.parse(r.data));
    const noName  = db.prepare("SELECT COUNT(*) as c FROM properties WHERE name IS NULL OR name = '' OR name LIKE 'vf_%'").get().c;
    res.json({ total, withMuni, withPhone, noName, sample });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// API ROUTES — ENRICHMENT
// =============================================================================

app.get("/api/enrichment/:id", requireAuth, (req, res) => {
  const row = db.prepare("SELECT data, enriched_at FROM enrichment WHERE id = ?").get(req.params.id);
  if (!row) return res.json(null);
  res.json({ ...JSON.parse(row.data), _enrichedAt: row.enriched_at });
});

app.get("/api/enrichment", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT id, data, enriched_at FROM enrichment").all();
  const result = {};
  for (const row of rows) result[row.id] = { ...JSON.parse(row.data), _enrichedAt: row.enriched_at };
  res.json(result);
});

app.post("/api/enrichment/:id", requireAuth, (req, res) => {
  const data = req.body;
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Invalid data" });
  db.prepare("INSERT OR REPLACE INTO enrichment (id, data, enriched_at) VALUES (?,?,?)").run(
    req.params.id, JSON.stringify(data), Date.now()
  );
  res.json({ ok: true });
});

app.get("/api/enrichment/stale", requireAuth, (req, res) => {
  const cutoff = Date.now() - AI_STALE_DAYS * 86400000;
  const stale = db.prepare("SELECT id FROM enrichment WHERE enriched_at < ?").all(cutoff).map(r => r.id);
  const enrichedIds = new Set(db.prepare("SELECT id FROM enrichment").all().map(r => r.id));
  const allIds = db.prepare("SELECT id FROM properties").all().map(r => r.id);
  const unenriched = allIds.filter(id => !enrichedIds.has(id));
  res.json({ stale: [...stale, ...unenriched].slice(0, 200) });
});

// =============================================================================
// API ROUTES — OUTCOMES
// =============================================================================

app.get("/api/outcomes", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT * FROM outcomes").all();
  const result = {};
  for (const row of rows) {
    result[row.id] = { outcome: row.outcome, note: row.note, contactNaam: row.contact_naam, updatedAt: row.updated_at };
  }
  res.json(result);
});

app.post("/api/outcomes/:id", requireAuth, (req, res) => {
  const { outcome, note, contactNaam } = req.body;
  db.prepare("INSERT OR REPLACE INTO outcomes (id, outcome, note, contact_naam, updated_at) VALUES (?,?,?,?,?)").run(
    req.params.id, outcome || null, note || null, contactNaam || null, Date.now()
  );
  res.json({ ok: true });
});

// =============================================================================
// API ROUTES — AUTH
// =============================================================================

app.post("/api/login", (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: "Gebruikersnaam en wachtwoord vereist" });
  const user = db.prepare("SELECT * FROM users WHERE username = ?").get(username.toLowerCase().trim());
  if (!user || user.password_hash !== hashPassword(password)) {
    return res.status(401).json({ error: "Ongeldige gebruikersnaam of wachtwoord" });
  }
  const token = generateToken();
  const now = Date.now();
  db.prepare("INSERT INTO sessions (token, user_id, username, name, created_at, expires_at) VALUES (?,?,?,?,?,?)").run(
    token, user.id, user.username, user.name || user.username, now, now + 30 * 86400000
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
  res.json(db.prepare("SELECT id, username, name, created_at FROM users").all());
});

app.post("/api/users", requireAuth, (req, res) => {
  const { username, password, name } = req.body;
  if (!username || !password) return res.status(400).json({ error: "username en password vereist" });
  try {
    db.prepare("INSERT INTO users (username, password_hash, name, created_at) VALUES (?,?,?,?)").run(
      username.toLowerCase().trim(), hashPassword(password), name || username, Date.now()
    );
    res.json({ ok: true });
  } catch { res.status(400).json({ error: "Gebruikersnaam bestaat al" }); }
});

app.delete("/api/users/:username", requireAuth, (req, res) => {
  db.prepare("DELETE FROM users WHERE username = ?").run(req.params.username);
  res.json({ ok: true });
});

app.post("/api/users/:username/password", requireAuth, (req, res) => {
  const { password } = req.body;
  if (!password) return res.status(400).json({ error: "password vereist" });
  db.prepare("UPDATE users SET password_hash = ? WHERE username = ?").run(hashPassword(password), req.params.username);
  res.json({ ok: true });
});

app.post("/api/admin/reset-password", (req, res) => {
  const adminPw = process.env.ADMIN_PASSWORD;
  if (!adminPw) return res.status(403).json({ error: "ADMIN_PASSWORD not set" });
  const { adminPassword, username, newPassword } = req.body;
  if (adminPassword !== adminPw) return res.status(403).json({ error: "Wrong admin password" });
  if (!username || !newPassword) return res.status(400).json({ error: "username and newPassword required" });
  const user = db.prepare("SELECT id FROM users WHERE username = ?").get(username);
  if (!user) return res.status(404).json({ error: "User not found" });
  db.prepare("UPDATE users SET password_hash = ? WHERE username = ?").run(hashPassword(newPassword), username);
  db.prepare("DELETE FROM sessions WHERE username = ?").run(username);
  res.json({ ok: true });
});

// =============================================================================
// API ROUTES — AI + MONDAY PROXIES
// =============================================================================

app.post("/api/ai", requireAuth, async (req, res) => {
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: "ANTHROPIC_KEY not configured" });
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify(req.body),
    });
    res.json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/monday", requireAuth, async (req, res) => {
  const apiKey = process.env.MONDAY_API_KEY;
  const { query, variables } = req.body;
  if (!apiKey) return res.status(500).json({ error: "MONDAY_API_KEY not configured" });
  if (!query) return res.status(400).json({ error: "query required" });
  try {
    const r = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: apiKey, "API-Version": "2024-01" },
      body: JSON.stringify({ query, variables: variables || {} }),
    });
    res.json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// CRON
// =============================================================================
// Every Sunday at 03:00 — re-sync all properties
cron.schedule("0 3 * * 0", () => {
  console.log("[cron] Weekly VF sync starting...");
  syncFromVF().catch(console.error);
});

// =============================================================================
// START
// =============================================================================
async function startServer() {
  const sqlDb = initDb(DB_PATH);
  db = {
    prepare: (sql) => sqlDb.prepare(sql),
    exec:    (sql) => sqlDb.exec(sql),
    transaction: (fn) => { const t = sqlDb.transaction(fn); return (items) => t(items); },
  };

  db.exec(DB_SCHEMA);

  // Migrations: add any missing columns
  const cols = db.prepare("PRAGMA table_info(properties)").all().map(r => r.name);
  for (const [col, type] of [
    ["name","TEXT"],["municipality","TEXT"],["province","TEXT"],["status","TEXT"],
    ["slaapplaatsen","INTEGER"],["phone","TEXT"],["email","TEXT"],["website","TEXT"],
    ["type","TEXT"],["regio","TEXT"],["date_online","TEXT"],["postal_code","TEXT"],["street","TEXT"],
  ]) {
    if (!cols.includes(col)) {
      db.exec(`ALTER TABLE properties ADD COLUMN ${col} ${type}`);
      console.log(`[migration] Added column: ${col}`);
    }
  }

  // Indexes
  db.exec(`CREATE INDEX IF NOT EXISTS idx_name ON properties(name)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_municipality ON properties(municipality)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_province ON properties(province)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_status ON properties(status)`);

  ensureDefaultUsers();

  app.listen(PORT, () => {
    console.log(`YourDomi server running on port ${PORT}`);
    console.log(`DB: ${DB_PATH}`);

    const count = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    const vfCount = db.prepare("SELECT COUNT(*) as c FROM properties WHERE id LIKE 'vf_%'").get().c;

    if (count === 0) {
      console.log("[startup] Empty DB — starting initial VF sync...");
      syncFromVF().catch(console.error);
    } else if (vfCount === 0) {
      console.log("[startup] Old UUID data detected — starting fresh VF sync...");
      // Wipe old data and start fresh
      db.exec("DELETE FROM properties");
      syncFromVF().catch(console.error);
    } else {
      console.log(`[startup] ${count} properties in DB (${vfCount} VF records).`);
    }
  });
}

startServer().catch(console.error);
