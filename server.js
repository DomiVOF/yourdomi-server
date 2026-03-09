const express = require("express");
const cors = require("cors");
const initSqlJs = require('sql.js');
const fs = require('fs');

// sql.js wrapper that mimics better-sqlite3 API
let _db = null;
let _dbPath = null;

function getDb() { return _db; }

async function initDb(dbPath) {
  _dbPath = dbPath;
  const SQL = await initSqlJs();
  if (fs.existsSync(dbPath)) {
    const fileBuffer = fs.readFileSync(dbPath);
    _db = new SQL.Database(fileBuffer);
  } else {
    _db = new SQL.Database();
  }
  // Persist to disk after every write
  return _db;
}

function saveDb() {
  if (_db && _dbPath) {
    const data = _db.export();
    fs.writeFileSync(_dbPath, Buffer.from(data));
  }
}

// Minimal better-sqlite3 compatible wrapper
class Statement {
  constructor(db, sql) { this.db = db; this.sql = sql; }
  run(...args) {
    this.db.run(this.sql, args.flat());
    return { changes: 1 };
  }
  get(...args) {
    const stmt = this.db.prepare(this.sql);
    const params = args.flat();
    if (params.length) stmt.bind(params);
    const result = stmt.step() ? stmt.getAsObject() : undefined;
    stmt.free();
    return result;
  }
  all(...args) {
    const stmt = this.db.prepare(this.sql);
    const params = args.flat();
    if (params.length) stmt.bind(params);
    const results = [];
    while (stmt.step()) results.push(stmt.getAsObject());
    stmt.free();
    return results;
  }
}

class Database {
  constructor(db) { this.db = db; }
  prepare(sql) { return new Statement(this.db, sql); }
  exec(sql) { this.db.run(sql); saveDb(); }
  transaction(fn) {
    return (items) => {
      fn(items); // no transaction wrapper - sql.js auto-commits
      saveDb();
    };
  }
}
const fetch = require("node-fetch");
const cron = require("node-cron");
const path = require("path");

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
    municipality TEXT,
    province TEXT,
    status TEXT,
    slaapplaatsen INTEGER,
    phone TEXT,
    email TEXT,
    website TEXT,
    type TEXT,
    regio TEXT,
    date_online TEXT
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
  const PAGE_SIZE = 100;
  const CONCURRENCY = 8;  // parallel requests at once
  const now = Date.now();

  const insert = db.prepare(
    "INSERT OR REPLACE INTO properties (id, data, fetched_at, municipality, province, status, slaapplaatsen, phone, email, website, type, regio, date_online) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );
  const insertMany = db.transaction((items) => {
    for (const item of items) {
      const parsed = parseLodging(item.raw, item.included);
      insert.run(
        item.id,
        JSON.stringify(item),
        now,
        parsed.municipality || "",
        parsed.province || "",
        parsed.status || "",
        parsed.slaapplaatsen || 0,
        parsed.phone || "",
        parsed.email || "",
        parsed.website || "",
        parsed.type || "",
        parsed.toeristischeRegio || "",
        parsed.dateOnline || ""
      );
    }
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

      await new Promise(r => setTimeout(r, 100)); // yield event loop between batches
    }

    console.log(`[sync] Done. Synced ${synced} properties.`);
  } catch (e) {
    console.error("[sync] Error:", e.message);
  }
}

// -- PARSE LODGING (same logic as frontend) -----------------------------------
function parseLodging(raw, included = []) {
  const attr = raw.attributes || {};
  const rel = raw.relationships || {};

  const getIncluded = (type, id) =>
    included.find((i) => i.type === type && i.id === id);

  // Municipality
  let municipality = "";
  let province = "";
  const muniRef = rel.municipality?.data;
  if (muniRef) {
    const muni = getIncluded(muniRef.type, muniRef.id);
    municipality = muni?.attributes?.name || muni?.attributes?.["schema:name"] || "";
    const provRef = muni?.relationships?.province?.data;
    if (provRef) {
      const prov = getIncluded(provRef.type, provRef.id);
      province = prov?.attributes?.name || prov?.attributes?.["schema:name"] || "";
    }
  }
  // Fallbacks from attributes
  if (!municipality) municipality = attr["schema:address"]?.["schema:addressLocality"] || attr["municipality"] || attr["address-municipality"] || "";
  if (!province) province = attr["schema:address"]?.["schema:addressRegion"] || attr["province"] || "";

  // Contact
  const contactInfo = attr["schema:contactPoint"] || [];
  const phones = [], emails = [], websites = [];
  (Array.isArray(contactInfo) ? contactInfo : [contactInfo]).forEach((c) => {
    if (!c) return;
    if (c["schema:telephone"]) phones.push(c["schema:telephone"]);
    if (c["schema:email"]) emails.push(c["schema:email"]);
    if (c["schema:url"]) websites.push(c["schema:url"]);
  });
  const cleanPhone = (p) => p?.replace(/\s/g, "").replace(/^00/, "+").replace(/^\+?0032/, "+32") || "";
  const phone = phones[0] ? cleanPhone(phones[0]) : "";
  const phoneNorm = phone.replace(/[^0-9+]/g, "");

  // Images
  const mediaRefs = rel.media?.data || [];
  const images = mediaRefs
    .map((m) => getIncluded(m.type, m.id)?.attributes?.contentUrl)
    .filter(Boolean)
    .slice(0, 5);

  // Tourist region
  let toeristischeRegio = "";
  const regioRef = rel["tourist-region"]?.data || rel["touristRegion"]?.data || rel["toeristische-regio"]?.data;
  if (regioRef) {
    const regio = Array.isArray(regioRef)
      ? getIncluded(regioRef[0]?.type, regioRef[0]?.id)
      : getIncluded(regioRef.type, regioRef.id);
    toeristischeRegio = regio?.attributes?.name || regio?.attributes?.["schema:name"] || "";
  }
  // Also check attributes directly
  if (!toeristischeRegio) {
    toeristischeRegio = attr["tourist-region"] || attr.touristRegion || attr["toeristische-regio"] || "";
  }

  // Accommodation type
  const type = attr["dcterms:type"] || attr["schema:additionalType"] || attr.type || "";

  return {
    id: raw.id,
    name: attr["schema:name"] || attr.name || `Pand ${raw.id.slice(-6)}`,
    municipality,
    province,
    toeristischeRegio,
    type,
    postalCode: attr["schema:address"]?.["schema:postalCode"] || "",
    street: attr["schema:address"]?.["schema:streetAddress"] || "",
    slaapplaatsen: attr["schema:numberOfRooms"] || attr.numberOfSleepingPlaces || 0,
    phone,
    phoneNorm,
    email: emails[0] || "",
    website: websites[0] || "",
    images,
    status: attr.registrationStatus || attr.status || "",
    dateOnline: attr["dcterms:created"] || attr["schema:dateCreated"] || attr.created || attr.dateCreated || "",
  };
}

// -- API ROUTES ----------------------------------------------------------------

// GET /api/panden?page=1&size=50&zoek=...&gemeente=...&provincie=...&status=...&minSlaap=...&maxSlaap=...&heeftTelefoon=...&heeftEmail=...&heeftWebsite=...
app.get("/api/panden", requireAuth, (req, res) => {
  const page = parseInt(req.query.page || "1");
  const size = Math.min(parseInt(req.query.size || "50"), 200);
  const { zoek, gemeente, provincie, status, minSlaap, maxSlaap, heeftTelefoon, heeftEmail, heeftWebsite } = req.query;

  const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
  if (total === 0) return res.json({ data: [], meta: { total: 0, page, size }, _needsSync: true });

  // Build SQL WHERE clause from filters
  const conditions = [];
  const params = [];

  if (zoek) {
    conditions.push("(LOWER(JSON_EXTRACT(data, '$.raw.attributes[\"schema:name\"]')) LIKE ? OR LOWER(municipality) LIKE ? OR JSON_EXTRACT(data, '$.raw.attributes[\"schema:address\"][\"schema:postalCode\"]') LIKE ?)");
    const q = `%${zoek.toLowerCase()}%`;
    params.push(q, q, `%${zoek}%`);
  }
  if (gemeente) { conditions.push("LOWER(municipality) LIKE ?"); params.push(`%${gemeente.toLowerCase()}%`); }
  if (provincie) { conditions.push("province = ?"); params.push(provincie); }
  if (status) { conditions.push("status = ?"); params.push(status); }
  if (minSlaap) { conditions.push("slaapplaatsen >= ?"); params.push(parseInt(minSlaap)); }
  if (maxSlaap) { conditions.push("slaapplaatsen <= ?"); params.push(parseInt(maxSlaap)); }
  if (req.query.type) { conditions.push("type = ?"); params.push(req.query.type); }
  if (req.query.regio) { conditions.push("regio = ?"); params.push(req.query.regio); }
  if (heeftTelefoon === "1") { conditions.push("phone != ''"); }
  if (heeftEmail === "1") { conditions.push("email != ''"); }
  if (heeftWebsite === "1") { conditions.push("website != ''"); }

  const where = conditions.length ? "WHERE " + conditions.join(" AND ") : "";
  const orderBy = req.query.sorteer === "nieuwste" ? "ORDER BY date_online DESC" : "";

  const filteredTotal = db.prepare(`SELECT COUNT(*) as c FROM properties ${where}`).get(...params).c;
  const offset = (page - 1) * size;
  const rows = db.prepare(`SELECT data FROM properties ${where} ${orderBy} LIMIT ? OFFSET ?`).all(...params, size, offset);

  const properties = rows.map(r => {
    const parsed = JSON.parse(r.data);
    return parseLodging(parsed.raw || parsed, parsed.included || []);
  });

  res.json({
    data: properties,
    meta: { total: filteredTotal, dbTotal: total, page, size, pages: Math.ceil(filteredTotal / size) },
  });
});

// GET /api/panden/count
app.get("/api/panden/count", requireAuth, (req, res) => {
  const total = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
  const lastFetch = db.prepare("SELECT MAX(fetched_at) as t FROM properties").get().t;
  res.json({ total, lastFetch });
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
app.get("/api/enrichment/:id", (req, res) => {
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
app.get("/api/enrichment/stale", (req, res) => {
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
  const sqlDb = await initDb(DB_PATH);
  db = new Database(sqlDb);
  db.exec(DB_SCHEMA);

  // Migrations: add columns to existing tables if they don't exist yet
  const existingCols = db.prepare("PRAGMA table_info(properties)").all().map(r => r.name);
  const newCols = [
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
  ];
  for (const [col, type] of newCols) {
    if (!existingCols.includes(col)) {
      db.exec(`ALTER TABLE properties ADD COLUMN ${col} ${type}`);
      console.log(`[migration] Added column: ${col}`);
    }
  }
  // Create indexes now that columns are guaranteed to exist
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_municipality ON properties(municipality)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_province ON properties(province)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_props_status ON properties(status)`);
  // Backfill any rows with empty municipality (existing data)
  const unfilled = db.prepare("SELECT COUNT(*) as c FROM properties WHERE municipality IS NULL OR municipality = ''").get().c;
  if (unfilled > 0) {
    console.log(`[migration] Will backfill ${unfilled} rows in background...`);
    setTimeout(() => {
      console.log("[migration] Starting backfill...");
      try {
        const rows = db.prepare("SELECT id, data FROM properties WHERE municipality IS NULL OR municipality = ''").all();
        const upd = db.prepare("UPDATE properties SET municipality=?, province=?, status=?, slaapplaatsen=?, phone=?, email=?, website=?, type=?, regio=?, date_online=? WHERE id=?");
        let count = 0;
        for (const row of rows) {
          try {
            const stored = JSON.parse(row.data);
            const p = parseLodging(stored.raw || stored, stored.included || []);
            upd.run(p.municipality||"", p.province||"", p.status||"", p.slaapplaatsen||0, p.phone||"", p.email||"", p.website||"", p.type||"", p.toeristischeRegio||"", p.dateOnline||"", row.id);
            count++;
          } catch(e) {}
        }
        saveDb();
        console.log(`[migration] Backfill complete: ${count} rows`);
      } catch(e) { console.error("[migration] Backfill error:", e.message); }
    }, 3000); // wait 3s after server starts
  }

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
