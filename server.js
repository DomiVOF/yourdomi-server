const express = require("express");
const cors = require("cors");
const initSqlJs = require("sql.js");
const fs = require("fs");

// sql.js wrapper that mimics better-sqlite3 API
let _db = null;
let _dbPath = null;

function getDb() {
  return _db;
}

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
  constructor(db, sql) {
    this.db = db;
    this.sql = sql;
  }
  run(...args) {
    this.db.run(this.sql, args.flat());
    // Note: saveDb() is called explicitly after important mutations (login, outcomes, etc.)
    // NOT here - would be called 25k times during backfill and kill performance
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
  constructor(db) {
    this.db = db;
  }
  prepare(sql) {
    return new Statement(this.db, sql);
  }
  exec(sql) {
    this.db.run(sql);
    saveDb();
  }
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

function lookupMunicipality() {
  return null;
}

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
    postal_code TEXT
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
  origin: function (origin, callback) {
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
  return crypto
    .createHash("sha256")
    .update(password + "yourdomi_salt_2025")
    .digest("hex");
}

function generateToken() {
  return crypto.randomBytes(32).toString("hex");
}

function requireAuth(req, res, next) {
  const token = req.headers["x-auth-token"];
  if (!token) return res.status(401).json({ error: "Niet ingelogd" });
  const session = db
    .prepare("SELECT * FROM sessions WHERE token = ? AND expires_at > ?")
    .get(token, Date.now());
  if (!session) return res.status(401).json({ error: "Sessie verlopen" });
  req.user = { id: session.user_id, username: session.username, name: session.name };
  next();
}

function ensureDefaultUsers() {
  const now = Date.now();
  const defaults = [
    { username: "aaron", name: "Aaron" },
    { username: "ruben", name: "Ruben" },
    { username: "vic", name: "Vic" },
  ];
  for (const u of defaults) {
    const existing = db.prepare("SELECT id FROM users WHERE username = ?").get(u.username);
    if (!existing) {
      db.prepare(
        "INSERT INTO users (username, password_hash, name, created_at) VALUES (?, ?, ?, ?)",
      ).run(u.username, hashPassword("yourdomi2026"), u.name, now);
      console.log(`[auth] Created user: ${u.username}`);
    } else {
      db.prepare("UPDATE users SET password_hash = ?, name = ? WHERE username = ?").run(
        hashPassword("yourdomi2026"),
        u.name,
        u.username,
      );
      console.log(`[auth] Updated user: ${u.username}`);
    }
  }
  saveDb(); // persist user records so login works after restart
}

// -- TOERISME VLAANDEREN FETCH -------------------------------------------------
const TV_BASE = "https://linked.toerismevlaanderen.be/lodgings";

async function fetchPageFromTV(page = 1, size = 100) {
  const url = `${TV_BASE}?page[size]=${size}&page[number]=${page}`;
  const res = await fetch(url, {
    headers: { Accept: "application/vnd.api+json", "User-Agent": "YourdomiServer/1.0" },
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`TV API ${res.status}: ${await res.text()}`);
  return res.json();
}

// keep only included records actually referenced by this item
function pickIncludedForItem(raw, included) {
  if (!Array.isArray(included) || !included.length) return [];

  const wanted = new Set();
  const rel = raw?.relationships || {};

  const addRelData = (data) => {
    if (!data) return;
    if (Array.isArray(data)) {
      for (const d of data) if (d?.type && d?.id) wanted.add(`${d.type}:${d.id}`);
      return;
    }
    if (data?.type && data?.id) wanted.add(`${data.type}:${data.id}`);
  };

  addRelData(rel["contact-points"]?.data);
  addRelData(rel.municipality?.data);
  addRelData(rel.address?.data);
  addRelData(rel.media?.data);
  addRelData(rel["main-media"]?.data);

  const byKey = new Map(included.map((i) => [`${i.type}:${i.id}`, i]));
  for (const key of [...wanted]) {
    const obj = byKey.get(key);
    const provRef = obj?.relationships?.province?.data;
    if (provRef?.type && provRef?.id) wanted.add(`${provRef.type}:${provRef.id}`);
  }

  return [...wanted].map((k) => byKey.get(k)).filter(Boolean);
}

async function syncPropertiesFromTV() {
  console.log("[sync] Starting fast parallel sync from Toerisme Vlaanderen...");
  const PAGE_SIZE = 100;
  const CONCURRENCY = 8; // parallel requests at once
  const now = Date.now();

  const sqlVal = (v) => {
    if (v === undefined || v === null) return null;
    const t = typeof v;
    if (t === "string" || t === "number") return v;
    if (t === "boolean") return v ? 1 : 0;
    return String(v);
  };

  const insert = db.prepare(
    "INSERT OR REPLACE INTO properties (id, data, fetched_at, name, municipality, province, status, slaapplaatsen, phone, email, website, type, regio, date_online, postal_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
  );
  const insertMany = db.transaction((items) => {
    for (const item of items) {
      const parsed = parseLodging(item.raw, item.included);
      insert.run(
        sqlVal(item.id),
        sqlVal(JSON.stringify({ raw: item.raw, included: item.included })),
        sqlVal(now),
        sqlVal(parsed.name || ""),
        sqlVal(parsed.municipality || ""),
        sqlVal(parsed.province || ""),
        sqlVal(parsed.status || ""),
        sqlVal(parsed.slaapplaatsen || 0),
        sqlVal(parsed.phone || ""),
        sqlVal(parsed.email || ""),
        sqlVal(parsed.website || ""),
        sqlVal(parsed.type || ""),
        sqlVal(parsed.toeristischeRegio || ""),
        sqlVal(parsed.dateOnline || ""),
        sqlVal(parsed.postalCode || ""),
      );
    }
  });

  try {
    const firstPage = await fetchPageFromTV(1, PAGE_SIZE);
    const total = firstPage.meta?.count || firstPage.meta?.total || 0;
    if (!total) {
      console.log("[sync] No properties found.");
      return;
    }
    const totalPages = Math.ceil(total / PAGE_SIZE);
    console.log(
      `[sync] Total: ${total} properties across ${totalPages} pages — ${CONCURRENCY} parallel`,
    );

    const firstIncluded = firstPage.included || [];
    const firstItems = (firstPage.data || []).map((raw) => ({
      id: raw.id,
      raw,
      included: pickIncludedForItem(raw, firstIncluded),
    }));
    insertMany(firstItems);
    let synced = firstItems.length;

    const remainingPages = Array.from({ length: totalPages - 1 }, (_, i) => i + 2);

    for (let i = 0; i < remainingPages.length; i += CONCURRENCY) {
      const batch = remainingPages.slice(i, i + CONCURRENCY);
      const results = await Promise.allSettled(batch.map((p) => fetchPageFromTV(p, PAGE_SIZE)));

      const toStore = [];
      for (let j = 0; j < results.length; j++) {
        const r = results[j];
        if (r.status === "fulfilled") {
          const inc = r.value.included || [];
          (r.value.data || []).forEach((raw) => {
            toStore.push({ id: raw.id, raw, included: pickIncludedForItem(raw, inc) });
          });
        } else {
          const reasonText =
            (r.reason && (r.reason.message || r.reason.stack || String(r.reason))) ||
            "Unknown error";
          console.warn(`[sync] Page ${batch[j]} failed: ${reasonText} — retrying`);
          try {
            const retry = await fetchPageFromTV(batch[j], PAGE_SIZE);
            const inc = retry.included || [];
            (retry.data || []).forEach((raw) => {
              toStore.push({ id: raw.id, raw, included: pickIncludedForItem(raw, inc) });
            });
          } catch (e) {
            console.error(
              `[sync] Page ${batch[j]} retry failed: ${
                e && (e.message || e.stack || String(e))
              }`,
            );
          }
        }
      }

      insertMany(toStore);
      synced += toStore.length;
      const pct = Math.round((synced / total) * 100);
      console.log(
        `[sync] Pages ${batch[0]}-${batch[batch.length - 1]}: ${synced}/${total} (${pct}%)`,
      );

      await new Promise((r) => setTimeout(r, 100));
    }

    console.log(`[sync] Done. Synced ${synced} properties.`);
  } catch (e) {
    console.error(
      "[sync] Error:",
      e && (e.message || e.stack || String(e)) ? e.message || e.stack || String(e) : e,
    );
  }
}

// -- PARSE LODGING (same logic as frontend) -----------------------------------
const s = (v) =>
  v && typeof v === "string" ? v : Array.isArray(v) ? v[0] || "" : v ? String(v) : "";
const n = (v) => (isNaN(parseInt(v)) ? 0 : parseInt(v));

function parseLodging(raw, included = []) {
  const attr = raw.attributes || {};
  const rel = raw.relationships || {};

  let name = attr["name"] || attr["schema:name"] || `Pand ${String(raw.id || "").slice(-6)}`;

  const uri = attr["uri"] ||