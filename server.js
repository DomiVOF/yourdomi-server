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
// BACKGROUND AGENTS RUNNER (Airbnb, Booking, website, pictures)
// NOTE: These can only run while the Railway container is awake.
// If Railway scales to zero / sleeps on no traffic, nothing runs until it wakes up.
// =============================================================================
let backgroundAgentsRunning = false;
async function runAllBackgroundAgentsOnce() {
  if (process.env.PLATFORM_SCAN_ENABLED !== "1") return;
  if (!ANTHROPIC_KEY || !db) return;
  if (backgroundAgentsRunning) return;
  backgroundAgentsRunning = true;
  try {
    await Promise.all([
      runAirbnbAgentBatch(),
      runBookingAgentBatch(),
      runWebsiteAgentBatch(),
      runImageScrapeBatch(),
    ]);
  } finally {
    backgroundAgentsRunning = false;
  }
}

async function runBackgroundBurst() {
  const cycles = Math.min(parseInt(process.env.BACKGROUND_BURST_CYCLES || "3", 10), 12);
  const maxMs = Math.min(parseInt(process.env.BACKGROUND_BURST_MAX_MS || "170000", 10), 600000);
  const sleepMs = Math.max(0, parseInt(process.env.BACKGROUND_BURST_SLEEP_MS || "8000", 10));
  const started = Date.now();
  for (let i = 0; i < cycles; i++) {
    if (Date.now() - started > maxMs) break;
    await runAllBackgroundAgentsOnce();
    if (i < cycles - 1 && sleepMs > 0) await new Promise(r => setTimeout(r, sleepMs));
  }
}

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
    street      TEXT,
    phone2      TEXT
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

  CREATE TABLE IF NOT EXISTS platform_scan (
    id          TEXT PRIMARY KEY,
    data        TEXT NOT NULL,
    scanned_at  INTEGER NOT NULL
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
  CREATE INDEX IF NOT EXISTS idx_platform_scan_age ON platform_scan(scanned_at);

  CREATE INDEX IF NOT EXISTS idx_properties_name ON properties(name);
  CREATE INDEX IF NOT EXISTS idx_properties_municipality ON properties(municipality);
  CREATE INDEX IF NOT EXISTS idx_properties_province ON properties(province);
  CREATE INDEX IF NOT EXISTS idx_properties_status ON properties(status);
  CREATE INDEX IF NOT EXISTS idx_properties_type ON properties(type);
  CREATE INDEX IF NOT EXISTS idx_properties_regio ON properties(regio);
  CREATE INDEX IF NOT EXISTS idx_properties_slaapplaatsen ON properties(slaapplaatsen);
  CREATE INDEX IF NOT EXISTS idx_properties_date_online ON properties(date_online);
  CREATE INDEX IF NOT EXISTS idx_properties_postal_code ON properties(postal_code);
  CREATE INDEX IF NOT EXISTS idx_properties_street ON properties(street);
  CREATE INDEX IF NOT EXISTS idx_properties_phone ON properties(phone);
  CREATE INDEX IF NOT EXISTS idx_properties_fetched_at ON properties(fetched_at);
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

const TV_BASE = "https://linked.toerismevlaanderen.be/lodgings";

async function fetchPageFromTV(page = 1, size = 100) {
  // Only these includes are accepted by the API (municipality causes 500)
  const includes = ["address", "welcome-addresses", "contact-points", "registrations", "main-media", "media"].join(",");
  const url = `${TV_BASE}?page[size]=${size}&page[number]=${page}&include=${includes}`;
  const res = await fetch(url, {
    headers: { Accept: "application/vnd.api+json", "User-Agent": "YourdomiServer/1.0" },
    signal: AbortSignal.timeout(60000),
  });
  if (!res.ok) throw new Error(`TV API ${res.status}: ${await res.text()}`);
  return res.json();
}

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
  addRelData(rel.address?.data);
  addRelData(rel["welcome-addresses"]?.data);
  addRelData(rel.registrations?.data);
  addRelData(rel.media?.data);
  addRelData(rel["main-media"]?.data);
  const addrRef = rel.address?.data ?? rel["welcome-addresses"]?.data;
  const addrSingle = addrRef != null ? (Array.isArray(addrRef) ? addrRef[0] : addrRef) : null;
  if (addrSingle) {
    const addr = included.find((i) => i.id === addrSingle.id);
    if (addr?.relationships?.province?.data) addRelData(addr.relationships.province.data);
    if (addr?.relationships?.municipality?.data) addRelData(addr.relationships.municipality.data);
  }

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
  const CONCURRENCY = 8;
  const now = Date.now();

  const sqlVal = (v) => {
    if (v === undefined || v === null) return null;
    const t = typeof v;
    if (t === "string" || t === "number") return v;
    if (t === "boolean") return v ? 1 : 0;
    return String(v);
  };

  const insert = db.prepare(
    "INSERT OR REPLACE INTO properties (id, data, fetched_at, name, municipality, province, status, slaapplaatsen, phone, phone2, email, website, type, regio, date_online, postal_code, street) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        sqlVal(parsed.phone2 || ""),
        sqlVal(parsed.email || ""),
        sqlVal(parsed.website || ""),
        sqlVal(parsed.type || ""),
        sqlVal(parsed.toeristischeRegio || ""),
        sqlVal(parsed.dateOnline || ""),
        sqlVal(parsed.postalCode || ""),
        sqlVal(parsed.street || ""),
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

const s = (v) =>
  v && typeof v === "string" ? v : Array.isArray(v) ? v[0] || "" : v ? String(v) : "";
const n = (v) => (isNaN(parseInt(v)) ? 0 : parseInt(v));

function lookupMunicipality() {
  return null;
}

function findIncluded(ref, included) {
  if (!ref || !Array.isArray(included) || !included.length) return null;
  const id = ref.id;
  const type = ref.type;
  const byTypeAndId = included.find((i) => i.type === type && i.id === id);
  if (byTypeAndId) return byTypeAndId;
  return included.find((i) => i.id === id) || null;
}

function parseLodging(raw, included = []) {
  const attr = raw.attributes || {};
  const rel = raw.relationships || {};

  const uri = attr["uri"] || attr["@id"] || "";
  let province = "",
    postalCode = "",
    toeristischeRegio = "";

  const one = (obj, keys) => {
    if (!obj) return "";
    for (const k of keys) {
      let v = obj[k];
      if (v == null) continue;
      if (typeof v === "string" && v.trim()) return v.trim();
      if (typeof v === "object" && v["@value"] != null) return String(v["@value"]).trim() || "";
      if (typeof v === "object" && v.value != null) return String(v.value).trim() || "";
      // TV API: name is array of { content, language }
      if (Array.isArray(v) && v.length) {
        const first = v[0];
        const str = first?.content ?? first?.value ?? first?.["@value"];
        if (str != null && String(str).trim()) return String(str).trim();
      }
    }
    return "";
  };

  // ---- 0. ADDRESS REF (used for street, municipality, postalCode) ----
  const addrRef =
    rel.address?.data ??
    rel.addresses?.data ??
    rel["welcome-addresses"]?.data ??
    rel["onthaalAdres"]?.data;
  const addrSingle = addrRef != null ? (Array.isArray(addrRef) ? addrRef[0] : addrRef) : null;
  const addr = addrSingle ? findIncluded(addrSingle, included) : null;
  const addrAttr = addr?.attributes || {};

  // ---- 1. STREET (TV API uses thoroughfare, sometimes with house-number) ----
  const streetKeys = ["street", "straat", "thoroughfare", "locn:thoroughfare", "streetAddress", "schema:streetAddress", "addressStreet", "locn:locatorDesignator"];
  let street = one(attr, streetKeys);
  if (!street && attr["schema:address"] && typeof attr["schema:address"] === "object") {
    street = one(attr["schema:address"], streetKeys);
  }
  if (!street && addrAttr) {
    street = one(addrAttr, streetKeys) || one(addrAttr, ["fullAddress", "locn:fullAddress"]);
    const houseNum = one(addrAttr, ["house-number", "houseNumber", "locatorDesignator"]);
    if (street && houseNum) street = `${street} ${houseNum}`.trim();
    else if (houseNum) street = houseNum;
  }
  street = s(street);

  // ---- 2. CITY / MUNICIPALITY (TV API address has municipality, province) ----
  const cityKeys = ["municipality", "addressLocality", "schema:addressLocality", "locality", "gemeente", "hoofdgemeente", "city", "adminUnitL2", "locn:adminUnitL2"];
  let municipality = one(attr, cityKeys);
  if (!municipality && attr["schema:address"] && typeof attr["schema:address"] === "object") {
    municipality = one(attr["schema:address"], cityKeys);
  }
  if (!municipality && addrAttr) municipality = one(addrAttr, cityKeys) || one(addrAttr, ["name"]) || (addrAttr.name && String(addrAttr.name).trim()) || "";
  if (!province && addrAttr) province = one(addrAttr, ["province", "adminUnitL1", "schema:addressRegion"]);
  const muniRef = rel.municipality?.data;
  const muniSingle = muniRef != null ? (Array.isArray(muniRef) ? muniRef[0] : muniRef) : null;
  const muni = muniSingle ? findIncluded(muniSingle, included) : null;
  if (!municipality && muni?.attributes) {
    municipality = one(muni.attributes, ["name", "schema:name", ...cityKeys]);
    const provRef = muni.relationships?.province?.data;
    if (provRef && !province) {
      const prov = findIncluded(Array.isArray(provRef) ? provRef[0] : provRef, included);
      if (prov?.attributes) province = prov.attributes.name || prov.attributes["schema:name"] || "";
    }
  }
  const luEntry = lookupMunicipality(raw.id, uri);
  if (luEntry) {
    if (!municipality) municipality = luEntry[1] || "";
    if (!province) province = luEntry[2] || "";
    if (!postalCode) postalCode = luEntry[3] || "";
    toeristischeRegio = luEntry[4] || "";
  }
  municipality = s(municipality);
  province = s(province);

  // ---- POSTAL CODE (TV API uses post-code) ----
  const postalKeys = ["postalCode", "postcode", "postal-code", "post-code", "locn:postCode", "schema:postalCode"];
  if (!postalCode) postalCode = one(attr, postalKeys);
  if (!postalCode && addrAttr) postalCode = one(addrAttr, postalKeys);
  postalCode = s(postalCode);

  // ---- 3. NAME: use real lodging name from API first; fallback to "Vakantiewoning, Straat, Stad" ----
  const nameKeys = ["name", "schema:name", "title", "label", "prefLabel", "dcterms:title"];
  let rawName = one(attr, nameKeys);
  if (!rawName && rel.registrations?.data) {
    const regRef = Array.isArray(rel.registrations.data) ? rel.registrations.data[0] : rel.registrations.data;
    const reg = regRef ? findIncluded(regRef, included) : null;
    if (reg?.attributes) rawName = one(reg.attributes, nameKeys);
  }
  const genericPatterns = /^(pand\s*\d*|vakantiewoning|naamloze|no name|—|\s*)$/i;
  const isGeneric = !rawName || genericPatterns.test(rawName.trim());
  const displayName = !isGeneric
    ? rawName.trim()
    : (["Vakantiewoning", street, municipality].filter(Boolean).join(", ") || "Vakantiewoning");

  // ---- 4. CONTACT ----
  const cpData = rel["contact-points"]?.data;
  const contactPoints = Array.isArray(cpData) ? cpData : (cpData ? [cpData] : []);
  const phones = [], emails = [], websites = [];
  const pick = (obj, keys) => { for (const k of keys) { const v = obj?.[k]; if (v != null && String(v).trim()) return String(v).trim(); } return ""; };
  const phoneKeys = ["schema:telephone", "telephone", "phone", "contact-phone", "tel", "fax"];
  const emailKeys = ["schema:email", "email", "contact-email", "e-mail", "mail"];
  const urlKeys = ["schema:url", "url", "contact-website", "website", "homepage"];
  for (const cp of contactPoints) {
    const c = findIncluded(cp, included);
    if (!c?.attributes) continue;
    const t = pick(c.attributes, phoneKeys); if (t) phones.push(t);
    const e = pick(c.attributes, emailKeys); if (e) emails.push(e);
    const u = pick(c.attributes, urlKeys); if (u) websites.push(u);
  }
  if (!phones.length) { const t = pick(attr, phoneKeys); if (t) phones.push(t); }
  if (!emails.length) { const e = pick(attr, emailKeys); if (e) emails.push(e); }
  if (!websites.length) { const w = pick(attr, urlKeys); if (w) websites.push(w); }
  const cleanPhone = (p) =>
    s(p).replace(/\s/g, "").replace(/^tel:/i, "").replace(/^00/, "+").replace(/^\+?0032/, "+32");
  const phone = phones[0] ? cleanPhone(phones[0]) : null;
  const phone2 = phones[1] ? cleanPhone(phones[1]) : null;
  const phoneNorm = phone ? phone.replace(/[^0-9+]/g, "") : null;

  const mainMedia = rel["main-media"]?.data ? [rel["main-media"].data] : [];
  const allMediaRefs = ([]).concat(rel.media?.data || [], mainMedia).filter(Boolean);
  const images = allMediaRefs
    .map((m) => findIncluded(m, included)?.attributes?.contentUrl)
    .filter(Boolean)
    .slice(0, 5);

  const slaapplaatsen = parseInt(
    attr["number-of-sleeping-places"] || attr["number-of-sleep-places"] || attr["schema:numberOfRooms"] || 0,
  );
  const status = attr["registration-status"] || attr.status || "";
  const type = attr["category"] || attr["dcterms:type"] || attr["schema:additionalType"] || attr.type || "";
  const dateOnline = attr["modified"] || attr["registration-date"] || attr["dcterms:created"] || "";

  return {
    id: s(raw.id),
    name: displayName,
    municipality,
    province,
    toeristischeRegio: s(toeristischeRegio),
    type: s(type),
    postalCode,
    street,
    sleepPlaces: n(
      attr["number-of-sleeping-places"] ||
        attr["number-of-sleep-places"] ||
        attr["schema:numberOfRooms"] ||
        0,
    ),
    slaapplaatsen: n(
      attr["number-of-sleeping-places"] ||
        attr["number-of-sleep-places"] ||
        attr["schema:numberOfRooms"] ||
        0,
    ),
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

// Geen exclusie meer: alle panden tonen/syncen (Railway heeft meer geheugen)
const BASE_WHERE = " 1=1 ";
const BASE_PARAMS = [];

// GET /api/panden — disable cache so client always gets fresh data (avoids 304 with stale/empty body)
app.get("/api/panden", requireAuth, (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  try {
    const page = parseInt(req.query.page || "1");
    const size = Math.min(parseInt(req.query.size || "50"), 200);
    const { zoek, gemeente, provincie, status, minSlaap, maxSlaap, heeftTelefoon, heeftEmail, heeftWebsite } = req.query;

    const totalRow = db.prepare(
      `SELECT COUNT(*) as c FROM properties WHERE ${BASE_WHERE}`
    ).get(...BASE_PARAMS);
    const total = totalRow?.c ?? 0;
    if (total === 0) return res.json({ data: [], meta: { total: 0, page, size }, _needsSync: true });

    const conditions = [BASE_WHERE];
    const params = [...BASE_PARAMS];

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

    const where   = "WHERE " + conditions.join(" AND ");
    const sortMap = {
      nieuwste: "ORDER BY date_online DESC",
      oudste: "ORDER BY date_online ASC",
      naam: "ORDER BY name ASC",
      naam_desc: "ORDER BY name DESC",
      gemeente: "ORDER BY municipality ASC",
      gemeente_desc: "ORDER BY municipality DESC",
      provincie: "ORDER BY province ASC",
      slaapplaatsen: "ORDER BY slaapplaatsen DESC",
      slaapplaatsen_asc: "ORDER BY slaapplaatsen ASC",
      straat: "ORDER BY street ASC",
      postcode: "ORDER BY postal_code ASC",
    };
    const orderBy = sortMap[req.query.sorteer] || "ORDER BY name ASC";

    const filteredTotal = db.prepare(`SELECT COUNT(*) as c FROM properties ${where}`).get(...params).c;
    const offset = (page - 1) * size;
    const rows = db
      .prepare(
        `SELECT id, data, name, municipality, province, status, slaapplaatsen, phone, phone2, email, website, type, regio, date_online, postal_code, street FROM properties ${where} ${orderBy} LIMIT ? OFFSET ?`
      )
      .all(...params, size, offset);

    const properties = rows.map(r => {
      try {
        const data = JSON.parse(r.data);
        if (data && data.raw && Array.isArray(data.included)) return parseLodging(data.raw, data.included);
        if (data && data.name !== undefined && !data.raw) return data;
        if (data && typeof data === "object") return data;
      } catch (_) {}
      // Fallback: build minimal object from indexed columns so we never drop rows
      return {
        id: r.id,
        name: ["Vakantiewoning", r.street, r.municipality].filter(Boolean).join(", ") || r.name || "Vakantiewoning",
        municipality: r.municipality || "",
        province: r.province || "",
        street: r.street || "",
        postalCode: r.postal_code || "",
        status: r.status || "",
        slaapplaatsen: r.slaapplaatsen || 0,
        sleepPlaces: r.slaapplaatsen || 0,
        units: 1,
        phone: r.phone || null,
        phone2: r.phone2 || null,
        email: r.email || null,
        website: r.website || null,
        type: r.type || "",
        toeristischeRegio: r.regio || "",
        dateOnline: r.date_online || "",
        onlineSince: r.date_online || "",
      };
    });

    const totalNum = Number(filteredTotal) || 0;
    const pagesNum = Math.ceil(totalNum / size) || 0;
    res.json({
      data: properties,
      meta: {
        total: totalNum,
        dbTotal: Number(total) || 0,
        page,
        size,
        pages: pagesNum,
        sort: req.query.sorteer || "naam",
      },
    });
  } catch (e) {
    console.error("[/api/panden]", e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/panden/count
app.get("/api/panden/count", requireAuth, (req, res) => {
  const row = db
    .prepare(`SELECT COUNT(*) as c FROM properties WHERE ${BASE_WHERE}`)
    .get(...BASE_PARAMS);
  const total = row?.c ?? 0;
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

// GET /api/meta — facetwaarden (alle panden)
app.get("/api/meta", requireAuth, (req, res) => {
  const baseWhere = `WHERE ${BASE_WHERE}`;
  const baseParams = [...BASE_PARAMS];
  const provinces = db.prepare(`SELECT DISTINCT province FROM properties ${baseWhere} AND province IS NOT NULL AND TRIM(COALESCE(province,'')) != '' ORDER BY "province"`).all(...baseParams).map(r => r.province);
  const municipalities = db.prepare(`SELECT DISTINCT municipality FROM properties ${baseWhere} AND municipality IS NOT NULL AND TRIM(COALESCE(municipality,'')) != '' ORDER BY "municipality"`).all(...baseParams).map(r => r.municipality);
  const types = db.prepare(`SELECT DISTINCT type FROM properties ${baseWhere} AND type IS NOT NULL AND TRIM(COALESCE(type,'')) != '' ORDER BY "type"`).all(...baseParams).map(r => r.type);
  const regios = db.prepare(`SELECT DISTINCT regio FROM properties ${baseWhere} AND regio IS NOT NULL AND TRIM(COALESCE(regio,'')) != '' ORDER BY "regio"`).all(...baseParams).map(r => r.regio);
  const statuses = db.prepare(`SELECT DISTINCT status FROM properties ${baseWhere} AND status IS NOT NULL AND TRIM(COALESCE(status,'')) != '' ORDER BY "status"`).all(...baseParams).map(r => r.status);
  const slaapRange = db.prepare(`SELECT MIN(slaapplaatsen) as minSlaap, MAX(slaapplaatsen) as maxSlaap FROM properties ${baseWhere} AND slaapplaatsen IS NOT NULL`).get(...baseParams);
  const total = db.prepare(`SELECT COUNT(*) as c FROM properties ${baseWhere}`).get(...baseParams).c;
  res.json({
    provinces,
    municipalities,
    types,
    regios,
    statuses,
    slaapplaatsenRange: total ? { min: slaapRange?.minSlaap ?? 0, max: slaapRange?.maxSlaap ?? 0 } : null,
    total,
    sortOptions: [
      { value: "naam", label: "Naam A–Z" },
      { value: "naam_desc", label: "Naam Z–A" },
      { value: "gemeente", label: "Gemeente A–Z" },
      { value: "gemeente_desc", label: "Gemeente Z–A" },
      { value: "provincie", label: "Provincie" },
      { value: "nieuwste", label: "Nieuwste eerst" },
      { value: "oudste", label: "Oudste eerst" },
      { value: "slaapplaatsen", label: "Meeste slaapplaatsen" },
      { value: "slaapplaatsen_asc", label: "Minste slaapplaatsen" },
      { value: "straat", label: "Straat" },
      { value: "postcode", label: "Postcode" },
    ],
  });
});

// GET /api/health
app.get("/api/health", requireAuth, (req, res) => {
  const props = db.prepare(`SELECT COUNT(*) as c FROM properties WHERE ${BASE_WHERE}`).get(...BASE_PARAMS)?.c ?? 0;
  const enriched = db.prepare("SELECT COUNT(*) as c FROM enrichment").get().c;
  const outcomes = db.prepare("SELECT COUNT(*) as c FROM outcomes").get().c;
  const lastSync = db.prepare("SELECT MAX(fetched_at) as t FROM properties").get()?.t;
  const withPhone = props;
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

app.post("/api/sync/tv", (req, res) => {
  res.json({ ok: true, message: "TV sync started (Toerisme Vlaanderen). Duurt enkele minuten." });
  syncPropertiesFromTV().catch(console.error);
});
app.get("/api/sync/tv", (req, res) => {
  res.json({ ok: true, message: "TV sync started (Toerisme Vlaanderen). Duurt enkele minuten." });
  syncPropertiesFromTV().catch(console.error);
});

// POST /api/backfill — re-parse stored data (raw+included) and re-index into columns
app.post("/api/backfill", requireAuth, (req, res) => {
  res.json({ ok: true, message: "Backfill started" });
  setTimeout(() => {
    try {
      const rows = db.prepare("SELECT id, data FROM properties").all();
      const upd = db.prepare(
        "UPDATE properties SET name=?,municipality=?,province=?,status=?,slaapplaatsen=?,phone=?,phone2=?,email=?,website=?,type=?,regio=?,date_online=?,postal_code=?,street=? WHERE id=?"
      );
      let n = 0;
      for (const row of rows) {
        try {
          const p = JSON.parse(row.data);
          const parsed = p.raw && Array.isArray(p.included) ? parseLodging(p.raw, p.included) : p;
          upd.run(
            parsed.name ?? "", parsed.municipality ?? "", parsed.province ?? "", parsed.status ?? "",
            parsed.slaapplaatsen ?? 0, parsed.phone ?? "", parsed.phone2 ?? "", parsed.email ?? "", parsed.website ?? "",
            parsed.type ?? "", parsed.toeristischeRegio ?? "", parsed.dateOnline ?? "", parsed.postalCode ?? "", parsed.street ?? "",
            row.id
          );
          n++;
        } catch (_) {}
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

// POST /api/admin/reset-db — helemaal opnieuw: leeg properties, enrichment, outcomes (voor fresh TV-sync)
app.post("/api/admin/reset-db", requireAuth, (req, res) => {
  try {
    const p = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    db.prepare("DELETE FROM properties").run();
    db.prepare("DELETE FROM enrichment").run();
    db.prepare("DELETE FROM outcomes").run();
    console.log("[admin] DB reset — properties, enrichment, outcomes cleared.");
    res.json({
      ok: true,
      message: "Database gereset. Run nu ‘Sync TV-data nu’ om opnieuw te vullen.",
      deletedProperties: p,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/admin/remove-no-city — uitgeschakeld: geen exclusie/verwijdering meer (Railway heeft meer geheugen)
app.post("/api/admin/remove-no-city", requireAuth, (req, res) => {
  res.json({
    ok: true,
    message: "Cleanup uitgeschakeld. Alle panden blijven behouden.",
    deleted: 0,
  });
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

// GET /api/debug/tv-sample — fetch one page from TV API and return first lodging + included (to inspect real structure)
app.get("/api/debug/tv-sample", async (req, res) => {
  try {
    const json = await fetchPageFromTV(1, 5);
    const data = json.data || [];
    const included = json.included || [];
    const first = data[0];
    if (!first) return res.json({ message: "No data in TV response", meta: json.meta });
    const itemIncluded = pickIncludedForItem(first, included);
    const parsed = parseLodging(first, itemIncluded);
    res.json({
      meta: json.meta,
      firstRaw: first,
      firstIncluded: itemIncluded,
      firstParsed: parsed,
      allTypesInIncluded: [...new Set(included.map((i) => i.type))],
      relationshipKeys: first ? Object.keys(first.relationships || {}) : [],
      attributeKeys: first && first.attributes ? Object.keys(first.attributes) : [],
    });
  } catch (e) {
    res.status(500).json({ error: e.message || String(e), stack: e.stack });
  }
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

// POST /api/admin/run-background-agents — manual trigger (useful for Railway Cron)
// Security: requires ADMIN_PASSWORD (does not rely on logged-in browser session).
app.post("/api/admin/run-background-agents", (req, res) => {
  const adminPw = process.env.ADMIN_PASSWORD;
  if (!adminPw) return res.status(403).json({ error: "ADMIN_PASSWORD not set" });
  const provided = (req.body && (req.body.adminPassword || req.body.password)) || req.headers["x-admin-password"];
  if (String(provided || "") !== String(adminPw)) return res.status(403).json({ error: "Wrong admin password" });
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: "ANTHROPIC_KEY not configured" });
  if (!db) return res.status(500).json({ error: "Database not ready" });
  runAllBackgroundAgentsOnce()
    .then(() => res.json({ ok: true }))
    .catch((e) => res.status(500).json({ error: e.message }));
});

// =============================================================================
// ONE-TIME: Full AI enrichment for all type=vakantiewoning
// =============================================================================
function normalizePhoneForGroup(s) {
  if (!s || typeof s !== "string") return "";
  return s.replace(/\D/g, "").slice(-9) || s.replace(/\D/g, "");
}

function buildFullEnrichmentPrompt(property, portfolioInfo) {
  const portfolioContext = portfolioInfo
    ? `\nBELANGRIJK - PORTFOLIO EIGENAAR: Deze eigenaar heeft ${portfolioInfo.count} panden: ${portfolioInfo.names.join(", ")}\nDit is een HOGE PRIORITEIT portfolio lead. Verwerk dit expliciet in de openingszin.`
    : "";
  const name = property.name || "onbekend";
  const street = property.street || "";
  const postalCode = property.postalCode || "";
  const municipality = property.municipality || "onbekend";
  const province = property.province || "";
  const status = property.status || "";
  const starRating = property.starRating || "geen";
  const sleepPlaces = property.sleepPlaces ?? property.slaapplaatsen ?? "?";
  const units = property.units ?? 1;
  const phone = property.phone || "niet beschikbaar";
  const email = property.email || "niet beschikbaar";
  const website = property.website || "niet gevonden";
  const websiteStep = website ? `6. web_fetch "${website}" → controleer HTTP status. Als 200 met echte verhuurcontent: werkt=true en zoek foto URLs. Bij fout/parkeerdomein: werkt=false, gevonden=false.` : "";

  return `Je bent een verkoopintelligentie-assistent voor yourdomi.be, een Belgisch beheerbedrijf voor kortetermijnverhuur (Airbnb, Booking.com, VRBO).
VERKOOPSFILOSOFIE: Wij stellen vragen ipv uitleggen wie we zijn. We laten eigenaars zichzelf "verkopen" door te vragen naar hun situatie, pijnpunten en wensen. Goede verkopers luisteren 70%, spreken 30%.${portfolioContext}

Pandgegevens uit Toerisme Vlaanderen register:
- Naam: ${name}
- Adres: ${street}, ${postalCode} ${municipality}, ${province}
- Status: ${status} | Sterren: ${starRating} | Slaapplaatsen: ${sleepPlaces} | Units: ${units}
- Tel: ${phone} | Email: ${email} | Website: ${website}

STAP 1 - Online aanwezigheid zoeken (VERPLICHT - gebruik web_search + web_fetch):
Zoek systematisch met deze queries (doe elke zoekactie apart):
1. web_search: "${(name || "").slice(0, 40)} ${municipality} Airbnb" → zoek exacte Airbnb listing URL (airbnb.com/rooms/...)
2. web_search: "${(name || "").slice(0, 40)} ${municipality} Booking.com" → zoek exacte Booking URL (booking.com/hotel/...)
3. web_search: "${(name || "").slice(0, 40)} ${municipality} vakantiewoning" → zoek directe website
4. Als Airbnb URL gevonden: web_fetch de listing pagina → extraheer foto URLs (a0.muscache.com CDN), prijs, beoordeling, en inhoud van gastreviews (tekst of snippets).
5. Als Booking URL gevonden: web_fetch de listing pagina → extraheer foto URLs (cf.bstatic.com CDN), prijs, beoordeling, en inhoud van gastreviews.
${websiteStep ? websiteStep + "\n\n" : ""}REVIEWS VOOR VERKOOPGESPREK: Als je een Airbnb- of Booking-listing hebt opgehaald, analyseer de gastreviews (of review-snippets op de pagina / in zoekresultaten). Zoek terugkerende thema's die we in het verkoopgesprek kunnen gebruiken, bv.: slechte of inconsistente schoonmaak, lawaai/geluid, parkeren, trage of slechte communicatie, ontbrekende voorzieningen, prijs/kwaliteit. Geef 2-5 korte punten in "reviewThemes" (Nederlands). Geen punten = lege array. Zet "slechteReviews" op true als de gastreviews overwegend negatief zijn of terugkerende klachten noemen die een duidelijke verbeterkans geven.

BELANGRIJK voor websites:
- Voeg ALLEEN een website toe als je deze effectief hebt kunnen ophalen via web_fetch en hij HTTP 200 teruggeeft met echte vakantieverhuur content
- Als de fetch faalt (timeout, 404, 403, redirect naar parkeerdomein), zet directWebsite.werkt = false en directWebsite.gevonden = false
- Parkeer/placeholder sites (bv. "This domain is for sale", Sedo, GoDaddy) tellen NIET als werkende website
- Zet directWebsite.poorlyBuilt = true als de site WEL werkt maar slecht is: verouderd design, kapotte layout, geen boekingsmogelijkheid, amateuraanpak. Dat is een HEET-signaal (eigenaar kan baat hebben bij yourdomi).
- Geef ECHTE foto URLs terug die je hebt gevonden via web_fetch op de listing pagina (airbnb CDN: a0.muscache.com, booking CDN: cf.bstatic.com) - geen placeholders
- Als je geen foto URLs kan extraheren uit de pagina inhoud, geef een lege array terug

STAP 2 - Agentuur detectie:
Analyseer of het telefoonnummer/email waarschijnlijk een beheerskantoor/agentuur is ipv de eigenaar zelf. Signalen: generiek emaildomein, bekende vastgoedkantoren, meerdere panden op hetzelfde nr, "info@" adressen van vakantieverhuurders.

STAP 3 - Consultieve gespreksstructuur:
Maak 5-7 open vragen die de eigenaar aan het woord laten. Begin met de situatie peilen, dan pijnpunten, dan wensen. NIET pitchen, NIET uitleggen - VRAGEN. Structuur: situatievragen -> implicatievragen -> wensvragen.

SCORE CRITERIA (volg dit strikt):
🔥 HEET = Eigenaar beheert ZELF (geen agentuur), heeft directe contactgegevens, pand is NIET of slecht online (kans om waarde te tonen), OF staat al online maar heeft lage reviews/slechte prijszetting (duidelijke pijnpunten). Een SLECHT GEBOUWDE WEBSITE (verouderd, kapotte layout, geen boekingsmogelijkheid) telt als extra HEET-signaal.
W WARM = Eigenaar beheert zelf maar is al redelijk goed online. Of: contactgegevens beschikbaar maar onduidelijk of zelf beheert. Bellen loont maar minder urgent.
K KOUD = Duidelijk al professioneel beheerd (agentuur gedetecteerd), geen contactgegevens, of pand is al perfect geoptimaliseerd zonder ruimte voor yourdomi.

PRIORITEIT: Geef 1-10. Als er een Airbnb- of Booking.com-listing is gevonden, tel +2 bij de prioriteit (max 10) zodat onze bellers deze eigenaars sneller kunnen contacteren.

Geef ALLEEN deze JSON (geen markdown):
{
  "score": "HEET"|"WARM"|"KOUD",
  "scoreReden": "Concrete reden op basis van de criteria: waarom precies HEET/WARM/KOUD? Vermeld specifiek: beheert zelf of agentuur? Online aanwezig of niet? Ruimte voor verbetering? Max 2 zinnen.",
  "prioriteit": 1-10,
  "openingszin": "Als NIET online gevonden: stel meteen een vraag of ze online zichtbaar zijn en waar ze staan. Als WEL gevonden: verwijs concreet naar hun listing/locatie/portfolio. Max 2 zinnen. NOOIT jezelf introduceren als 'wij zijn...', altijd starten vanuit hun situatie.",
  "consultieveVragen": ["Vraag 1...", "Vraag 2...", "..."],
  "waarschuwingAgentuur": true|false,
  "agentuurSignalen": "Uitleg of leeg",
  "pitchhoek": "Na de vragen: wat biedt yourdomi specifiek voor DEZE eigenaar. 2 zinnen.",
  "zwaktes": ["verbeterpunt 1", "verbeterpunt 2", "..."],
  "reviewThemes": ["terugkerend punt uit reviews", "..."],
  "slechteReviews": true|false,
  "airbnb": { "gevonden": true|false, "url": "...", "beoordeling": "...", "aantalReviews": "...", "prijsPerNacht": "...", "bezettingsgraad": "...", "fotoUrls": [] },
  "booking": { "gevonden": true|false, "url": "...", "beoordeling": "...", "aantalReviews": "...", "prijsPerNacht": "...", "fotoUrls": [] },
  "directWebsite": { "gevonden": true|false, "werkt": true|false, "poorlyBuilt": true|false, "url": "...", "fotoUrls": [] },
  "alleFotos": [],
  "geschatMaandelijksInkomen": "...",
  "geschatBezetting": "...",
  "inkomensNota": "...",
  "potentieelMetYourDomi": "...",
  "potentieelNota": "...",
  "locatieHighlights": [],
  "eigenaarProfiel": "...",
  "contractadvies": "full"|"partial"|"visibility",
  "contractUitleg": "..."
}`;
}

let fullAiVakantiewoningRunning = false;

async function runOneFullEnrichment(property, portfolioInfo) {
  const prompt = buildFullEnrichmentPrompt(property, portfolioInfo);
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 2500,
        tools: [{ type: "web_search_20250305", name: "web_search" }],
        messages: [{ role: "user", content: prompt }],
      }),
      signal: AbortSignal.timeout(120000),
    });
    const data = await r.json();
    const textBlock = [...(data.content || [])].reverse().find((c) => c.type === "text");
    const raw = textBlock?.text || "{}";
    const clean = raw.replace(/```json|```/g, "").trim();
    const s = clean.indexOf("{");
    const e = clean.lastIndexOf("}");
    return JSON.parse(clean.slice(s, e + 1));
  } catch (e) {
    console.warn("[full-ai-vakantiewoning] Error for", property.id, e.message);
    return {
      score: "WARM",
      scoreReden: "Analyse mislukt: " + (e.message || "timeout"),
      prioriteit: 5,
      openingszin: `Goedemiddag, ik bel over uw vakantiewoning in ${property.municipality || "België"}.`,
      pitchhoek: "yourdomi.be kan uw kortetermijnverhuur volledig beheren.",
      zwaktes: [],
      reviewThemes: [],
      slechteReviews: false,
      airbnb: { gevonden: false },
      booking: { gevonden: false },
      directWebsite: { gevonden: false },
      alleFotos: [],
      geschatMaandelijksInkomen: "Onbekend",
      geschatBezetting: "Onbekend",
      inkomensNota: "",
      potentieelMetYourDomi: "Onbekend",
      potentieelNota: "",
      locatieHighlights: [],
      eigenaarProfiel: "",
      consultieveVragen: [],
      waarschuwingAgentuur: false,
      agentuurSignalen: "",
      contractadvies: "partial",
      contractUitleg: "",
    };
  }
}

app.post("/api/admin/run-full-ai-vakantiewoning", requireAuth, async (req, res) => {
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: "ANTHROPIC_KEY not set" });
  if (fullAiVakantiewoningRunning) return res.status(409).json({ error: "Job already running" });
  if (!db) return res.status(500).json({ error: "Database not ready" });

  const rows = db
    .prepare(
      `SELECT id, data, name, municipality, province, status, slaapplaatsen, phone, phone2, email, website, type, regio, date_online, postal_code, street
       FROM properties
       WHERE LOWER(TRIM(COALESCE(type, ''))) LIKE '%vakantiewoning%'
       ORDER BY name ASC`
    )
    .all();

  const properties = rows.map((r) => {
    let parsed = {};
    try {
      if (r.data) parsed = JSON.parse(r.data);
    } catch (_) {}
    const fromRaw = parsed.raw && Array.isArray(parsed.included) ? parseLodging(parsed.raw, parsed.included) : null;
    const p = fromRaw || parsed;
    return {
      id: r.id,
      name: r.name || p?.name || "Vakantiewoning",
      street: r.street || p?.street || "",
      postalCode: r.postal_code || p?.postalCode || "",
      municipality: r.municipality || p?.municipality || "",
      province: r.province || p?.province || "",
      status: r.status || p?.status || "",
      starRating: p?.starRating || null,
      sleepPlaces: p?.sleepPlaces ?? r.slaapplaatsen ?? null,
      slaapplaatsen: r.slaapplaatsen ?? p?.slaapplaatsen ?? null,
      units: p?.units ?? 1,
      phone: r.phone || p?.phone || null,
      phone2: r.phone2 || p?.phone2 || null,
      email: r.email || p?.email || null,
      website: r.website || p?.website || null,
      type: r.type || "",
      toeristischeRegio: r.regio || p?.toeristischeRegio || "",
      dateOnline: r.date_online || p?.dateOnline || "",
    };
  });

  fullAiVakantiewoningRunning = true;
  res.json({ started: true, total: properties.length, message: "Full AI enrichment started for all vakantiewoning. Check server logs for progress." });

  const FULL_AI_PARALLEL = Math.min(parseInt(process.env.FULL_AI_PARALLEL || "2"), 4);
  const FULL_AI_DELAY_MS = parseInt(process.env.FULL_AI_DELAY_MS || "3000", 10);
  const insert = db.prepare("INSERT OR REPLACE INTO enrichment (id, data, enriched_at) VALUES (?,?,?)");

  (async () => {
    const phoneGroups = new Map();
    for (const prop of properties) {
      const key = normalizePhoneForGroup(prop.phone || prop.phone2);
      if (!key) continue;
      if (!phoneGroups.has(key)) phoneGroups.set(key, []);
      phoneGroups.get(key).push(prop.id);
    }
    const portfolioByKey = new Map();
    for (const [key, ids] of phoneGroups) {
      if (ids.length > 1) portfolioByKey.set(key, { count: ids.length, names: ids.map((id) => properties.find((p) => p.id === id)?.name || id) });
    }

    for (let i = 0; i < properties.length; i += FULL_AI_PARALLEL) {
      const chunk = properties.slice(i, i + FULL_AI_PARALLEL);
      const results = await Promise.all(
        chunk.map((prop) => {
          const key = normalizePhoneForGroup(prop.phone || prop.phone2);
          const portfolioInfo = portfolioByKey.get(key) || null;
          return runOneFullEnrichment(prop, portfolioInfo).then((data) => ({ id: prop.id, data }));
        })
      );
      const now = Date.now();
      for (const { id, data } of results) {
        insert.run(id, JSON.stringify(data), now);
        console.log("[full-ai-vakantiewoning] Enriched", id, "(", i + results.length, "/", properties.length, ")");
      }
      if (i + FULL_AI_PARALLEL < properties.length) await new Promise((r) => setTimeout(r, FULL_AI_DELAY_MS));
    }
    fullAiVakantiewoningRunning = false;
    console.log("[full-ai-vakantiewoning] Done. Total enriched:", properties.length);
  })().catch((e) => {
    fullAiVakantiewoningRunning = false;
    console.error("[full-ai-vakantiewoning] Job failed:", e.message);
  });
});

// =============================================================================
// PLATFORM SCAN — light AI across list: website + Airbnb + Booking only (no button)
// =============================================================================
app.get("/api/platform-scan", requireAuth, (req, res) => {
  const rows = db.prepare("SELECT id, data, scanned_at FROM platform_scan").all();
  const result = {};
  for (const row of rows) {
    try {
      result[row.id] = { ...JSON.parse(row.data), _scannedAt: row.scanned_at };
    } catch (_) { result[row.id] = { _scannedAt: row.scanned_at }; }
  }
  res.json(result);
});

app.post("/api/platform-scan/:id", requireAuth, (req, res) => {
  const data = req.body;
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Invalid data" });
  db.prepare("INSERT OR REPLACE INTO platform_scan (id, data, scanned_at) VALUES (?,?,?)").run(
    req.params.id, JSON.stringify(data), Date.now()
  );
  res.json({ ok: true });
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

// POST /api/meet/summarize — AI notetaker: extract outcome, note, contactNaam from Google Meet transcript
app.post("/api/meet/summarize", requireAuth, async (req, res) => {
  if (!ANTHROPIC_KEY) return res.status(500).json({ error: "ANTHROPIC_KEY not configured" });
  const transcript = req.body.transcript;
  if (!transcript || typeof transcript !== "string" || transcript.trim().length < 20) {
    return res.status(400).json({ error: "Transcript ontbreekt of te kort. Plak de tekst van je Google Meet-opname." });
  }
  const prompt = `Je bent een CRM-assistent voor yourdomi.be (kortetermijnverhuur België). Analyseer dit gesprek/meeting-transcript en haal er gestructureerde notities uit.

TRANSCRIPT (Google Meet / gesprek met klant):
---
${transcript.slice(0, 25000)}
---

Geef ALLEEN een JSON-object terug, geen uitleg:
{
  "outcome": "<exact één van: gebeld_interesse | callback | terugbellen | afgewezen | null>",
  "note": "<korte samenvatting van het gesprek, actiepunten, afspraken, wat de klant zei - in het Nederlands, 2-8 zinnen>",
  "contactNaam": "<naam van de contactpersoon / eigenaar als die genoemd wordt, anders null>"
}

Regels:
- outcome: gebeld_interesse = interesse getoond, afspraak/meeting gepland of wil meer info; callback/terugbellen = moet terugbellen; afgewezen = geen interesse of stopt; null als onduidelijk.
- note: kern van het gesprek, afspraken, vervolgstappen. Gebruik dezelfde taal als het transcript.
- contactNaam: alleen de naam (bijv. "Jan Janssen"), geen titel. null als niet genoemd.`;
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1024,
        messages: [{ role: "user", content: prompt }],
      }),
      signal: AbortSignal.timeout(30000),
    });
    const data = await r.json();
    const text = data.content?.[0]?.text?.trim() || "";
    const clean = text.replace(/```json|```/g, "").trim();
    let parsed;
    try { parsed = JSON.parse(clean); } catch (_) { return res.status(502).json({ error: "AI gaf geen geldige JSON terug" }); }
    res.json({
      outcome: parsed.outcome === "gebeld_interesse" || parsed.outcome === "callback" || parsed.outcome === "terugbellen" || parsed.outcome === "afgewezen" ? parsed.outcome : null,
      note: typeof parsed.note === "string" ? parsed.note.trim() : "",
      contactNaam: typeof parsed.contactNaam === "string" ? parsed.contactNaam.trim() || null : null,
    });
  } catch (e) {
    if (e.name === "AbortError") return res.status(504).json({ error: "Timeout" });
    res.status(500).json({ error: e.message || "AI-samenvatting mislukt" });
  }
});

// Background: four separate agents (Airbnb, Booking, website, pictures), each batched, running in parallel for quicker results.
const PLATFORM_SCAN_DAYS = parseInt(process.env.PLATFORM_SCAN_DAYS || "7");
const PLATFORM_SCAN_BATCH = parseInt(process.env.PLATFORM_SCAN_BATCH || "12");
const PLATFORM_SCAN_PARALLEL = Math.min(parseInt(process.env.PLATFORM_SCAN_PARALLEL || "4"), 8);

function getPlatformScanRow(db, id) {
  const row = db.prepare("SELECT data, scanned_at FROM platform_scan WHERE id = ?").get(id);
  let data = {};
  if (row && row.data) try { data = JSON.parse(row.data); } catch (_) {}
  return data;
}
function mergePlatformScan(db, id, partial) {
  const data = getPlatformScanRow(db, id);
  const merged = { ...data, ...partial };
  const now = Date.now();
  db.prepare("INSERT OR REPLACE INTO platform_scan (id, data, scanned_at) VALUES (?,?,?)").run(id, JSON.stringify(merged), now);
}

// Agent-safe merge: read latest row once, update only the agent field + _meta, write back.
// This avoids using a stale snapshot for merging and prevents overwriting fields written by other agents.
function mergePlatformScanAgentUpdate(db, id, partial, kind, ok) {
  const current = getPlatformScanRow(db, id);
  const updated = updateMetaFor(current, kind, ok);
  const merged = { ...current, ...partial, _meta: updated._meta };
  const now = Date.now();
  db.prepare("INSERT OR REPLACE INTO platform_scan (id, data, scanned_at) VALUES (?,?,?)").run(id, JSON.stringify(merged), now);
}

// Per-field staleness/backoff so agents can work efficiently in short wake windows.
const AGENT_STALE_DAYS_AIRBNB  = parseInt(process.env.AGENT_STALE_DAYS_AIRBNB  || "14", 10);
const AGENT_STALE_DAYS_BOOKING = parseInt(process.env.AGENT_STALE_DAYS_BOOKING || "14", 10);
const AGENT_STALE_DAYS_WEBSITE = parseInt(process.env.AGENT_STALE_DAYS_WEBSITE || "30", 10);
const AGENT_STALE_DAYS_IMAGES  = parseInt(process.env.AGENT_STALE_DAYS_IMAGES  || "30", 10);

function backoffDays(baseDays, failCount) {
  const f = Math.max(0, parseInt(failCount || 0, 10));
  if (f <= 1) return baseDays;
  if (f === 2) return Math.min(90, baseDays * 2);
  return Math.min(90, baseDays * 4);
}

function needsRescan(checkedAt, baseDays, failCount) {
  if (!checkedAt) return true;
  const age = Date.now() - Number(checkedAt);
  const days = backoffDays(baseDays, failCount) * 86400000;
  return age > days;
}

function scanMeta(data) {
  const m = (data && data._meta && typeof data._meta === "object") ? data._meta : {};
  return {
    airbnbCheckedAt: m.airbnbCheckedAt || null,
    bookingCheckedAt: m.bookingCheckedAt || null,
    websiteCheckedAt: m.websiteCheckedAt || null,
    imagesCheckedAt: m.imagesCheckedAt || null,
    airbnbFailCount: m.airbnbFailCount || 0,
    bookingFailCount: m.bookingFailCount || 0,
    websiteFailCount: m.websiteFailCount || 0,
    imagesFailCount: m.imagesFailCount || 0,
  };
}

function updateMetaFor(data, kind, ok) {
  const now = Date.now();
  const m = scanMeta(data);
  const meta = { ...(data && data._meta && typeof data._meta === "object" ? data._meta : {}) };
  if (kind === "airbnb") {
    meta.airbnbCheckedAt = now;
    meta.airbnbFailCount = ok ? 0 : (m.airbnbFailCount + 1);
  } else if (kind === "booking") {
    meta.bookingCheckedAt = now;
    meta.bookingFailCount = ok ? 0 : (m.bookingFailCount + 1);
  } else if (kind === "website") {
    meta.websiteCheckedAt = now;
    meta.websiteFailCount = ok ? 0 : (m.websiteFailCount + 1);
  } else if (kind === "images") {
    meta.imagesCheckedAt = now;
    meta.imagesFailCount = ok ? 0 : (m.imagesFailCount + 1);
  }
  return { ...(data || {}), _meta: meta };
}

function loadPlatformScanMap() {
  const rows = db.prepare("SELECT id, data FROM platform_scan").all();
  const map = new Map();
  for (const r of rows) {
    try { map.set(r.id, JSON.parse(r.data)); } catch { map.set(r.id, {}); }
  }
  return map;
}

async function runOneAirbnbScan(id, name, municipality) {
  const prompt = `Vakantieverhuur België: Naam: ${name || "onbekend"}, Gemeente: ${municipality || "onbekend"}. Zoek met web_search: "${(name || "").slice(0, 40)} ${(municipality || "").slice(0, 30)} Airbnb". Geef ALLEEN JSON: { "airbnb": { "gevonden": true|false, "url": "https://www.airbnb.com/rooms/..." of null } }`;
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 256, tools: [{ type: "web_search_20250305", name: "web_search" }], messages: [{ role: "user", content: prompt }] }),
      signal: AbortSignal.timeout(40000),
    });
    const data = await r.json();
    const text = (data.content || []).find(c => c.type === "text")?.text || "{}";
    const clean = text.replace(/```json|```/g, "").trim();
    const s = clean.indexOf("{"), e = clean.lastIndexOf("}");
    const parsed = JSON.parse(clean.slice(s, e + 1));
    const airbnb = parsed.airbnb && typeof parsed.airbnb === "object" ? { gevonden: !!parsed.airbnb.gevonden, url: parsed.airbnb.url || null } : { gevonden: false, url: null };
    return { airbnb };
  } catch (e) {
    console.warn("[agent-airbnb] Error", id, e.message);
    return { airbnb: { gevonden: false, url: null } };
  }
}

async function runOneBookingScan(id, name, municipality) {
  const prompt = `Vakantieverhuur België: Naam: ${name || "onbekend"}, Gemeente: ${municipality || "onbekend"}. Zoek met web_search: "${(name || "").slice(0, 40)} ${(municipality || "").slice(0, 30)} Booking.com". Geef ALLEEN JSON: { "booking": { "gevonden": true|false, "url": "https://www.booking.com/hotel/..." of null } }`;
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 256, tools: [{ type: "web_search_20250305", name: "web_search" }], messages: [{ role: "user", content: prompt }] }),
      signal: AbortSignal.timeout(40000),
    });
    const data = await r.json();
    const text = (data.content || []).find(c => c.type === "text")?.text || "{}";
    const clean = text.replace(/```json|```/g, "").trim();
    const s = clean.indexOf("{"), e = clean.lastIndexOf("}");
    const parsed = JSON.parse(clean.slice(s, e + 1));
    const booking = parsed.booking && typeof parsed.booking === "object" ? { gevonden: !!parsed.booking.gevonden, url: parsed.booking.url || null } : { gevonden: false, url: null };
    return { booking };
  } catch (e) {
    console.warn("[agent-booking] Error", id, e.message);
    return { booking: { gevonden: false, url: null } };
  }
}

async function runOneWebsiteScan(id, name, municipality, website) {
  const prompt = `Vakantieverhuur België: Naam: ${name || "onbekend"}, Gemeente: ${municipality || "onbekend"}, Website: ${website || "geen"}. Zoek eventueel met web_search: "${(name || "").slice(0, 40)} ${(municipality || "").slice(0, 30)} vakantiewoning". Als er een directe website is (geen Airbnb/Booking), geef die. Geef ALLEEN JSON: { "website": { "gevonden": true|false, "url": "https://..." of null } }`;
  try {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 256, tools: [{ type: "web_search_20250305", name: "web_search" }], messages: [{ role: "user", content: prompt }] }),
      signal: AbortSignal.timeout(40000),
    });
    const data = await r.json();
    const text = (data.content || []).find(c => c.type === "text")?.text || "{}";
    const clean = text.replace(/```json|```/g, "").trim();
    const s = clean.indexOf("{"), e = clean.lastIndexOf("}");
    const parsed = JSON.parse(clean.slice(s, e + 1));
    const websiteOut = parsed.website && typeof parsed.website === "object" ? { gevonden: !!parsed.website.gevonden, url: parsed.website.url || null } : { gevonden: false, url: null };
    return { website: websiteOut };
  } catch (e) {
    console.warn("[agent-website] Error", id, e.message);
    return { website: { gevonden: false, url: null } };
  }
}

function agentTier(row, kind) {
  const w = (s) => (s || "").toLowerCase();
  const web = w(row.website || "");
  const name = w(row.name || "");
  if (kind === "airbnb") return web.includes("airbnb.com") || name.includes("airbnb");
  if (kind === "booking") return web.includes("booking.com") || name.includes("booking");
  if (kind === "website") return true; // all rest for website agent
  return false;
}

async function runAirbnbAgentBatch() {
  if (!ANTHROPIC_KEY || !db) return;
  const scanMap = loadPlatformScanMap();
  const all = db.prepare("SELECT id, name, municipality, website FROM properties").all();
  let toScan = all.filter(r => {
    if (!agentTier(r, "airbnb")) return false;
    const data = scanMap.get(r.id) || {};
    const meta = scanMeta(data);
    const hasUrl = !!(data.airbnb && data.airbnb.url);
    return !hasUrl && needsRescan(meta.airbnbCheckedAt, AGENT_STALE_DAYS_AIRBNB, meta.airbnbFailCount);
  });
  toScan = toScan.slice(0, PLATFORM_SCAN_BATCH);
  if (toScan.length === 0) return;
  for (let i = 0; i < toScan.length; i += PLATFORM_SCAN_PARALLEL) {
    const chunk = toScan.slice(i, i + PLATFORM_SCAN_PARALLEL);
    const results = await Promise.all(chunk.map(row => runOneAirbnbScan(row.id, row.name, row.municipality).then(data => ({ id: row.id, data }))));
    for (const { id, data } of results) {
      const ok = !!(data?.airbnb?.gevonden && data?.airbnb?.url);
      mergePlatformScanAgentUpdate(db, id, { airbnb: data.airbnb }, "airbnb", ok);
    }
    if (i + PLATFORM_SCAN_PARALLEL < toScan.length) await new Promise(r => setTimeout(r, 600));
  }
  console.log("[agent-airbnb] Scanned", toScan.length, "listings.");
}

async function runBookingAgentBatch() {
  if (!ANTHROPIC_KEY || !db) return;
  const scanMap = loadPlatformScanMap();
  const all = db.prepare("SELECT id, name, municipality, website FROM properties").all();
  let toScan = all.filter(r => {
    if (!agentTier(r, "booking")) return false;
    const data = scanMap.get(r.id) || {};
    const meta = scanMeta(data);
    const hasUrl = !!(data.booking && data.booking.url);
    return !hasUrl && needsRescan(meta.bookingCheckedAt, AGENT_STALE_DAYS_BOOKING, meta.bookingFailCount);
  });
  toScan = toScan.slice(0, PLATFORM_SCAN_BATCH);
  if (toScan.length === 0) return;
  for (let i = 0; i < toScan.length; i += PLATFORM_SCAN_PARALLEL) {
    const chunk = toScan.slice(i, i + PLATFORM_SCAN_PARALLEL);
    const results = await Promise.all(chunk.map(row => runOneBookingScan(row.id, row.name, row.municipality).then(data => ({ id: row.id, data }))));
    for (const { id, data } of results) {
      const ok = !!(data?.booking?.gevonden && data?.booking?.url);
      mergePlatformScanAgentUpdate(db, id, { booking: data.booking }, "booking", ok);
    }
    if (i + PLATFORM_SCAN_PARALLEL < toScan.length) await new Promise(r => setTimeout(r, 600));
  }
  console.log("[agent-booking] Scanned", toScan.length, "listings.");
}

async function runWebsiteAgentBatch() {
  if (!ANTHROPIC_KEY || !db) return;
  const scanMap = loadPlatformScanMap();
  const all = db.prepare("SELECT id, name, municipality, website FROM properties").all();
  let toScan = all.filter(r => {
    if (agentTier(r, "airbnb") || agentTier(r, "booking")) return false;
    const data = scanMap.get(r.id) || {};
    const meta = scanMeta(data);
    const hasUrl = !!(data.website && data.website.url);
    return !hasUrl && needsRescan(meta.websiteCheckedAt, AGENT_STALE_DAYS_WEBSITE, meta.websiteFailCount);
  });
  toScan = toScan.slice(0, PLATFORM_SCAN_BATCH);
  if (toScan.length === 0) return;
  for (let i = 0; i < toScan.length; i += PLATFORM_SCAN_PARALLEL) {
    const chunk = toScan.slice(i, i + PLATFORM_SCAN_PARALLEL);
    const results = await Promise.all(chunk.map(row => runOneWebsiteScan(row.id, row.name, row.municipality, row.website).then(data => ({ id: row.id, data }))));
    for (const { id, data } of results) {
      const ok = !!(data?.website?.gevonden && data?.website?.url);
      mergePlatformScanAgentUpdate(db, id, { website: data.website }, "website", ok);
    }
    if (i + PLATFORM_SCAN_PARALLEL < toScan.length) await new Promise(r => setTimeout(r, 600));
  }
  console.log("[agent-website] Scanned", toScan.length, "listings.");
}

// Agent 4: Pictures — fetch listing pages, extract photo URLs, store in platform_scan for cards and dossier
const IMAGE_SCRAPE_BATCH = Math.min(parseInt(process.env.IMAGE_SCRAPE_BATCH || "6"), 12);
const IMAGE_SCRAPE_PARALLEL = Math.min(parseInt(process.env.IMAGE_SCRAPE_PARALLEL || "3"), 6);
const PHOTO_DOMAINS = ["a0.muscache.com", "muscache.com", "cf.bstatic.com", "bstatic.com", "dynamicmedia", "images.unsplash.com", "imgur.com", "cloudinary.com"];

function extractImageUrlsFromHtml(html, baseUrl) {
  const urls = new Set();
  const re = /<img[^>]+src=["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    let u = m[1].trim();
    if (u.startsWith("//")) u = "https:" + u;
    if (u.startsWith("/")) {
      try { const b = new URL(baseUrl); u = b.origin + u; } catch (_) { continue; }
    }
    if (!u.startsWith("http")) continue;
    if (/\.(jpg|jpeg|png|webp|gif)(\?|$)/i.test(u) || PHOTO_DOMAINS.some(d => u.includes(d))) urls.add(u);
  }
  const re2 = /["'](https?:\/\/[^"']+\.(?:jpg|jpeg|png|webp|gif)(?:\?[^"']*)?)["']/gi;
  while ((m = re2.exec(html)) !== null) urls.add(m[1].trim());
  return [...urls];
}

async function fetchImagesForUrls(urls) {
  const all = [];
  const ua = "Mozilla/5.0 (compatible; YourDomiBot/1.0)";
  const timeoutMs = Math.min(parseInt(process.env.IMAGE_FETCH_TIMEOUT_MS || "10000", 10), 20000);
  for (const url of urls.slice(0, 3)) {
    if (!url || !url.startsWith("http")) continue;
    try {
      const r = await fetch(url, { headers: { "User-Agent": ua }, signal: AbortSignal.timeout(timeoutMs) });
      const html = await r.text();
      all.push(...extractImageUrlsFromHtml(html, url));
    } catch (_) {}
  }
  const seen = new Set();
  return all.filter(u => { if (seen.has(u)) return false; seen.add(u); return true; }).slice(0, 24);
}

function imageScrapeTier(entry) {
  const d = entry.data || {};
  if (d.airbnb?.url) return 0;
  if (d.booking?.url) return 1;
  if (d.website?.url) return 2;
  return 3;
}

async function runImageScrapeBatch() {
  if (!db) return;
  const rows = db.prepare("SELECT id, data FROM platform_scan").all();
  const props = new Map(db.prepare("SELECT id, website FROM properties").all().map(r => [r.id, r.website]));
  const toScrape = [];
  for (const row of rows) {
    let data;
    try { data = JSON.parse(row.data); } catch { continue; }
    const meta = scanMeta(data);
    const hasFotos = Array.isArray(data.fotoUrls) && data.fotoUrls.length > 0;
    const stale = needsRescan(meta.imagesCheckedAt, AGENT_STALE_DAYS_IMAGES, meta.imagesFailCount);
    // Refresh when stale, even if fotos already exist. Skip only when not stale.
    if (!stale) continue;
    const urls = [];
    if (data.airbnb?.url) urls.push(data.airbnb.url);
    if (data.booking?.url) urls.push(data.booking.url);
    if (data.website?.url) urls.push(data.website.url);
    const pw = (props.get(row.id) || "").trim();
    if (pw && pw.startsWith("http") && !urls.includes(pw)) {
      if (pw.toLowerCase().includes("airbnb.com") || pw.toLowerCase().includes("booking.com")) urls.push(pw);
      else if (!data.website?.url) urls.push(pw);
    }
    if (urls.length === 0) continue;
    toScrape.push({ id: row.id, urls, data: { airbnb: data.airbnb, booking: data.booking, website: data.website } });
  }
  if (toScrape.length === 0) return;
  toScrape.sort((a, b) => imageScrapeTier(a) - imageScrapeTier(b));
  const batch = toScrape.slice(0, IMAGE_SCRAPE_BATCH);
  const update = db.prepare("UPDATE platform_scan SET data = ?, scanned_at = ? WHERE id = ?");
  for (let i = 0; i < batch.length; i += IMAGE_SCRAPE_PARALLEL) {
    const chunk = batch.slice(i, i + IMAGE_SCRAPE_PARALLEL);
    const results = await Promise.all(chunk.map(async ({ id, urls }) => {
      const fotoUrls = await fetchImagesForUrls(urls);
      const row = db.prepare("SELECT data FROM platform_scan WHERE id = ?").get(id);
      let data = {};
      try { data = row ? JSON.parse(row.data) : {}; } catch (_) {}
      data.fotoUrls = fotoUrls;
      const ok = Array.isArray(fotoUrls) && fotoUrls.length > 0;
      data = updateMetaFor(data, "images", ok);
      return { id, data };
    }));
    for (const { id, data } of results) {
      update.run(JSON.stringify(data), Date.now(), id);
    }
    if (i + IMAGE_SCRAPE_PARALLEL < batch.length) await new Promise(r => setTimeout(r, 600));
  }
  console.log("[image-scrape] Fetched images for", batch.length, "properties.");
}

app.post("/api/monday", requireAuth, async (req, res) => {
  const apiKey = req.body.apiKey || process.env.MONDAY_API_KEY;
  const { query, variables } = req.body;
  if (!apiKey) return res.status(400).json({ error: "Monday API-token ontbreekt. Vul het token in en klik op Verbinden." });
  if (!query) return res.status(400).json({ error: "query required" });
  try {
    const r = await fetch("https://api.monday.com/v2", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: String(apiKey).trim(), "API-Version": "2024-01" },
      body: JSON.stringify({ query, variables: variables || {} }),
      signal: AbortSignal.timeout(15000),
    });
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch { return res.status(502).json({ error: "Monday API gaf geen geldige JSON terug" }); }
    res.json(data);
  } catch (e) {
    if (e.name === "AbortError") return res.status(504).json({ error: "Timeout: Monday API reageerde niet op tijd." });
    res.status(502).json({ error: e.message || "Kon Monday API niet bereiken." });
  }
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
    ["slaapplaatsen","INTEGER"],["phone","TEXT"],["phone2","TEXT"],["email","TEXT"],["website","TEXT"],
    ["type","TEXT"],["regio","TEXT"],["date_online","TEXT"],["postal_code","TEXT"],["street","TEXT"],
  ]) {
    if (!cols.includes(col)) {
      db.exec(`ALTER TABLE properties ADD COLUMN ${col} ${type}`);
      console.log(`[migration] Added column: ${col}`);
    }
  }

  // Indexes for filter/sort (Power BI–style) — created in schema too
  [
    "name", "municipality", "province", "status", "type", "regio",
    "slaapplaatsen", "date_online", "postal_code", "street", "phone", "fetched_at",
  ].forEach((col) => db.exec(`CREATE INDEX IF NOT EXISTS idx_properties_${col} ON properties(${col})`));

  ensureDefaultUsers();

  app.listen(PORT, () => {
    console.log(`YourDomi server running on port ${PORT}`);
    console.log(`DB: ${DB_PATH}`);

    const count = db.prepare("SELECT COUNT(*) as c FROM properties").get().c;
    const vfCount = db.prepare("SELECT COUNT(*) as c FROM properties WHERE id LIKE 'vf_%'").get().c;
    const useTvPrimary = process.env.USE_TV_AS_PRIMARY === "1";

    if (count === 0) {
      if (useTvPrimary) {
        console.log("[startup] Empty DB + USE_TV_AS_PRIMARY=1 — starting TV sync in background...");
        syncPropertiesFromTV().catch(console.error);
      } else {
        console.log("[startup] Empty DB — starting initial VF sync...");
        syncFromVF().catch(console.error);
      }
    } else if (vfCount === 0 && !useTvPrimary) {
      console.log("[startup] Old UUID data detected — starting fresh VF sync...");
      db.exec("DELETE FROM properties");
      syncFromVF().catch(console.error);
    } else {
      console.log(`[startup] ${count} properties in DB (${vfCount} VF records).`);
    }
    if (process.env.AUTO_SYNC_TV_ON_STARTUP === "1" && count > 0) {
      console.log("[startup] AUTO_SYNC_TV_ON_STARTUP=1 — starting TV sync in background...");
      syncPropertiesFromTV().catch(console.error);
    }
    if (process.env.PLATFORM_SCAN_ENABLED === "1" && ANTHROPIC_KEY) {
      setTimeout(() => {
        const runAllAgents = () => runAllBackgroundAgentsOnce().catch(console.error);
        // Burst on wake/startup so we make progress even if the container idles again.
        runBackgroundBurst().catch(console.error);
        const interval = setInterval(runAllAgents, 2 * 60 * 1000);
        interval.unref?.();
        console.log("[startup] Four background agents enabled (Airbnb, Booking, website, pictures) — running in parallel every 2 min.");
      }, 15000);
    }
  });
}

// Optional cron trigger (still requires the container to be awake).
// Example: BACKGROUND_AGENTS_CRON="*/2 * * * *"
if (process.env.BACKGROUND_AGENTS_CRON && process.env.PLATFORM_SCAN_ENABLED === "1") {
  try {
    cron.schedule(process.env.BACKGROUND_AGENTS_CRON, () => {
      runAllBackgroundAgentsOnce().catch(console.error);
    });
    console.log("[startup] BACKGROUND_AGENTS_CRON enabled:", process.env.BACKGROUND_AGENTS_CRON);
  } catch (e) {
    console.warn("[startup] Invalid BACKGROUND_AGENTS_CRON:", e.message);
  }
}

startServer().catch(console.error);
