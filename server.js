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
    "INSERT OR REPLACE INTO properties (id, data, fetched_at, name, municipality, province, status, slaapplaatsen, phone, email, website, type, regio, date_online, postal_code, street) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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

function parseLodging(raw, included = []) {
  const attr = raw.attributes || {};
  const rel = raw.relationships || {};

  let name = attr["name"] || attr["schema:name"] || `Pand ${String(raw.id || "").slice(-6)}`;

  const uri = attr["uri"] || attr["@id"] || "";
  let municipality = "",
    province = "",
    postalCode = "",
    toeristischeRegio = "";

  const luEntry = lookupMunicipality(name, uri);
  if (luEntry) {
    if (!name || name.startsWith("Pand ")) name = luEntry[0] || name;
    municipality = luEntry[1] || "";
    province = luEntry[2] || "";
    postalCode = luEntry[3] || "";
    toeristischeRegio = luEntry[4] || "";
  }

  if (!municipality && included.length) {
    const muniRef = rel.municipality?.data || rel.address?.data;
    if (muniRef) {
      const muni = included.find((i) => i.type === muniRef.type && i.id === muniRef.id);
      if (muni) {
        municipality = muni?.attributes?.name || muni?.attributes?.["schema:name"] || "";
        const provRef = muni?.relationships?.province?.data;
        if (provRef) {
          const prov = included.find((i) => i.type === provRef.type && i.id === provRef.id);
          province = prov?.attributes?.name || "";
        }
      }
    }
  }

  // Address from main resource or from related address in included (TV uses locn/thoroughfare, schema:streetAddress)
  let street = s(attr["street"] || attr["schema:address"]?.["schema:streetAddress"] || attr["thoroughfare"] || attr["locn:thoroughfare"] || "");
  const addrRef = rel.address?.data != null ? (Array.isArray(rel.address.data) ? rel.address.data[0] : rel.address.data) : null;
  if (!street && addrRef && included.length) {
    const addr = included.find((i) => i.type === addrRef.type && i.id === addrRef.id);
    if (addr) {
      const aa = addr.attributes || {};
      street = s(aa["street"] || aa["schema:streetAddress"] || aa["thoroughfare"] || aa["locn:thoroughfare"] || "");
      if (!postalCode) postalCode = s(aa["postalCode"] || aa["postcode"] || aa["postCode"] || aa["locn:postCode"] || "");
      if (!municipality) municipality = s(aa["addressLocality"] || aa["schema:addressLocality"] || aa["adminUnitL2"] || "");
    }
  }
  if (!postalCode) postalCode = s(attr["postalCode"] || attr["postcode"] || attr["postal-code"] || attr["locn:postCode"] || "");

  const contactPoints = rel["contact-points"]?.data || [];
  const phones = [],
    emails = [],
    websites = [];
  for (const cp of contactPoints) {
    const c = included.find((i) => i.type === cp.type && i.id === cp.id);
    if (!c) continue;
    const ca = c.attributes || {};
    if (ca["schema:telephone"]) phones.push(s(ca["schema:telephone"]));
    if (ca["schema:email"]) emails.push(s(ca["schema:email"]));
    if (ca["schema:url"]) websites.push(s(ca["schema:url"]));
  }
  if (!phones.length && attr["schema:telephone"]) phones.push(s(attr["schema:telephone"]));
  if (!emails.length && attr["schema:email"]) emails.push(s(attr["schema:email"]));
  if (!websites.length && attr["schema:url"]) websites.push(s(attr["schema:url"]));

  const cleanPhone = (p) =>
    s(p).replace(/\s/g, "").replace(/^00/, "+").replace(/^\+?0032/, "+32");
  const phone = phones[0] ? cleanPhone(phones[0]) : null;
  const phoneNorm = phone ? phone.replace(/[^0-9+]/g, "") : null;

  const mainMedia = rel["main-media"]?.data ? [rel["main-media"].data] : [];
  const allMediaRefs = ([]).concat(rel.media?.data || [], mainMedia).filter(Boolean);
  const images = allMediaRefs
    .map((m) => included.find((i) => i.type === m.type && i.id === m.id)?.attributes?.contentUrl)
    .filter(Boolean)
    .slice(0, 5);

  const slaapplaatsen = parseInt(
    attr["number-of-sleeping-places"] ||
      attr["number-of-sleep-places"] ||
      attr["schema:numberOfRooms"] ||
      0,
  );

  const status = attr["registration-status"] || attr.status || "";

  const type =
    attr["category"] ||
    attr["dcterms:type"] ||
    attr["schema:additionalType"] ||
    attr.type ||
    "";

  const dateOnline = attr["modified"] || attr["registration-date"] || attr["dcterms:created"] || "";

  // Build a display name:
  // - Gebruik echte naam als die duidelijk is
  // - Als het een generieke "Pand 123456" is of leeg: "Vakantiewoning in [gemeente]"
  // - Als er geen gemeente is maar wel provincie: "Vakantiewoning in [provincie]"
  // - Als er helemaal niets is: "Naamloze woning"
  let displayName = s(name);
  if (!displayName || /^pand\s/i.test(displayName)) {
    if (municipality) {
      displayName = `Vakantiewoning in ${municipality}`;
    } else if (province) {
      displayName = `Vakantiewoning in ${province}`;
    } else {
      displayName = "Naamloze woning";
    }
  }

  return {
    id: s(raw.id),
    name: displayName,
    municipality: s(municipality),
    province: s(province),
    toeristischeRegio: s(toeristischeRegio),
    type: s(type),
    postalCode: s(postalCode),
    street: street,
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

// GET /api/panden — disable cache so client always gets fresh data (avoids 304 with stale/empty body)
app.get("/api/panden", requireAuth, (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
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
    const rows = db.prepare(
      `SELECT id, data, name, municipality, province, status, slaapplaatsen, phone, email, website, type, regio, date_online, postal_code, street FROM properties ${where} ${orderBy} LIMIT ? OFFSET ?`
    ).all(...params, size, offset);

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
        name: r.name || "Vakantiewoning",
        municipality: r.municipality || "",
        province: r.province || "",
        street: r.street || "",
        postalCode: r.postal_code || "",
        status: r.status || "",
        slaapplaatsen: r.slaapplaatsen || 0,
        sleepPlaces: r.slaapplaatsen || 0,
        units: 1,
        phone: r.phone || null,
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
      meta: { total: totalNum, dbTotal: Number(total) || 0, page, size, pages: pagesNum },
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
