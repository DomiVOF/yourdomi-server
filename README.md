# YourDomi Server

Backend voor YourDomi Bellijst. Haalt panden op van Toerisme Vlaanderen, slaat AI-verrijking op, en bewaart uitkomsten per pand.

## Deploy op Railway (15 min)

### 1. Maak een GitHub repo
1. Ga naar github.com → New repository
2. Naam: `yourdomi-server`
3. Private ✓ → Create
4. Upload alle bestanden uit deze map (server.js, package.json, railway.toml, .gitignore)

### 2. Deploy op Railway
1. Ga naar railway.app → Login met GitHub
2. New Project → Deploy from GitHub repo → kies `yourdomi-server`
3. Railway detecteert automatisch Node.js en deployt

### 3. Voeg een Volume toe (persistent database)
1. In Railway: klik op je service → Settings → Volumes
2. Add Volume:
   - Mount path: `/data`
3. Sla op

### 4. Stel environment variables in
In Railway → je service → Variables, voeg toe:

| Variable | Waarde |
|---|---|
| `PORT` | `3001` |
| `DB_PATH` | `/data/yourdomi.db` |
| `ANTHROPIC_KEY` | jouw Anthropic API key |
| `FRONTEND_URL` | `https://yourdomi-bellist.vercel.app` |
| `AI_STALE_DAYS` | `100` |

### 5. Noteer je server URL
Railway geeft je een URL zoals: `https://yourdomi-server-production-xxxx.up.railway.app`

### 6. Update de frontend
Voeg toe in Vercel → yourdomi-bellist → Settings → Environment Variables:

| Variable | Waarde |
|---|---|
| `VITE_API_URL` | jouw Railway URL (zonder trailing slash) |

Daarna: `vercel --prod` opnieuw uitvoeren.

## API Endpoints

| Endpoint | Beschrijving |
|---|---|
| `GET /api/health` | Status + counts |
| `GET /api/panden?page=1&size=50` | Gepagineerde lijst |
| `GET /api/panden/count` | Totaal aantal |
| `POST /api/sync` | Forceer sync van TV |
| `GET /api/enrichment` | Alle AI-verrijkingen |
| `POST /api/enrichment/:id` | Sla AI-verrijking op |
| `GET /api/enrichment/stale` | IDs die herverrijking nodig hebben |
| `GET /api/outcomes` | Alle uitkomsten + notities |
| `POST /api/outcomes/:id` | Sla uitkomst op |

## Automatische taken

- **Elke zondag 03:00** — hersynct alle panden van Toerisme Vlaanderen
- **Bij startup** — als DB leeg is, start automatisch initiële sync (~30k panden, duurt ~5 min)
- **AI stale** — na 100 dagen wordt een pand gemarkeerd voor herverrijking
