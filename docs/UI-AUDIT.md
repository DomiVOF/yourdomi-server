# YourDomi Bellist — UI Audit

**Brand:** YourDomi — Belgian short-term rental management (proptech SaaS)  
**Goal:** Clean, modern, trustworthy; high-end proptech feel.  
**Scope:** Existing Bellist frontend (property list, filters, dossier, deal scoring).

---

## 1. What needs the most work (priority order)

### 🔴 High impact — do first

1. **Property cards (list view)**  
   - Cards are the main scanning surface; they set the “premium” tone.
   - **Issues:** Flat look, weak hierarchy (title vs meta vs contact), score badge feels generic, dense blocks of text, little breathing room.
   - **Fix:** Stronger typography hierarchy, subtle layered shadows, clearer score badge (HEET/WARM/KOUD or 0–100), more whitespace and clear sections.

2. **Deal / AI score treatment**  
   - Score is critical for prioritisation but can look like an afterthought.
   - **Issues:** Pill styling can feel noisy; HEET vs WARM vs KOUD not always visually distinct; no clear “premium” treatment.
   - **Fix:** Dedicated score component with clear colour semantics, subtle depth (e.g. soft shadow), optional numeric + label, consistent with brand (e.g. gold/green accents).

3. **Header and global chrome**  
   - First thing users see; sets trust and “pro” feel.
   - **Issues:** Stats (Heet, Portfolio, Interesse, AI-scanned) can look busy; bar can feel heavy or flat.
   - **Fix:** Calmer layout, clear typography scale, subtle separation from content (e.g. light border or shadow), optional compact secondary row for actions.

### 🟠 Medium impact

4. **Filter bar and filters panel**  
   - **Issues:** Many controls in one row; filter panel can feel cramped; “Filters” dropdown vs inline filters can be unclear.
   - **Fix:** Group filters (e.g. search | view/sort | actions), clearer labels, more spacing; consider a slide-out or anchored panel for many options.

5. **Table view (list vs cards)**  
   - **Issues:** Table can feel utilitarian; HEET row highlight (e.g. gold) helps but overall table styling is basic.
   - **Fix:** Consistent row hover, clearer column hierarchy, optional subtle zebra or borders, align typography with card view.

6. **Dossier (property detail) page**  
   - **Issues:** Long page with many sections; hero removed but structure can still feel dense; “Verkoopintelligentie” and other blocks need clear hierarchy.
   - **Fix:** Clear section headings, consistent spacing, optional sticky TOC or anchor nav; same design tokens as list (shadows, type scale, colours).

### 🟡 Lower priority (polish)

7. **Buttons and CTAs**  
   - “Start AI”, “Zoeken”, outcome actions (Terugbellen, Afwijzen, etc.): ensure they use the same radius, weight, and hover states as the rest of the UI.

8. **Empty and loading states**  
   - Skeleton loaders and “geen resultaten” should use the same spacing and typography as real content; avoid generic spinners.

9. **Modals and small overlays**  
   - Caller feedback, filters panel, config: same shadow/depth and border-radius as cards for consistency.

---

## 2. Technical / consistency notes

- **Design tokens:** Centralise colours (e.g. gold `#C89B3C`, green, neutrals), shadows, radius, and type scale so cards, header, and dossier all match.
- **Tailwind:** If moving to Tailwind, define these in `tailwind.config` (e.g. `yd-gold`, `yd-score-heet`) and use `cn()` for conditional classes.
- **Typography:** One clear sans for UI (e.g. system-ui or Inter); optional serif (e.g. Georgia) for headings only. Hierarchy: one dominant title size, one body, one muted/caption size.
- **Depth:** Prefer 2–3 shadow layers for cards (soft + medium) and one stronger layer on hover; avoid single harsh shadows.

---

## 3. Summary

**Most work:** Property cards and deal score treatment — they define “premium” and are used most. Then header/chrome, then filters and table, then dossier and smaller UI pieces.  
**Keep:** Existing data and behaviour; only visual treatment and hierarchy need to change to feel more professional and premium.
