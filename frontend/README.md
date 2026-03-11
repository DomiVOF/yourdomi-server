# Frontend components (YourDomi Bellist)

This folder contains **drop-in React components** for the YourDomi Bellist app. Copy `src/` into your existing React + Vite + Tailwind app.

## PropertyCard

- **Path:** `src/components/PropertyCard.tsx`
- **Dependency:** `src/lib/utils.ts` (minimal `cn()` — optional: use `clsx` + `tailwind-merge` in your app).

### Usage

```tsx
import { PropertyCard } from "./components/PropertyCard";

<PropertyCard
  property={property}        // from /api/panden
  enrichment={enriched[id]}  // from /api/enrichment
  outcome={outcomes[id]}     // from /api/outcomes
  portfolioCount={phoneGroups[property.phoneNorm]?.length}
  isEnriching={enrichingIds.has(property.id)}
  isDimmed={hidden.includes(property.id) || outcome === "afgewezen"}
  onClick={() => { setSelected(property); setView("dossier"); }}
/>
```

### Design

- **Typography:** Clear hierarchy (title 1.0625rem bold, meta xs, tags small).
- **Depth:** Layered box-shadows at rest and on hover; subtle lift and scale on hover.
- **Score badge:** HEET (amber), WARM (orange), KOUD (slate); pill with soft shadow.
- **Left border:** 4px accent by score (amber / orange / slate).

### Tailwind config (optional)

In your `tailwind.config.ts` you can add brand tokens:

```ts
theme: {
  extend: {
    colors: {
      "yd-gold": "#C89B3C",
      "yd-gold-pale": "#FFF7E0",
      "yd-green": "#2D5C4E",
    },
  },
}
```

Then use `border-yd-gold`, `bg-yd-gold-pale`, etc. in the component if you replace the current amber/slate palette.
