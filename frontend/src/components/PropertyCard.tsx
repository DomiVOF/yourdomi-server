import React from "react";
import { cn } from "../lib/utils";

// -----------------------------------------------------------------------------
// Types — align with yourdomi-server API (properties + enrichment + outcomes)
// -----------------------------------------------------------------------------

export interface Property {
  id: string;
  name: string;
  street?: string | null;
  municipality?: string | null;
  postalCode?: string | null;
  province?: string | null;
  status?: string | null;
  slaapplaatsen?: number | null;
  sleepPlaces?: number | null;
  units?: number | null;
  phone?: string | null;
  phone2?: string | null;
  email?: string | null;
  website?: string | null;
  type?: string | null;
  onlineSince?: string | null;
  dateOnline?: string | null;
}

export interface Enrichment {
  score?: "HEET" | "WARM" | "KOUD" | string;
  waarschuwingAgentuur?: boolean;
  slechteReviews?: boolean;
  poorWebsite?: boolean;
  directWebsite?: { url?: string; werkt?: boolean; gevonden?: boolean };
}

export type Outcome = "terugbellen" | "callback" | "interesse" | "gebeld_interesse" | "afgewezen" | "none";

export interface PropertyCardProps {
  property: Property;
  enrichment?: Enrichment | null;
  outcome?: Outcome | null;
  /** e.g. number of properties with same phone (portfolio) */
  portfolioCount?: number;
  /** Show "AI bezig…" state */
  isEnriching?: boolean;
  /** Dimmed / hidden state */
  isDimmed?: boolean;
  onClick?: () => void;
  className?: string;
}

// -----------------------------------------------------------------------------
// Score styling — clean, premium badge
// -----------------------------------------------------------------------------

const SCORE_STYLES: Record<string, { label: string; bg: string; text: string; border: string; emoji?: string }> = {
  HEET: {
    label: "Heet",
    bg: "bg-amber-50",
    text: "text-amber-800",
    border: "border-amber-200",
    emoji: "🔥",
  },
  WARM: {
    label: "Warm",
    bg: "bg-orange-50",
    text: "text-orange-700",
    border: "border-orange-200",
    emoji: "☀️",
  },
  KOUD: {
    label: "Koud",
    bg: "bg-slate-100",
    text: "text-slate-600",
    border: "border-slate-200",
  },
};

function getScoreStyle(score: string | undefined) {
  if (!score) return null;
  const key = String(score).toUpperCase();
  return SCORE_STYLES[key] ?? {
    label: score,
    bg: "bg-slate-100",
    text: "text-slate-600",
    border: "border-slate-200",
  };
}

const OUTCOME_LABELS: Record<string, string> = {
  terugbellen: "Terugbellen",
  callback: "Terugbellen",
  interesse: "Interesse",
  gebeld_interesse: "Interesse",
  afgewezen: "Afgewezen",
  none: "",
};

// -----------------------------------------------------------------------------
// PropertyCard — premium visual treatment only (same data)
// -----------------------------------------------------------------------------

export function PropertyCard({
  property,
  enrichment,
  outcome,
  portfolioCount = 0,
  isEnriching = false,
  isDimmed = false,
  onClick,
  className,
}: PropertyCardProps) {
  const scoreStyle = getScoreStyle(enrichment?.score);
  const scoreKey = enrichment?.score ? String(enrichment.score).toUpperCase() : null;
  const addressParts = [property.street, property.postalCode, property.municipality].filter(Boolean);
  const address = addressParts.length ? addressParts.join(", ") : null;
  const sleep = property.slaapplaatsen ?? property.sleepPlaces ?? 0;
  const units = property.units ?? 1;
  const phones = [property.phone, property.phone2].filter(Boolean) as string[];
  const hasPortfolio = portfolioCount > 1;
  const outcomeLabel = outcome && outcome !== "none" ? OUTCOME_LABELS[outcome] ?? outcome : null;

  const leftBorderClass =
    scoreKey === "HEET"
      ? "border-l-4 border-l-amber-400"
      : scoreKey === "WARM"
        ? "border-l-4 border-l-orange-400"
        : scoreKey === "KOUD"
          ? "border-l-4 border-l-slate-300"
          : "border-l-4 border-l-slate-200";

  const formatDate = (d: string | null | undefined) => {
    if (!d) return "—";
    try {
      return new Date(d).toLocaleDateString("nl-BE", { day: "numeric", month: "short", year: "numeric" });
    } catch {
      return d;
    }
  };

  return (
    <article
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && onClick?.()}
      className={cn(
        "relative flex flex-col rounded-2xl border border-slate-200/90 bg-white text-left overflow-hidden",
        leftBorderClass,
        "shadow-[0_1px_3px_rgba(0,0,0,0.04),0_6px_16px_rgba(15,23,42,0.06),0_12px_32px_rgba(15,23,42,0.04)]",
        "transition-[transform,box-shadow,border-color] duration-250 ease-out",
        "hover:shadow-[0_4px_8px_rgba(0,0,0,0.04),0_12px_28px_rgba(15,23,42,0.1),0_24px_48px_rgba(15,23,42,0.08)]",
        "hover:-translate-y-1.5 hover:scale-[1.008] hover:border-amber-200/60",
        "active:-translate-y-0.5 active:scale-[1.004]",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-400/50 focus-visible:ring-offset-2",
        isDimmed && "opacity-50 pointer-events-none",
        className
      )}
    >
      <div className="flex flex-col flex-1 p-5 pb-5 gap-4">
        {/* Top row: title + score badge */}
        <div className="flex justify-between items-start gap-3">
          <div className="min-w-0 flex-1">
            <h3 className="text-[1.0625rem] font-bold text-slate-900 leading-tight tracking-tight line-clamp-2">
              {property.name}
            </h3>
            {address && (
              <p className="mt-1.5 text-xs text-slate-500 truncate" title={address}>
                {address}
              </p>
            )}
          </div>
          <div className="flex flex-col items-end gap-1.5 flex-shrink-0">
            {scoreStyle && (
              <span
                className={cn(
                  "inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[0.6875rem] font-semibold tracking-wide uppercase",
                  scoreStyle.bg,
                  scoreStyle.text,
                  scoreStyle.border,
                  "shadow-[0_1px_2px_rgba(0,0,0,0.06)]"
                )}
              >
                {scoreStyle.emoji && <span aria-hidden>{scoreStyle.emoji}</span>}
                {scoreStyle.label}
              </span>
            )}
            {isEnriching && (
              <span className="rounded-full bg-amber-50 text-amber-700 border border-amber-200/60 px-2.5 py-1 text-[0.625rem] font-semibold uppercase tracking-wider animate-pulse">
                AI bezig…
              </span>
            )}
            {enrichment && !isEnriching && (
              <span className="rounded-md bg-emerald-50 text-emerald-700 border border-emerald-200/70 px-2 py-0.5 text-[0.625rem] font-semibold">
                AI gescand
              </span>
            )}
          </div>
        </div>

        {/* Meta: date, units, sleep */}
        <div className="flex flex-col gap-1 text-xs text-slate-500">
          <div className="flex items-center gap-2">
            <span className="text-slate-400" aria-hidden>📅</span>
            <span>{formatDate(property.onlineSince ?? property.dateOnline)}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-slate-400" aria-hidden>🏠</span>
            <span>{units > 1 ? `${units} units` : "1 unit"}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-slate-400" aria-hidden>🛏</span>
            <span>{sleep > 0 ? `${sleep} slaapplaatsen` : "—"}</span>
          </div>
        </div>

        {/* Tags: status, portfolio, outcome */}
        <div className="flex flex-wrap items-center gap-2">
          {property.status && (
            <span className="rounded-full bg-slate-100 text-slate-600 border border-slate-200/80 px-2.5 py-0.5 text-[0.6875rem] font-medium tracking-tight shadow-[0_1px_2px_rgba(0,0,0,0.04)]">
              {property.status}
            </span>
          )}
          {hasPortfolio && (
            <span className="rounded-full bg-amber-50 text-amber-800 border border-amber-200/70 px-2.5 py-0.5 text-[0.6875rem] font-semibold shadow-[0_1px_2px_rgba(0,0,0,0.05)]">
              🏘 {portfolioCount} panden
            </span>
          )}
          {outcomeLabel && (
            <span
              className={cn(
                "rounded-md px-2 py-0.5 text-[0.625rem] font-medium",
                outcome === "afgewezen" && "bg-red-50 text-red-700 border border-red-200/70",
                (outcome === "terugbellen" || outcome === "callback") && "bg-amber-50 text-amber-800 border border-amber-200/70",
                (outcome === "interesse" || outcome === "gebeld_interesse") && "bg-emerald-50 text-emerald-700 border border-emerald-200/70"
              )}
            >
              {outcomeLabel}
            </span>
          )}
        </div>

        {/* Contact block */}
        <div className="mt-auto pt-3 border-t border-slate-100 space-y-2">
          {phones.length > 0 ? (
            phones.map((tel, i) => (
              <a
                key={i}
                href={`tel:${tel}`}
                onClick={(e) => e.stopPropagation()}
                className="flex items-center gap-2 text-xs text-slate-600 hover:text-slate-900 font-medium"
              >
                <span className="text-slate-400" aria-hidden>📞</span>
                {tel}
                {phones.length > 1 && <span className="text-slate-400 text-[0.65rem]">#{i + 1}</span>}
              </a>
            ))
          ) : property.email ? (
            <a
              href={`https://mail.google.com/mail/?view=cm&to=${encodeURIComponent(property.email)}`}
              target="_blank"
              rel="noreferrer"
              onClick={(e) => e.stopPropagation()}
              className="flex items-center gap-2 text-xs text-slate-600 hover:text-slate-900 truncate font-medium"
            >
              <span className="text-slate-400" aria-hidden>✉️</span>
              <span className="truncate">{property.email}</span>
            </a>
          ) : (
            <p className="flex items-center gap-2 text-xs text-slate-400 italic">
              <span aria-hidden>📵</span> Geen contact
            </p>
          )}
        </div>
      </div>
    </article>
  );
}
