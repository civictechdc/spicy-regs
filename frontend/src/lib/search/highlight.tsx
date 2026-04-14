import { ReactNode } from "react";

/**
 * Render `text` with spans matching any term in `terms` wrapped in <mark>.
 *
 * Matches are case-insensitive and prefix-based, mirroring MiniSearch's
 * query semantics so the rendered highlights line up with why the
 * result matched.
 *
 * Does NOT use dangerouslySetInnerHTML — every piece goes through React's
 * normal escaping, so `text` is safe even if it contains HTML.
 */
export function renderHighlighted(text: string, terms: string[]): ReactNode {
  if (!text) return text;
  if (terms.length === 0) return text;

  const escaped = terms
    .map((t) => t.trim())
    .filter((t) => t.length > 1)
    .map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));

  if (escaped.length === 0) return text;

  // Join with | and wrap in a group; match each term as a word-boundary-ish
  // prefix so "clean" matches "cleaning" but "nder" doesn't randomly match
  // inside "underneath".
  const pattern = new RegExp(`(${escaped.join("|")})`, "gi");
  const parts = text.split(pattern);

  return parts.map((part, i) => {
    if (i % 2 === 1) {
      // odd indices are the matched groups
      return (
        <mark
          key={i}
          className="bg-[var(--accent-primary)]/20 text-[var(--foreground)] rounded px-0.5"
        >
          {part}
        </mark>
      );
    }
    return part;
  });
}

/**
 * Build a snippet centered on the first occurrence of any term.
 * Falls back to the leading portion of the text if no term matches.
 */
export function buildSnippet(text: string, terms: string[], window = 240): string {
  if (!text) return "";
  if (text.length <= window) return text;

  const lower = text.toLowerCase();
  let firstMatch = -1;
  for (const term of terms) {
    const t = term.trim().toLowerCase();
    if (t.length < 2) continue;
    const idx = lower.indexOf(t);
    if (idx !== -1 && (firstMatch === -1 || idx < firstMatch)) {
      firstMatch = idx;
    }
  }

  if (firstMatch === -1) {
    return text.slice(0, window) + "…";
  }

  const halfWindow = Math.floor(window / 2);
  const start = Math.max(0, firstMatch - halfWindow);
  const end = Math.min(text.length, start + window);
  const prefix = start > 0 ? "…" : "";
  const suffix = end < text.length ? "…" : "";
  return prefix + text.slice(start, end) + suffix;
}
