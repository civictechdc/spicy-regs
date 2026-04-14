'use client';

import { useEffect, useMemo } from 'react';
import { useDocketSearch } from '@/lib/search/useDocketSearch';
import { MiniDocketCard } from './MiniDocketCard';

const RELATED_LIMIT = 3;

/**
 * Common English + boilerplate words that show up in nearly every regulatory
 * docket title and add no signal to a "find similar dockets" query.
 *
 * Includes generic English stop-words ("the", "of") and regulatory boilerplate
 * ("notice", "proposed", "rulemaking", "rule", "comment", "request", "agency").
 * Without stripping these, a search for one regulation's title tends to surface
 * any other regulation that follows the same procedural template.
 */
const STOP_WORDS = new Set([
  // generic
  'a', 'an', 'and', 'or', 'but', 'the', 'of', 'in', 'on', 'at', 'to', 'for',
  'with', 'by', 'from', 'as', 'is', 'are', 'was', 'were', 'be', 'been',
  'this', 'that', 'these', 'those', 'it', 'its',
  // regulatory boilerplate
  'notice', 'proposed', 'rulemaking', 'rule', 'rules', 'comment', 'comments',
  'request', 'requests', 'agency', 'agencies', 'federal', 'public', 'final',
  'information', 'collection', 'meeting', 'availability', 'extension',
  'period', 'reopening', 'amendment', 'amendments',
]);

/**
 * Extract a search query from a docket title for finding related dockets.
 *
 * Strategy: split on non-letters, drop stop-words and short tokens, and keep
 * the longest remaining tokens (likely the topical keywords). Cap to a handful
 * of terms — MiniSearch ORs terms internally, so adding more rapidly drifts
 * off-topic.
 */
function extractSearchQuery(title: string): string {
  const tokens = title
    .toLowerCase()
    .split(/[^a-z]+/)
    .filter(t => t.length >= 4 && !STOP_WORDS.has(t));

  // Prefer longer tokens (proper nouns, technical terms) over short ones.
  const ranked = Array.from(new Set(tokens)).sort((a, b) => b.length - a.length);
  return ranked.slice(0, 5).join(' ');
}

interface RelatedDocketsProps {
  docketId: string;
  title: string;
}

export function RelatedDockets({ docketId, title }: RelatedDocketsProps) {
  const { ensure, search, status } = useDocketSearch();

  // Lazy-load the search index when this component mounts.
  useEffect(() => {
    void ensure().catch(() => { /* fail-soft: section just won't render */ });
  }, [ensure]);

  const related = useMemo(() => {
    if (!search || !title) return [];
    const query = extractSearchQuery(title);
    if (!query) return [];

    const upperId = docketId.toUpperCase();
    return search
      .search(query, { limit: RELATED_LIMIT + 5 })
      .filter(r => r.docket.docketId.toUpperCase() !== upperId)
      .slice(0, RELATED_LIMIT)
      .map(r => r.docket);
  }, [search, title, docketId]);

  // Fail-soft: render nothing if index isn't ready or there are no matches.
  if (status === 'error' || related.length === 0) return null;

  return (
    <section className="mt-4">
      <h3 className="text-sm font-semibold text-[var(--muted)] mb-2 px-1">
        Related Dockets
      </h3>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
        {related.map(d => (
          <MiniDocketCard key={d.docketId} docket={d} />
        ))}
      </div>
    </section>
  );
}
