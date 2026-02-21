'use client';

import { useEffect, useState, useCallback, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { Header } from '@/components/Header';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';

type SearchMode = 'hybrid' | 'keyword';

interface SearchResult {
  id: string;
  title: string;
  text?: string;
  comment?: string;
  docket_id: string;
  agency_code: string;
  posted_date?: string;
  score?: number;
  type?: string;
}

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

function ResultCard({ result }: { result: SearchResult }) {
  const regsUrl = result.id
    ? `https://www.regulations.gov/comment/${result.id}`
    : undefined;
  const docketUrl = result.agency_code && result.docket_id
    ? `/sr/${result.agency_code}/${result.docket_id}`
    : undefined;
  const commentText = result.comment || result.text || '';

  return (
    <div className="card p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            {result.agency_code && (
              <span className="text-xs font-mono px-1.5 py-0.5 rounded bg-[var(--surface-raised)] text-[var(--muted)]">
                {result.agency_code}
              </span>
            )}
            {result.type && (
              <span className="text-xs px-1.5 py-0.5 rounded bg-[var(--surface-raised)] text-[var(--muted)]">
                {result.type}
              </span>
            )}
            {result.score != null && (
              <span className="text-xs px-1.5 py-0.5 rounded bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]">
                {(result.score * 100).toFixed(0)}% match
              </span>
            )}
          </div>
          <h3 className="text-sm font-medium text-[var(--foreground)] mb-1 line-clamp-2">
            {result.title || 'Untitled'}
          </h3>
          {commentText && (
            <p className="text-xs text-[var(--muted)] line-clamp-3 mb-2">
              {commentText}
            </p>
          )}
          <div className="flex items-center gap-3 text-xs text-[var(--muted)]">
            {result.posted_date && (
              <span>{new Date(result.posted_date).toLocaleDateString()}</span>
            )}
            {docketUrl && (
              <a
                href={docketUrl}
                className="hover:text-[var(--accent-primary)] transition-colors"
              >
                {result.docket_id}
              </a>
            )}
            {regsUrl && (
              <a
                href={regsUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="hover:text-[var(--accent-primary)] transition-colors"
              >
                regulations.gov
              </a>
            )}
            {result.id && (
              <a
                href={`/search?similar=${encodeURIComponent(result.id)}`}
                className="hover:text-[var(--accent-primary)] transition-colors"
              >
                Find similar
              </a>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function normalizeKeywordResult(result: any): SearchResult {
  return {
    id: stripQuotes(result.comment_id) || stripQuotes(result.document_id) || stripQuotes(result.docket_id) || '',
    title: stripQuotes(result.title) || '',
    comment: stripQuotes(result.comment) || '',
    docket_id: stripQuotes(result.docket_id) || '',
    agency_code: stripQuotes(result.agency_code) || '',
    posted_date: stripQuotes(result.posted_date) || '',
    type: result.type || '',
  };
}

function SearchResults() {
  const searchParams = useSearchParams();
  const query = searchParams.get('q');
  const similarId = searchParams.get('similar');
  const modeParam = searchParams.get('mode');

  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [searchMode, setSearchMode] = useState<SearchMode>(
    modeParam === 'keyword' ? 'keyword' : 'hybrid'
  );

  const { searchResources } = useDuckDBService();

  const SEARCH_API = process.env.NEXT_PUBLIC_SEARCH_API_URL || '/api/search';

  const performHybridSearch = useCallback(async (q: string) => {
    const res = await fetch(`${SEARCH_API}?q=${encodeURIComponent(q)}`);
    if (!res.ok) throw new Error(`Search API returned ${res.status}`);
    const data = await res.json();
    return data.results as SearchResult[];
  }, [SEARCH_API]);

  const performSimilarSearch = useCallback(async (id: string) => {
    const res = await fetch(`${SEARCH_API}/similar?id=${encodeURIComponent(id)}`);
    if (!res.ok) throw new Error(`Similar API returned ${res.status}`);
    const data = await res.json();
    return data.results as SearchResult[];
  }, [SEARCH_API]);

  // Handle similar comment lookup
  useEffect(() => {
    if (!similarId) return;

    async function fetchSimilar() {
      try {
        setLoading(true);
        setSearchMode('hybrid');
        const similar = await performSimilarSearch(similarId!);
        setResults(similar);
      } catch (e) {
        console.error('Similar search failed', e);
        setResults([]);
      } finally {
        setLoading(false);
      }
    }
    fetchSimilar();
  }, [similarId, performSimilarSearch]);

  // Handle query-based search
  useEffect(() => {
    if (!query || similarId) return;

    async function performSearch() {
      try {
        setLoading(true);
        if (searchMode === 'hybrid') {
          const hybridResults = await performHybridSearch(query!);
          setResults(hybridResults);
        } else {
          const keywordResults = await searchResources(query!);
          setResults(keywordResults.map(normalizeKeywordResult));
        }
      } catch (e) {
        console.error('Search failed', e);
      } finally {
        setLoading(false);
      }
    }
    performSearch();
  }, [query, searchMode, similarId, searchResources, performHybridSearch]);

  const hasQuery = query || similarId;

  if (!hasQuery) {
    return (
      <div className="text-center py-12 text-[var(--muted)]">
        Use the search bar above to find regulations.
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex justify-center p-12">
        <div className="animate-spin h-8 w-8 border-2 border-[var(--accent-primary)] border-t-transparent rounded-full" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Search mode toggle (only show for query-based search, not similar) */}
      {query && !similarId && (
        <div className="flex items-center gap-2">
          <button
            onClick={() => setSearchMode('hybrid')}
            className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
              searchMode === 'hybrid'
                ? 'border-[var(--accent-primary)] bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]'
                : 'border-[var(--border)] text-[var(--muted)] hover:border-[var(--accent-primary)]/50'
            }`}
          >
            Hybrid Search
          </button>
          <button
            onClick={() => setSearchMode('keyword')}
            className={`text-xs px-3 py-1.5 rounded-full border transition-colors ${
              searchMode === 'keyword'
                ? 'border-[var(--accent-primary)] bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]'
                : 'border-[var(--border)] text-[var(--muted)] hover:border-[var(--accent-primary)]/50'
            }`}
          >
            Keyword Only
          </button>
        </div>
      )}

      <h2 className="text-xl font-semibold">
        {similarId ? (
          <>Similar comments to <span className="gradient-text font-mono text-base">{similarId}</span></>
        ) : (
          <>Results for "<span className="gradient-text">{query}</span>"</>
        )}
      </h2>

      {results.length === 0 && (
        <div className="text-center py-12 text-[var(--muted)]">
          No results found.{' '}
          {searchMode === 'hybrid' && (
            <>Try <button onClick={() => setSearchMode('keyword')} className="underline">keyword search</button> instead.</>
          )}
        </div>
      )}

      {results.length > 0 && (
        <div className="space-y-4">
          {results.map((result, idx) => (
            <ResultCard key={`${result.id}-${idx}`} result={result} />
          ))}
        </div>
      )}
    </div>
  );
}

export default function SearchPage() {
  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <Suspense fallback={
          <div className="flex justify-center p-12">
            <div className="animate-spin h-8 w-8 border-2 border-[var(--accent-primary)] border-t-transparent rounded-full" />
          </div>
        }>
          <SearchResults />
        </Suspense>
      </main>
    </div>
  );
}
