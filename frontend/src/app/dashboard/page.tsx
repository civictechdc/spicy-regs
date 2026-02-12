'use client';

import { useState, useCallback, useEffect, Suspense, useMemo } from 'react';
import { Virtuoso } from 'react-virtuoso';
import { Header } from '@/components/Header';
import { DocketPost } from '@/components/feed/DocketPost';
import { FeedFilters } from '@/components/feed/FeedFilters';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { Flame, Loader2 } from 'lucide-react';

const BOOKMARKS_KEY = 'spicy-regs-bookmarks';
const PAGE_SIZE = 20;

function getStoredBookmarks(): Set<string> {
  if (typeof window === 'undefined') return new Set();
  try {
    const stored = localStorage.getItem(BOOKMARKS_KEY);
    return stored ? new Set(JSON.parse(stored)) : new Set();
  } catch {
    return new Set();
  }
}

function saveBookmarks(bookmarks: Set<string>) {
  if (typeof window === 'undefined') return;
  try {
    localStorage.setItem(BOOKMARKS_KEY, JSON.stringify([...bookmarks]));
  } catch (e) {
    console.error('Failed to save bookmarks', e);
  }
}

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

function DocketFeed() {
  const { getRecentDockets, getCommentCounts, getDataCount, isReady } = useDuckDBService();

  const [dockets, setDockets] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [commentCounts, setCommentCounts] = useState<Record<string, number>>({});


  // Filters
  const [selectedAgency, setSelectedAgency] = useState('');
  const [sortBy, setSortBy] = useState<'recent' | 'popular' | 'open'>(() => {
    if (typeof window === 'undefined') return 'recent';
    try {
      return (localStorage.getItem('spicy-regs-sort-preference') as any) || 'recent';
    } catch {
      return 'recent';
    }
  });

  // Bookmarks
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  useEffect(() => { setBookmarks(getStoredBookmarks()); }, []);

  const handleToggleBookmark = useCallback((docketId: string) => {
    setBookmarks(prev => {
      const next = new Set(prev);
      if (next.has(docketId)) {
        next.delete(docketId);
      } else {
        next.add(docketId);
      }
      saveBookmarks(next);
      return next;
    });
  }, []);

  // Load dockets
  const loadDockets = useCallback(async (reset = false) => {
    if (!isReady || loading) return;

    try {
      setLoading(true);
      const newOffset = reset ? 0 : offset;
      const results = await getRecentDockets(PAGE_SIZE, newOffset, selectedAgency || undefined, sortBy);

      if (reset) {
        setDockets(results);
        setOffset(PAGE_SIZE);
      } else {
        setDockets(prev => [...prev, ...results]);
        setOffset(prev => prev + PAGE_SIZE);
      }

      setHasMore(results.length === PAGE_SIZE);
    } catch (err) {
      console.error('Failed to load dockets:', err);
    } finally {
      setLoading(false);
      setInitialLoading(false);
    }
  }, [isReady, loading, offset, selectedAgency, sortBy, getRecentDockets]);

  // Initial load and filter change
  useEffect(() => {
    if (isReady) {
      setInitialLoading(true);
      loadDockets(true);
    }
  }, [isReady, selectedAgency, sortBy]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch comment counts when dockets change
  useEffect(() => {
    if (dockets.length === 0) return;
    const ids = dockets.map(d => stripQuotes(d.docket_id));
    getCommentCounts(ids)
      .then(counts => setCommentCounts(prev => ({ ...prev, ...counts })))
      .catch(console.error);
  }, [dockets, getCommentCounts]);
  // Deduplicate
  const uniqueDockets = useMemo(() => {
    const seen = new Set<string>();
    return dockets.filter(d => {
      const id = stripQuotes(d.docket_id);
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    });
  }, [dockets]);

  if (initialLoading) {
    return (
      <div className="flex flex-col items-center justify-center py-24 gap-4">
        <Loader2 size={32} className="animate-spin text-[var(--accent-primary)]" />
        <p className="text-[var(--muted)] text-sm">Loading feed...</p>
      </div>
    );
  }

  return (
    <div>
      {/* Filter Bar */}
      <div className="mb-6 p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)]">
        <FeedFilters
          selectedAgency={selectedAgency}
          onAgencyChange={(a) => { setSelectedAgency(a); }}
          sortBy={sortBy}
          onSortChange={(s) => { setSortBy(s); }}
        />
      </div>

      {/* Feed */}
      {uniqueDockets.length === 0 && !loading ? (
        <div className="text-center py-16">
          <p className="text-lg text-[var(--muted)]">No dockets found.</p>
          <p className="text-sm text-[var(--muted-foreground)] mt-1">
            Try adjusting your filters or search.
          </p>
        </div>
      ) : (
        <div style={{ height: '75vh' }}>
          <Virtuoso
            style={{ height: '100%' }}
            data={uniqueDockets}
            endReached={() => {
              if (hasMore && !loading) loadDockets(false);
            }}
            overscan={400}
            itemContent={(index, item) => {
              const docketId = stripQuotes(item.docket_id);
              const isBookmarked = bookmarks.has(docketId);

              return (
                <div className="pb-3">
                  <DocketPost
                    item={item}
                    isBookmarked={isBookmarked}
                    onToggleBookmark={() => handleToggleBookmark(docketId)}
                    commentCount={commentCounts[docketId.toUpperCase()] || 0}
                  />
                </div>
              );
            }}
            components={{
              Footer: () =>
                loading ? (
                  <div className="flex justify-center py-6">
                    <Loader2 size={20} className="animate-spin text-[var(--accent-primary)]" />
                  </div>
                ) : !hasMore && uniqueDockets.length > 0 ? (
                  <div className="text-center py-6 text-sm text-[var(--muted)]">
                    You&apos;ve reached the end 🌶️
                  </div>
                ) : null,
            }}
          />
        </div>
      )}
    </div>
  );
}

export default function DashboardPage() {
  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-3xl mx-auto px-4 py-6">
        {/* Hero */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center gap-2 mb-3">
            <Flame size={28} className="text-[var(--accent-primary)]" />
            <h1 className="text-3xl font-bold">
              <span className="gradient-text">Spicy Regs</span>
            </h1>
          </div>
          <p className="text-[var(--muted)] text-sm">
            Explore federal regulations. Dockets as posts, agencies as communities.
          </p>
        </div>

        <Suspense
          fallback={
            <div className="flex justify-center py-12">
              <Loader2 size={24} className="animate-spin text-[var(--accent-primary)]" />
            </div>
          }
        >
          <DocketFeed />
        </Suspense>
      </main>
    </div>
  );
}
