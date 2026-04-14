'use client';

import { useState, useCallback, useEffect, Suspense, useMemo } from 'react';
import { Virtuoso } from 'react-virtuoso';
import { Header } from '@/components/Header';
import { DocketPost } from '@/components/feed/DocketPost';
import { FeedFilters } from '@/components/feed/FeedFilters';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { Flame, Loader2 } from 'lucide-react';

const PAGE_SIZE = 20;

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

function DocketFeed() {
  const { getRecentDocketsWithCounts, isReady } = useDuckDBService();

  const [dockets, setDockets] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [commentCounts, setCommentCounts] = useState<Record<string, number>>({});


  // Filters
  const [selectedAgency, setSelectedAgency] = useState('');
  const [sortBy, setSortBy] = useState<'recent' | 'popular' | 'open' | 'closed'>(() => {
    if (typeof window === 'undefined') return 'recent';
    try {
      return (localStorage.getItem('spicy-regs-sort-preference') as any) || 'recent';
    } catch {
      return 'recent';
    }
  });
  const [dateRange, setDateRange] = useState<'' | '7d' | '30d' | '90d' | '365d'>(() => {
    if (typeof window === 'undefined') return '';
    try {
      return (localStorage.getItem('spicy-regs-date-preference') as any) || '';
    } catch {
      return '';
    }
  });
  const [docketType, setDocketType] = useState<'' | 'rule' | 'nonrule' | 'other'>(() => {
    if (typeof window === 'undefined') return '';
    try {
      return (localStorage.getItem('spicy-regs-type-preference') as any) || '';
    } catch {
      return '';
    }
  });

  // Load dockets (combined with comment counts in a single query)
  const loadDockets = useCallback(async (reset = false) => {
    if (!isReady || loading) return;

    try {
      setLoading(true);
      const newOffset = reset ? 0 : offset;
      const { dockets: results, commentCounts: counts } = await getRecentDocketsWithCounts(
        PAGE_SIZE, newOffset, selectedAgency || undefined, sortBy, dateRange || undefined, docketType || undefined
      );

      if (reset) {
        setDockets(results);
        setCommentCounts(counts);
        setOffset(PAGE_SIZE);
      } else {
        setDockets(prev => [...prev, ...results]);
        setCommentCounts(prev => ({ ...prev, ...counts }));
        setOffset(prev => prev + PAGE_SIZE);
      }

      setHasMore(results.length === PAGE_SIZE);
    } catch (err) {
      console.error('Failed to load dockets:', err);
    } finally {
      setLoading(false);
      setInitialLoading(false);
    }
  }, [isReady, loading, offset, selectedAgency, sortBy, dateRange, docketType, getRecentDocketsWithCounts]);

  // Initial load and filter change
  useEffect(() => {
    if (isReady) {
      setInitialLoading(true);
      loadDockets(true);
    }
  }, [isReady, selectedAgency, sortBy, dateRange, docketType]); // eslint-disable-line react-hooks/exhaustive-deps

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

  return (
    <div>
      {/* Filter Bar — always visible */}
      <div className="mb-6 p-4 bg-[var(--surface)] rounded-xl border border-[var(--border)]">
        <FeedFilters
          selectedAgency={selectedAgency}
          onAgencyChange={(a) => { setSelectedAgency(a); }}
          sortBy={sortBy}
          onSortChange={(s) => { setSortBy(s); }}
          dateRange={dateRange}
          onDateRangeChange={(d) => { setDateRange(d); }}
          docketType={docketType}
          onDocketTypeChange={(t) => { setDocketType(t); }}
        />
      </div>

      {/* Feed */}
      {initialLoading ? (
        <div className="flex flex-col items-center justify-center py-24 gap-4">
          <Loader2 size={32} className="animate-spin text-[var(--accent-primary)]" />
          <p className="text-[var(--muted)] text-sm">Loading feed...</p>
        </div>
      ) : uniqueDockets.length === 0 && !loading ? (
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

              return (
                <div className="pb-3">
                  <DocketPost
                    item={item}
                    commentCount={Number(item.comment_count || 0) || commentCounts[docketId.toUpperCase()] || 0}
                    documentCount={Number(item.document_count || 0)}
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
