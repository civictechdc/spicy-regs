'use client';

import { useState, useCallback, useEffect, useMemo } from 'react';
import { useParams } from 'next/navigation';
import { Virtuoso } from 'react-virtuoso';
import { Header } from '@/components/Header';
import { DocketPost } from '@/components/feed/DocketPost';
import { FeedFilters } from '@/components/feed/FeedFilters';
import { AgencySidebar } from '@/components/feed/AgencySidebar';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { getAgencyInfo } from '@/lib/agencyMetadata';
import { AgencyAvatar } from '@/components/feed/AgencyAvatar';
import { Loader2 } from 'lucide-react';

const PAGE_SIZE = 20;

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

export default function AgencyPage() {
  const params = useParams();
  const agencyCode = (params.code as string || '').toUpperCase();
  const agency = useMemo(() => getAgencyInfo(agencyCode), [agencyCode]);

  const { getRecentDocketsWithCounts, getAgencyStats, isReady } = useDuckDBService();

  const [dockets, setDockets] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [commentCounts, setCommentCounts] = useState<Record<string, number>>({});
  const [stats, setStats] = useState<{ docketCount: number; documentCount: number; commentCount: number } | undefined>();

  // Filters (same as feed)
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

  // Bookmarks
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());

  const handleToggleBookmark = useCallback((docketId: string) => {
    setBookmarks(prev => {
      const next = new Set(prev);
      if (next.has(docketId)) next.delete(docketId);
      else next.add(docketId);
      return next;
    });
  }, []);

  // Load stats
  useEffect(() => {
    if (!isReady || !agencyCode) return;
    getAgencyStats(agencyCode).then(setStats).catch(console.error);
  }, [isReady, agencyCode, getAgencyStats]);

  // Load dockets (with comment counts in a single query, same as feed)
  const loadDockets = useCallback(async (reset = false) => {
    if (!isReady || loading) return;
    try {
      setLoading(true);
      const newOffset = reset ? 0 : offset;
      const { dockets: results, commentCounts: counts } = await getRecentDocketsWithCounts(
        PAGE_SIZE, newOffset, agencyCode, sortBy, dateRange || undefined, docketType || undefined
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
  }, [isReady, loading, offset, agencyCode, sortBy, dateRange, docketType, getRecentDocketsWithCounts]);

  useEffect(() => {
    if (isReady && agencyCode) {
      setInitialLoading(true);
      loadDockets(true);
    }
  }, [isReady, agencyCode, sortBy, dateRange, docketType]); // eslint-disable-line react-hooks/exhaustive-deps
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
    <div className="min-h-screen bg-[var(--background)]">
      <Header />

          

      {/* Content */}
      <div className="max-w-5xl mx-auto p-4 pb-12">
        <div className="flex gap-6">
          {/* Feed */}
          <div className="flex-1 min-w-0">
            {/* Filters */}
            <div className="mb-4 p-3 bg-[var(--surface)] rounded-xl border border-[var(--border)]">
              <FeedFilters
                selectedAgency={selectedAgency}
                onAgencyChange={setSelectedAgency}
                sortBy={sortBy}
                onSortChange={setSortBy}
                dateRange={dateRange}
                onDateRangeChange={setDateRange}
                docketType={docketType}
                onDocketTypeChange={setDocketType}
              />
            </div>

            {initialLoading ? (
              <div className="flex justify-center py-16">
                <Loader2 size={24} className="animate-spin text-[var(--accent-primary)]" />
              </div>
            ) : uniqueDockets.length === 0 ? (
              <div className="text-center py-16">
                <p className="text-lg text-[var(--muted)]">No dockets found for this agency.</p>
              </div>
            ) : (
              <div style={{ height: 'calc(100vh - 180px)' }}>
                <Virtuoso
                  style={{ height: '100%' }}
                  data={uniqueDockets}
                  endReached={() => { if (hasMore && !loading) loadDockets(false); }}
                  overscan={400}
                  itemContent={(index, item) => {
                    const docketId = stripQuotes(item.docket_id);

                    return (
                      <div className="pb-3">
                        <DocketPost
                          item={item}
                          commentCount={Number(item.comment_count || 0) || commentCounts[docketId.toUpperCase()] || 0}
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
                          End of dockets for sr/{agencyCode} 🌶️
                        </div>
                      ) : null,
                  }}
                />
              </div>
            )}
          </div>

          {/* Sidebar */}
          <div className="hidden lg:block w-72 flex-shrink-0">
            <div className="sticky top-20">
              <AgencySidebar agencyCode={agencyCode} stats={stats} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
