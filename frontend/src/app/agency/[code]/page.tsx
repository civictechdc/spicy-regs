'use client';

import { useState, useCallback, useEffect, useMemo } from 'react';
import { useParams } from 'next/navigation';
import { Virtuoso } from 'react-virtuoso';
import { Header } from '@/components/Header';
import { DocketPost } from '@/components/feed/DocketPost';
import { ThreadedComments } from '@/components/feed/ThreadedComments';
import { AgencySidebar } from '@/components/feed/AgencySidebar';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { getAgencyInfo } from '@/lib/agencyMetadata';
import { Loader2 } from 'lucide-react';

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
  } catch {}
}

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

type SortTab = 'new' | 'hot' | 'open' | 'top';

export default function AgencyPage() {
  const params = useParams();
  const agencyCode = (params.code as string || '').toUpperCase();
  const agency = useMemo(() => getAgencyInfo(agencyCode), [agencyCode]);

  const { getRecentDockets, getAgencyStats, isReady } = useDuckDBService();

  const [dockets, setDockets] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [activeTab, setActiveTab] = useState<SortTab>('new');
  const [expandedComments, setExpandedComments] = useState<Set<string>>(new Set());
  const [stats, setStats] = useState<{ docketCount: number; documentCount: number; commentCount: number } | undefined>();

  // Bookmarks
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  useEffect(() => { setBookmarks(getStoredBookmarks()); }, []);

  const handleToggleBookmark = useCallback((docketId: string) => {
    setBookmarks(prev => {
      const next = new Set(prev);
      if (next.has(docketId)) next.delete(docketId);
      else next.add(docketId);
      saveBookmarks(next);
      return next;
    });
  }, []);

  // Load stats
  useEffect(() => {
    if (!isReady || !agencyCode) return;
    getAgencyStats(agencyCode).then(setStats).catch(console.error);
  }, [isReady, agencyCode, getAgencyStats]);

  // Load dockets
  const loadDockets = useCallback(async (reset = false) => {
    if (!isReady || loading) return;
    try {
      setLoading(true);
      const newOffset = reset ? 0 : offset;
      const results = await getRecentDockets(PAGE_SIZE, newOffset, agencyCode, 'recent');

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
  }, [isReady, loading, offset, agencyCode, getRecentDockets]);

  useEffect(() => {
    if (isReady && agencyCode) {
      setInitialLoading(true);
      loadDockets(true);
    }
  }, [isReady, agencyCode, activeTab]); // eslint-disable-line react-hooks/exhaustive-deps

  const toggleComments = useCallback((docketId: string) => {
    setExpandedComments(prev => {
      const next = new Set(prev);
      if (next.has(docketId)) next.delete(docketId);
      else next.add(docketId);
      return next;
    });
  }, []);

  const uniqueDockets = useMemo(() => {
    const seen = new Set<string>();
    return dockets.filter(d => {
      const id = stripQuotes(d.docket_id);
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    });
  }, [dockets]);

  const tabs: { key: SortTab; label: string }[] = [
    { key: 'hot', label: 'Hot' },
    { key: 'new', label: 'New' },
    { key: 'open', label: 'Open for Comment' },
    { key: 'top', label: 'Top' },
  ];

  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />

      {/* Banner */}
      <div
        className="h-32 relative"
        style={{
          background: `linear-gradient(135deg, ${agency.color}44, ${agency.color}11, var(--background))`,
        }}
      >
        <div className="absolute inset-0 bg-gradient-to-b from-transparent to-[var(--background)]" />
      </div>

      {/* Agency Header */}
      <div className="max-w-5xl mx-auto px-4 -mt-10 relative z-10 mb-6">
        <div className="flex items-end gap-4">
          <div
            className="agency-avatar agency-avatar-lg border-4 border-[var(--background)]"
            style={{ backgroundColor: agency.color }}
          >
            {agency.shortName}
          </div>
          <div className="pb-1">
            <h1 className="text-2xl font-bold text-[var(--foreground)]">
              {agency.name}
            </h1>
            <span className="text-sm text-[var(--muted)]">sr/{agencyCode}</span>
          </div>
        </div>

        {/* Stats */}
        {stats && (
          <div className="flex items-center gap-4 mt-3 text-sm text-[var(--muted)]">
            <span>{stats.docketCount.toLocaleString()} dockets</span>
            <span>·</span>
            <span>{stats.commentCount.toLocaleString()} comments</span>
            <span>·</span>
            <span>{stats.documentCount.toLocaleString()} documents</span>
          </div>
        )}

        <p className="text-sm text-[var(--muted)] mt-2 max-w-2xl">
          {agency.description}
        </p>
      </div>

      {/* Content */}
      <div className="max-w-5xl mx-auto px-4">
        <div className="flex gap-6">
          {/* Feed */}
          <div className="flex-1 min-w-0">
            {/* Sort Tabs */}
            <div className="flex items-center gap-1 mb-4 p-1 bg-[var(--surface)] rounded-xl border border-[var(--border)]">
              {tabs.map(tab => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`filter-chip !rounded-lg ${activeTab === tab.key ? 'filter-chip-active' : ''}`}
                >
                  {tab.label}
                </button>
              ))}
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
              <div style={{ height: '70vh' }}>
                <Virtuoso
                  style={{ height: '100%' }}
                  data={uniqueDockets}
                  endReached={() => { if (hasMore && !loading) loadDockets(false); }}
                  overscan={400}
                  itemContent={(index, item) => {
                    const docketId = stripQuotes(item.docket_id);
                    const isBookmarked = bookmarks.has(docketId);
                    const showComments = expandedComments.has(docketId);

                    return (
                      <div className="pb-3">
                        <DocketPost
                          item={item}
                          isBookmarked={isBookmarked}
                          onToggleBookmark={() => handleToggleBookmark(docketId)}
                          onViewComments={() => toggleComments(docketId)}
                          showComments={showComments}
                        />
                        {showComments && (
                          <div className="mt-0 rounded-b-xl overflow-hidden border border-t-0 border-[var(--border)]">
                            <ThreadedComments docketId={docketId} />
                          </div>
                        )}
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
