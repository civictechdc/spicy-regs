'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { Header } from '@/components/Header';
import { DocketPost } from '@/components/feed/DocketPost';
import { DocumentList } from '@/components/feed/DocumentList';
import { ThreadedComments } from '@/components/feed/ThreadedComments';
import { AgencySidebar } from '@/components/feed/AgencySidebar';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { Loader2, ArrowLeft } from 'lucide-react';

const BOOKMARKS_KEY = 'spicy-regs-bookmarks';

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

export default function DocketDetailPage() {
  const params = useParams();
  const agencyCode = ((params.code as string) || '').toUpperCase();
  const rawId = params.id as string || '';
  const docketId = decodeURIComponent(rawId).toUpperCase();

  const { getDocketById, getAgencyStats, getDocumentsForDocket, isReady } = useDuckDBService();
  const [docket, setDocket] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState<any>();
  const [documents, setDocuments] = useState<any[]>([]);
  const [docsLoading, setDocsLoading] = useState(true);

  // Bookmarks
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  useEffect(() => { setBookmarks(getStoredBookmarks()); }, []);

  const handleToggleBookmark = useCallback(() => {
    setBookmarks(prev => {
      const next = new Set(prev);
      if (next.has(docketId)) next.delete(docketId);
      else next.add(docketId);
      saveBookmarks(next);
      return next;
    });
  }, [docketId]);



  useEffect(() => {
    if (!isReady || !docketId) return;
    getDocketById(docketId)
      .then(result => {
        setDocket(result);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load docket:', err);
        setLoading(false);
      });
  }, [isReady, docketId, getDocketById]);

  // Load agency stats
  useEffect(() => {
    if (!isReady || !agencyCode) return;
    getAgencyStats(agencyCode).then(setStats).catch(console.error);
  }, [isReady, agencyCode, getAgencyStats]);

  // Load documents for this docket
  useEffect(() => {
    if (!isReady || !docketId) return;
    setDocsLoading(true);
    getDocumentsForDocket(docketId)
      .then(docs => {
        setDocuments(docs);
        setDocsLoading(false);
      })
      .catch(err => {
        console.error('Failed to load documents:', err);
        setDocsLoading(false);
      });
  }, [isReady, docketId, getDocumentsForDocket]);

  if (loading) {
    return (
      <div className="min-h-screen bg-[var(--background)]">
        <Header />
        <div className="flex justify-center py-24">
          <Loader2 size={32} className="animate-spin text-[var(--accent-primary)]" />
        </div>
      </div>
    );
  }

  if (!docket) {
    return (
      <div className="min-h-screen bg-[var(--background)]">
        <Header />
        <div className="max-w-3xl mx-auto px-4 py-16 text-center">
          <h1 className="text-2xl font-bold mb-2">Docket not found</h1>
          <p className="text-[var(--muted)] mb-4">{docketId}</p>
          <Link href={`/sr/${agencyCode}`} className="text-[var(--accent-primary)] hover:underline">
            ← Back to sr/{agencyCode}
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-5xl mx-auto px-4 py-6">
        {/* Back button */}
        <Link
          href={`/sr/${agencyCode}`}
          className="inline-flex items-center gap-1.5 text-sm text-[var(--muted)] hover:text-[var(--foreground)] mb-4 transition-colors"
        >
          <ArrowLeft size={14} />
          sr/{agencyCode}
        </Link>

        <div className="flex gap-6">
          {/* Main content */}
          <div className="flex-1 min-w-0">
            {/* Docket Post */}
            <DocketPost
              item={docket}
              isBookmarked={bookmarks.has(docketId)}
              onToggleBookmark={handleToggleBookmark}
              commentCount={Number(docket.comment_count || 0)}
              documentCount={Number(docket.document_count || 0)}
              isDetailView={true}
              showComments={true}
            />

            {/* Documents */}
            <div className="mt-4">
              <DocumentList documents={documents} loading={docsLoading} />
            </div>

            {/* Comments */}
            <div className="mt-4 rounded-xl overflow-hidden border border-[var(--border)]">
              <ThreadedComments docketId={docketId} modifyDate={stripQuotes(docket?.modify_date)} />
            </div>
          </div>

          {/* Sidebar */}
          {agencyCode && (
            <div className="hidden lg:block w-72 flex-shrink-0">
              <div className="sticky top-20">
                <AgencySidebar agencyCode={agencyCode} stats={stats} />
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
