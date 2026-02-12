'use client';

import { useState, useCallback, useEffect } from 'react';
import { Link2, ChevronDown, Loader2 } from 'lucide-react';
import { stringToColor, getInitials, timeAgo } from '@/lib/agencyMetadata';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';

interface ThreadedCommentsProps {
  docketId: string;
}

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

function parseCommentData(item: Record<string, any>) {
  let attrs: Record<string, any> = {};
  try {
    const raw = typeof item.raw_json === 'string' ? JSON.parse(item.raw_json) : item.raw_json;
    attrs = raw?.data?.attributes || {};
  } catch {
    // raw_json parsing failed, use top-level columns only
  }

  // Comment text: prefer direct column, then raw_json attribute
  const comment = stripQuotes(item.comment) || attrs.comment || '';

  return {
    commentId: stripQuotes(item.comment_id) || attrs.id || '',
    title: stripQuotes(item.title) || attrs.title || 'Untitled Comment',
    comment,
    firstName: attrs.firstName || stripQuotes(item.first_name) || '',
    lastName: attrs.lastName || stripQuotes(item.last_name) || '',
    organization: attrs.organization || stripQuotes(item.organization) || '',
    submitterType: attrs.submitterType || '',
    postedDate: attrs.postedDate || stripQuotes(item.posted_date) || '',
    documentSubtype: attrs.documentSubtype || stripQuotes(item.document_subtype) || '',
  };
}

interface CommentItemData {
  commentId: string;
  title: string;
  comment: string;
  firstName: string;
  lastName: string;
  organization: string;
  submitterType: string;
  postedDate: string;
  documentSubtype: string;
}

function CommentItem({ data }: { data: CommentItemData }) {
  const [expanded, setExpanded] = useState(false);

  const authorName = data.organization
    || [data.firstName, data.lastName].filter(Boolean).join(' ')
    || 'Anonymous';

  const isOrg = !!data.organization;
  const avatarColor = stringToColor(authorName);
  const initials = getInitials(authorName);
  const commentText = data.comment || data.title;
  const truncateLength = 400;
  const needsTruncation = commentText.length > truncateLength;

  const regsUrl = data.commentId
    ? `https://www.regulations.gov/comment/${data.commentId}`
    : '#';

  return (
    <div className="comment-thread py-3">
      <div className="thread-line" />

      <div className="flex items-start gap-2.5">
        {/* Avatar */}
        <div
          className="comment-avatar"
          style={{ backgroundColor: avatarColor }}
        >
          {initials}
        </div>

        <div className="flex-1 min-w-0">
          {/* Author Line */}
          <div className="flex items-center gap-2 text-sm">
            <span className="font-semibold text-[var(--foreground)]">
              {authorName}
            </span>
            {isOrg && (
              <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-[rgba(99,102,241,0.15)] text-[var(--accent-primary)]">
                Organization
              </span>
            )}
            {data.postedDate && (
              <span className="text-xs text-[var(--muted)]">
                {timeAgo(data.postedDate)}
              </span>
            )}
          </div>

          {/* Comment Text */}
          <div className="mt-1 text-sm text-[var(--muted)] leading-relaxed">
            {expanded || !needsTruncation
              ? commentText
              : `${commentText.slice(0, truncateLength)}...`}
            {needsTruncation && (
              <button
                onClick={() => setExpanded(!expanded)}
                className="ml-1 text-[var(--accent-primary)] hover:underline text-xs font-medium"
              >
                {expanded ? 'Show less' : 'Read more'}
              </button>
            )}
          </div>

          {/* Comment Actions */}
          <div className="flex items-center gap-3 mt-1.5">
            <a
              href={regsUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="action-btn !px-1 !py-0.5 text-xs"
            >
              <Link2 size={12} />
              Link
            </a>
          </div>
        </div>
      </div>
    </div>
  );
}

const PAGE_SIZE = 10;

export function ThreadedComments({ docketId }: ThreadedCommentsProps) {
  const { getCommentsForDocket, isReady } = useDuckDBService();
  const [comments, setComments] = useState<CommentItemData[]>([]);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [offset, setOffset] = useState(0);
  const [sortBy, setSortBy] = useState<'recent' | 'oldest'>('recent');

  const loadComments = useCallback(async (reset = false) => {
    if (!isReady || loading) return;

    try {
      setLoading(true);
      const newOffset = reset ? 0 : offset;
      const results = await getCommentsForDocket(docketId, PAGE_SIZE, newOffset, sortBy);
      const parsed = results.map(parseCommentData);

      if (reset) {
        setComments(parsed);
        setOffset(PAGE_SIZE);
      } else {
        setComments(prev => [...prev, ...parsed]);
        setOffset(prev => prev + PAGE_SIZE);
      }

      setHasMore(results.length === PAGE_SIZE);
    } catch (err) {
      console.error('Failed to load comments:', err);
    } finally {
      setLoading(false);
    }
  }, [isReady, loading, offset, docketId, getCommentsForDocket, sortBy]);

  useEffect(() => {
    if (isReady) {
      loadComments(true);
    }
  }, [isReady, docketId, sortBy]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSortChange = (newSort: 'recent' | 'oldest') => {
    setSortBy(newSort);
  };

  return (
    <div className="border-t border-[var(--border)] bg-[rgba(15,23,42,0.5)]">
      <div className="px-5 py-3">
        {/* Header */}
        <div className="flex items-center justify-between mb-3">
          <span className="text-sm font-semibold text-[var(--foreground)]">
            Comments
          </span>
          <div className="flex items-center gap-1">
            <span className="text-xs text-[var(--muted)] mr-1">Sort by:</span>
            <button
              onClick={() => handleSortChange('recent')}
              className={`filter-chip text-xs !px-2 !py-0.5 ${sortBy === 'recent' ? 'filter-chip-active' : ''}`}
            >
              Recent
            </button>
            <button
              onClick={() => handleSortChange('oldest')}
              className={`filter-chip text-xs !px-2 !py-0.5 ${sortBy === 'oldest' ? 'filter-chip-active' : ''}`}
            >
              Oldest
            </button>
          </div>
        </div>

        {/* Comment List */}
        {comments.length === 0 && !loading && (
          <p className="text-sm text-[var(--muted)] py-4 text-center">
            No comments found for this docket.
          </p>
        )}

        {comments.map((c, i) => (
          <CommentItem key={`${c.commentId}-${i}`} data={c} />
        ))}

        {/* Load More */}
        {hasMore && (
          <button
            onClick={() => loadComments(false)}
            disabled={loading}
            className="w-full mt-2 py-2 text-sm font-medium text-[var(--accent-primary)] hover:bg-[var(--surface-elevated)] rounded-lg transition-colors flex items-center justify-center gap-2"
          >
            {loading ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Loading...
              </>
            ) : (
              <>
                <ChevronDown size={14} />
                Load more comments
              </>
            )}
          </button>
        )}

        {loading && comments.length === 0 && (
          <div className="flex justify-center py-6">
            <Loader2 size={20} className="animate-spin text-[var(--accent-primary)]" />
          </div>
        )}
      </div>
    </div>
  );
}
