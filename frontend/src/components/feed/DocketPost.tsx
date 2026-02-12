'use client';

import { useState, useMemo } from 'react';
import Link from 'next/link';
import {
  Bookmark,
  ExternalLink,
  MessageSquare,
  FileText,
  Paperclip,
  ChevronDown,
  ChevronUp,
  Download,
  Clock,
} from 'lucide-react';
import { getAgencyInfo, timeAgo, formatCount } from '@/lib/agencyMetadata';

interface DocketPostProps {
  item: Record<string, any>;
  isBookmarked: boolean;
  onToggleBookmark: () => void;
  commentCount?: number;
  documentCount?: number;
  /** If true, we're on the detail page — don't render title as link */
  isDetailView?: boolean;
  showComments?: boolean;
}

function stripQuotes(s: any): string {
  if (!s) return '';
  return String(s).replace(/^"|"$/g, '');
}

function decodeHtml(s: string): string {
  const doc = typeof document !== 'undefined'
    ? new DOMParser().parseFromString(s, 'text/html')
    : null;
  return doc?.documentElement.textContent || s;
}

function parseRawJson(item: Record<string, any>) {
  try {
    const raw = typeof item.raw_json === 'string' ? JSON.parse(item.raw_json) : item.raw_json;
    return raw?.data?.attributes || raw?.data || {};
  } catch {
    return {};
  }
}

export function DocketPost({
  item,
  isBookmarked,
  onToggleBookmark,
  commentCount = 0,
  documentCount = 0,
  isDetailView = false,
  showComments = false,
}: DocketPostProps) {
  const [expanded, setExpanded] = useState(false);

  const attrs = useMemo(() => parseRawJson(item), [item]);

  const docketId = stripQuotes(item.docket_id) || '';
  const agencyCode = stripQuotes(item.agency_code) || docketId.split('-')[0] || '';
  const agency = getAgencyInfo(agencyCode);

  const title = decodeHtml(stripQuotes(item.title) || attrs.title || docketId);
  const abstract = decodeHtml(stripQuotes(item.abstract) || attrs.abstract || '');
  const docketType = stripQuotes(item.docket_type) || attrs.docketType || '';
  const modifyDate = stripQuotes(item.modify_date) || attrs.modifyDate;

  // Comment period
  const commentEndDate = attrs.commentEndDate;
  const isOpenForComment = commentEndDate
    ? new Date(commentEndDate) > new Date()
    : false;

  // Attachments from documents
  const attachments = useMemo(() => {
    const fileUrl = stripQuotes(item.file_url);
    if (!fileUrl) return [];

    const fileName = fileUrl.split('/').pop() || 'Document';
    const ext = fileName.split('.').pop()?.toUpperCase() || 'FILE';

    return [{ name: fileName, url: fileUrl, type: ext }];
  }, [item]);

  const regsDotGovUrl = `https://www.regulations.gov/docket/${docketId}`;

  return (
    <article className="docket-post">
      <div className="p-5">
        {/* Header: Agency + Title */}
        <div className="flex items-start gap-3 mb-3">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 text-sm mb-1">
              <Link
                href={`/sr/${agencyCode}`}
                className="font-semibold hover:underline"
                style={{ color: agency.color }}
              >
                sr/{agencyCode}
              </Link>
              <span className="text-[var(--muted)]">·</span>
              <span className="font-mono-id text-[var(--muted)]">{docketId}</span>
              {modifyDate && (
                <>
                  <span className="text-[var(--muted)]">·</span>
                  <span className="text-[var(--muted)] text-xs">{timeAgo(modifyDate)}</span>
                </>
              )}
            </div>

            {isDetailView ? (
              <h3 className="text-lg font-semibold text-[var(--foreground)] leading-snug">
                {title}
              </h3>
            ) : (
              <Link
                href={`/sr/${agencyCode}/${encodeURIComponent(docketId)}`}
                className="text-lg font-semibold text-[var(--foreground)] leading-snug hover:text-[var(--accent-primary)] transition-colors block"
              >
                {title}
              </Link>
            )}

            {/* Badges */}
            <div className="flex items-center gap-2 mt-1.5">
              {docketType && (
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-[var(--surface-elevated)] text-[var(--muted)]">
                  {docketType}
                </span>
              )}
              {isOpenForComment && (
                <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium status-open bg-[rgba(34,197,94,0.1)]">
                  <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-green)]" />
                  Open for Comment
                  {commentEndDate && (
                    <span className="text-[var(--muted)] ml-1">
                      · Ends {new Date(commentEndDate).toLocaleDateString()}
                    </span>
                  )}
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Abstract */}
        {abstract && (
          <div className="mt-3 text-sm text-[var(--muted)] leading-relaxed">
            {expanded || abstract.length <= 280
              ? abstract
              : `${abstract.slice(0, 280)}...`}
            {abstract.length > 280 && (
              <button
                onClick={() => setExpanded(!expanded)}
                className="ml-1 text-[var(--accent-primary)] hover:underline text-xs font-medium"
              >
                {expanded ? 'Show less' : 'Read more'}
              </button>
            )}
          </div>
        )}

        {/* Attachments */}
        {attachments.length > 0 && (
          <div className="mt-3 space-y-1.5">
            <div className="flex items-center gap-1.5 text-xs font-medium text-[var(--muted)]">
              <Paperclip size={12} />
              Attachments
            </div>
            {attachments.map((att, i) => (
              <a
                key={i}
                href={att.url}
                target="_blank"
                rel="noopener noreferrer"
                className="attachment-link"
              >
                <FileText size={14} className="text-[var(--accent-primary)]" />
                <span className="flex-1 truncate">{att.name}</span>
                <span className="text-xs text-[var(--muted)]">{att.type}</span>
                <Download size={14} className="text-[var(--muted)]" />
              </a>
            ))}
          </div>
        )}

        {/* Metadata Row */}
        <div className="flex items-center gap-4 mt-4 text-xs text-[var(--muted)]">
          {documentCount > 0 && (
            <span className="metadata-pill">
              <FileText size={12} />
              {formatCount(documentCount)} docs
            </span>
          )}
          {commentCount > 0 && (
            <span className="metadata-pill">
              <MessageSquare size={12} />
              {formatCount(commentCount)} comments
            </span>
          )}
          {modifyDate && (
            <span className="metadata-pill">
              <Clock size={12} />
              Modified {timeAgo(modifyDate)}
            </span>
          )}
        </div>

        {/* Action Bar */}
        <div className="flex items-center gap-1 mt-3 pt-3 border-t border-[var(--border)]">
          <button
            onClick={onToggleBookmark}
            className={`action-btn ${isBookmarked ? 'text-[var(--accent-primary)]' : ''}`}
          >
            <Bookmark size={16} fill={isBookmarked ? 'currentColor' : 'none'} />
            {isBookmarked ? 'Saved' : 'Save'}
          </button>

          {!isDetailView && (
            <Link
              href={`/sr/${agencyCode}/${encodeURIComponent(docketId)}`}
              className="action-btn"
            >
              <MessageSquare size={16} />
              {commentCount > 0 ? `${formatCount(commentCount)} Comments` : 'Comments'}
            </Link>
          )}

          <a
            href={regsDotGovUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="action-btn ml-auto"
          >
            <ExternalLink size={14} />
            regulations.gov
          </a>
        </div>
      </div>
    </article>
  );
}
