'use client';

import { FileText, Download, ExternalLink } from 'lucide-react';
import { timeAgo } from '@/lib/agencyMetadata';

/** Strip wrapping quotes from Parquet string values */
function stripQuotes(s: any): string {
  if (s == null) return '';
  return String(s).replace(/^"|"$/g, '');
}

interface DocumentListProps {
  documents: any[];
  loading?: boolean;
}

export function DocumentList({ documents, loading = false }: DocumentListProps) {
  if (loading) {
    return (
      <div className="document-list">
        <h3 className="section-title">
          <FileText size={16} />
          Documents
        </h3>
        <div className="flex justify-center py-8 text-sm text-[var(--muted)]">
          Loading documents...
        </div>
      </div>
    );
  }

  if (documents.length === 0) {
    return null;
  }

  return (
    <div className="document-list">
      <h3 className="section-title">
        <FileText size={16} />
        Documents ({documents.length})
      </h3>
      <div className="space-y-2">
        {documents.map((doc, i) => {
          const docId = stripQuotes(doc.document_id);
          const title = stripQuotes(doc.title) || docId;
          const docType = stripQuotes(doc.document_type);
          const postedDate = stripQuotes(doc.posted_date);
          const commentEndDate = stripQuotes(doc.comment_end_date);
          const fileUrl = stripQuotes(doc.file_url);

          const isOpenForComment = commentEndDate
            ? new Date(commentEndDate) > new Date()
            : false;

          const daysLeft = commentEndDate
            ? Math.ceil((new Date(commentEndDate).getTime() - Date.now()) / (1000 * 60 * 60 * 24))
            : 0;

          const getUrgencyColor = () => {
            if (daysLeft < 3) return 'var(--accent-red, #dc2626)';
            if (daysLeft < 14) return 'var(--accent-amber, #d97706)';
            return 'var(--accent-green)';
          };

          return (
            <div key={docId || i} className="document-card">
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  {/* Document ID and type */}
                  <div className="flex items-center gap-2 mb-1">
                    <span className="font-mono-id text-[var(--muted)] text-xs">{docId}</span>
                    {docType && (
                      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-[var(--surface-elevated)] text-[var(--muted)]">
                        {docType}
                      </span>
                    )}
                  </div>

                  {/* Title */}
                  <p className="text-sm font-medium text-[var(--foreground)] leading-snug">
                    {title}
                  </p>

                  {/* Comment period / posted date */}
                  <div className="flex items-center gap-2 mt-1.5">
                    {isOpenForComment && commentEndDate ? (
                      <span
                        className="inline-flex items-center gap-1 text-xs font-medium"
                        style={{ color: getUrgencyColor() }}
                      >
                        <span
                          className="w-1.5 h-1.5 rounded-full"
                          style={{ backgroundColor: getUrgencyColor() }}
                        />
                        Open for Comment · {daysLeft}d left · Closes{' '}
                        {new Date(commentEndDate).toLocaleDateString()}
                      </span>
                    ) : commentEndDate ? (
                      <span className="text-xs text-[var(--muted)]">
                        Comment Period Closed
                      </span>
                    ) : postedDate ? (
                      <span className="text-xs text-[var(--muted)]">
                        Posted {timeAgo(postedDate)}
                      </span>
                    ) : null}
                  </div>
                </div>

                {/* File link */}
                {fileUrl && (
                  <a
                    href={fileUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-1 px-2.5 py-1.5 rounded-md text-xs font-medium
                      bg-[var(--surface-elevated)] text-[var(--accent-primary)] hover:bg-[var(--accent-primary)]
                      hover:text-white transition-colors shrink-0"
                    title="Download document"
                  >
                    <Download size={12} />
                    PDF
                  </a>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
