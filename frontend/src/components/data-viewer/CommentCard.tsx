import { useState } from 'react';
import { RegulationData } from '@/lib/api';
import { stripQuotes, formatDate, parseRawJson } from './utils';
import { BookmarkButton } from './BookmarkButton';

interface CommentCardProps {
  item: RegulationData;
  isBookmarked: boolean;
  onToggleBookmark: () => void;
}

export function CommentCard({ item, isBookmarked, onToggleBookmark }: CommentCardProps) {
  const [expanded, setExpanded] = useState(false);
  const parsedJson = parseRawJson(item.raw_json);
  const attributes = parsedJson?.data?.attributes || parsedJson?.attributes || (parsedJson && !parsedJson.data ? parsedJson : {});
  const included = parsedJson?.included || [];

  const commentId = stripQuotes(item.comment_id) || attributes.id || item.docket_id;
  const title = stripQuotes(item.title) || attributes.title || 'Untitled Comment';
  const commentText = stripQuotes(item.comment) || attributes.comment || '';
  const subtype = stripQuotes(item.subtype) || attributes.subtype || '';
  
  // Submitter info
  const firstName = attributes.firstName;
  const lastName = attributes.lastName;
  const organization = attributes.organization || item.organization;
  const submitterName = firstName && lastName 
    ? `${firstName} ${lastName}` 
    : firstName || lastName || organization || 'Anonymous';
  
  // Dates
  const postedDate = attributes.postedDate || item.posted_date;
  const receiveDate = attributes.receiveDate || item.receive_date;
  const modifyDate = attributes.modifyDate || item.modify_date;
  
  // Other metadata
  const trackingNbr = attributes.trackingNbr;
  const commentOnDocumentId = attributes.commentOnDocumentId;
  const pageCount = attributes.pageCount;
  
  // Attachments
  const attachments = included.filter((i: { type: string }) => i.type === 'attachments');


  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg hover:shadow-lg transition-all duration-200">
      <div className="p-5">
        {/* Header */}
        <div className="mb-4">
          <div className="flex items-start justify-between mb-2">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex-1">
              {title}
            </h3>
            <BookmarkButton 
              isBookmarked={isBookmarked} 
              onToggle={onToggleBookmark} 
              loading={false} 
            />
            {subtype && (
              <span className="flex-shrink-0 ml-2 text-xs px-2.5 py-1 bg-green-100 dark:bg-green-900/50 text-green-800 dark:text-green-200 rounded-full font-medium">
                {subtype}
              </span>
            )}
          </div>
          
          <div className="flex flex-wrap items-center gap-3 text-sm text-gray-600 dark:text-gray-400 mb-2">
            <span className="font-mono text-xs">{commentId}</span>
            {item.docket_id && (
              <>
                <span>•</span>
                <span>Docket: {item.docket_id}</span>
              </>
            )}
            {trackingNbr && (
              <>
                <span>•</span>
                <span className="font-mono text-xs">Tracking: {trackingNbr}</span>
              </>
            )}
          </div>
          
          <div className="text-sm text-gray-600 dark:text-gray-400">
            <span className="font-medium">Submitted by:</span> {submitterName}
            {commentOnDocumentId && (
              <span className="ml-3">
                <span className="font-medium">On document:</span> {commentOnDocumentId}
              </span>
            )}
          </div>
        </div>

        {/* Comment Text */}
        {commentText && (
          <div className="mt-4 p-4 bg-gray-50 dark:bg-gray-900/50 rounded-lg border border-gray-200 dark:border-gray-700">
            <div 
              className="text-sm text-gray-700 dark:text-gray-300 leading-relaxed prose prose-sm dark:prose-invert max-w-none"
              dangerouslySetInnerHTML={{ __html: commentText }}
            />
          </div>
        )}

        {/* Attachments */}
        {attachments.length > 0 && (
          <div className="mt-4">
            <div className="text-xs font-medium text-gray-600 dark:text-gray-400 mb-2">
              Attachments ({attachments.length})
            </div>
            <div className="space-y-2">
              {attachments.map((attachment: any, idx: number) => {
                const attAttrs = attachment.attributes || {};
                const fileFormats = attAttrs.fileFormats || [];
                const attachmentTitle = attAttrs.title || `Attachment ${idx + 1}`;
                
                return (
                  <div key={attachment.id || idx} className="flex items-center gap-2 p-2 bg-gray-50 dark:bg-gray-900/50 rounded border border-gray-200 dark:border-gray-700">
                    <svg className="w-4 h-4 text-gray-500 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">
                        {attachmentTitle}
                      </div>
                      {fileFormats.length > 0 && (
                        <div className="flex gap-2 mt-1">
                          {fileFormats.map((fmt: any, fmtIdx: number) => (
                            <a
                              key={fmtIdx}
                              href={fmt.fileUrl}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="text-xs text-blue-600 dark:text-blue-400 hover:underline"
                            >
                              {fmt.format.toUpperCase()} ({Math.round(fmt.size / 1024)}KB)
                            </a>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Metadata Row */}
        <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700 flex items-center justify-between">
          <div className="flex flex-wrap items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
            {postedDate && (
              <span>Posted: {formatDate(postedDate)}</span>
            )}
            {receiveDate && (
              <span>Received: {formatDate(receiveDate)}</span>
            )}
            {modifyDate && (
              <span>Modified: {formatDate(modifyDate)}</span>
            )}
            {pageCount && (
              <span>{pageCount} {pageCount === 1 ? 'page' : 'pages'}</span>
            )}
          </div>
          <button
            onClick={() => setExpanded(!expanded)}
            className="text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 font-medium"
          >
            {expanded ? 'Show Less' : 'Show More'}
          </button>
        </div>

        {/* Expanded Details */}
        {expanded && (
          <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700 space-y-3">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
              {Object.entries(attributes)
                .filter(([key, value]) => {
                  if (!value || value === null) return false;
                  if (['comment', 'title', 'subtype', 'documentType', 'firstName', 'lastName', 'organization', 
                       'postedDate', 'receiveDate', 'modifyDate', 'trackingNbr', 'commentOnDocumentId', 
                       'pageCount'].includes(key)) return false;
                  if (Array.isArray(value) && value.length === 0) return false;
                  return true;
                })
                .map(([key, value]) => (
                  <div key={key} className="flex flex-col">
                    <span className="font-medium text-gray-600 dark:text-gray-400 text-xs mb-1">
                      {key.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase())}
                    </span>
                    <span className="text-gray-900 dark:text-gray-100 break-words">
                      {typeof value === 'object' 
                        ? JSON.stringify(value, null, 2) 
                        : String(value)}
                    </span>
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
