import { RegulationData } from '@/lib/api';
import { stripQuotes, formatDate } from './utils';

interface CommentCardProps {
  item: RegulationData;
}

function parseAttachments(json: any): { title: string; formats: { url: string; format: string; size: number }[] }[] {
  if (!json) return [];
  try {
    const parsed = typeof json === 'string' ? JSON.parse(json) : json;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function CommentCard({ item }: CommentCardProps) {
  const commentId = stripQuotes(item.comment_id) || item.docket_id;
  const title = stripQuotes(item.title) || 'Untitled Comment';
  const commentText = stripQuotes(item.comment) || '';
  const subtype = stripQuotes(item.subtype) || '';

  // Submitter info
  const organization = item.organization;
  const submitterName = organization || 'Anonymous';

  // Dates
  const postedDate = item.posted_date;
  const receiveDate = item.receive_date;
  const modifyDate = item.modify_date;

  // Attachments
  const attachments = parseAttachments(item.attachments_json);


  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg hover:shadow-lg transition-all duration-200">
      <div className="p-5">
        {/* Header */}
        <div className="mb-4">
          <div className="flex items-start justify-between mb-2">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex-1">
              {title}
            </h3>
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
          </div>

          <div className="text-sm text-gray-600 dark:text-gray-400">
            <span className="font-medium">Submitted by:</span> {submitterName}
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
              {attachments.map((attachment, idx) => (
                <div key={idx} className="flex items-center gap-2 p-2 bg-gray-50 dark:bg-gray-900/50 rounded border border-gray-200 dark:border-gray-700">
                  <svg className="w-4 h-4 text-gray-500 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">
                      {attachment.title || `Attachment ${idx + 1}`}
                    </div>
                    {attachment.formats.length > 0 && (
                      <div className="flex gap-2 mt-1">
                        {attachment.formats.map((fmt, fmtIdx) => (
                          <a
                            key={fmtIdx}
                            href={fmt.url}
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
              ))}
            </div>
          </div>
        )}

        {/* Metadata Row */}
        <div className="mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
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
          </div>
        </div>
      </div>
    </div>
  );
}
