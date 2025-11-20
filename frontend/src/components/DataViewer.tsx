'use client';

import { useEffect, useState } from 'react';
import { getRegulationData, RegulationData, DataType } from '@/lib/api';

interface DataViewerProps {
  agencyCode: string | null;
  dataType: DataType;
  docketId: string | null;
}

function stripQuotes(str: string | null | undefined): string {
  if (!str) return '';
  return str.replace(/^"|"$/g, '').trim();
}

function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return '';
  try {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { 
      year: 'numeric', 
      month: 'short', 
      day: 'numeric' 
    });
  } catch {
    return dateStr;
  }
}

function parseRawJson(rawJson: string | undefined): any {
  if (!rawJson) return null;
  try {
    return JSON.parse(rawJson);
  } catch {
    return null;
  }
}

function CommentCard({ item }: { item: RegulationData }) {
  const [expanded, setExpanded] = useState(false);
  const parsedJson = parseRawJson(item.raw_json);
  const attributes = parsedJson?.data?.attributes || {};
  const included = parsedJson?.included || [];

  const commentId = stripQuotes(item.comment_id) || attributes.id || item.docket_id;
  const title = stripQuotes(item.title) || attributes.title || 'Untitled Comment';
  const commentText = stripQuotes(item.comment) || attributes.comment || '';
  const documentType = stripQuotes(item.document_type) || attributes.documentType || '';
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
  const attachments = included.filter((item: any) => item.type === 'attachments');

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
                .slice(0, 10)
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

function DocketOrDocumentCard({ item, dataType }: { item: RegulationData; dataType: DataType }) {
  const [expanded, setExpanded] = useState(false);
  const parsedJson = parseRawJson(item.raw_json);
  const attributes = parsedJson?.data?.attributes || {};
  const relationships = parsedJson?.data?.relationships || {};
  const included = parsedJson?.included || [];

  const title = stripQuotes(item.title) || attributes.title || '';
  const docketType = stripQuotes(item.docket_type) || attributes.docketType || '';
  const documentType = attributes.documentType || '';
  const docketId = item.docket_id;
  const year = item.year;
  const agencyCode = item.agency_code;

  // Common fields
  const abstract = attributes.dkAbstract || attributes.docAbstract || '';
  const rin = attributes.rin;
  const modifyDate = attributes.modifyDate || item.modify_date;
  const keywords = attributes.keywords;
  const program = attributes.program;
  const effectiveDate = attributes.effectiveDate;

  // Document-specific fields
  const subject = attributes.subject;
  const postedDate = attributes.postedDate;
  const commentStartDate = attributes.commentStartDate;
  const commentEndDate = attributes.commentEndDate;
  const openForComment = attributes.openForComment;
  const withinCommentPeriod = attributes.withinCommentPeriod;
  const frDocNum = attributes.frDocNum;
  const frVolNum = attributes.frVolNum;
  const pageCount = attributes.pageCount;
  
  // File formats for documents
  const fileFormats = attributes.fileFormats || [];
  
  // Attachments (from relationships/included)
  const attachments = (relationships.attachments?.data || [])
    .map((attachmentRef: { id: string }) =>
      included.find((inc: any) => inc.id === attachmentRef.id && inc.type === 'attachments')
    )
    .filter(Boolean);

  const isDocument = dataType === 'documents';

  return (
    <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg hover:shadow-lg transition-all duration-200">
      <div className="p-5">
        {/* Header */}
        <div className="mb-4">
          <div className="flex items-start justify-between mb-2">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 flex-1">
              {title || docketId}
            </h3>
            {(docketType || documentType) && (
              <span className="flex-shrink-0 ml-2 text-xs px-2.5 py-1 bg-blue-100 dark:bg-blue-900/50 text-blue-800 dark:text-blue-200 rounded-full font-medium">
                {documentType || docketType}
              </span>
            )}
          </div>
          
          <div className="flex flex-wrap items-center gap-3 text-sm text-gray-600 dark:text-gray-400 mb-2">
            <span className="font-mono text-xs">{docketId}</span>
            {year && (
              <>
                <span>•</span>
                <span>{year}</span>
              </>
            )}
            {agencyCode && (
              <>
                <span>•</span>
                <span>{agencyCode}</span>
              </>
            )}
            {rin && rin !== 'Not Assigned' && rin !== 'null' && (
              <>
                <span>•</span>
                <span className="font-mono text-xs">RIN: {rin}</span>
              </>
            )}
          </div>

          {/* Document-specific: Subject and Federal Register info */}
          {isDocument && subject && (
            <div className="text-sm text-gray-700 dark:text-gray-300 mb-2">
              <span className="font-medium">Subject:</span> {subject}
            </div>
          )}
          
          {isDocument && (frDocNum || frVolNum) && (
            <div className="text-xs text-gray-600 dark:text-gray-400 mb-2">
              {frDocNum && <span>Federal Register: {frDocNum}</span>}
              {frVolNum && frDocNum && <span> • </span>}
              {frVolNum && <span>{frVolNum}</span>}
            </div>
          )}

          {/* Docket-specific: Keywords and Program */}
          {!isDocument && (keywords || program) && (
            <div className="text-sm text-gray-600 dark:text-gray-400 mb-2">
              {keywords && (
                <div>
                  <span className="font-medium">Keywords:</span> {keywords}
                </div>
              )}
              {program && (
                <div>
                  <span className="font-medium">Program:</span> {program}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Abstract/Description */}
        {abstract && (
          <div className="mt-4 p-4 bg-gray-50 dark:bg-gray-900/50 rounded-lg border border-gray-200 dark:border-gray-700">
            <p className="text-sm text-gray-700 dark:text-gray-300 leading-relaxed">
              {abstract}
            </p>
          </div>
        )}

        {/* Document-specific: Comment Period */}
        {isDocument && (commentStartDate || commentEndDate) && (
          <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
            <div className="text-sm">
              <div className="font-medium text-blue-900 dark:text-blue-100 mb-1">
                Public Comment Period
              </div>
              <div className="text-blue-700 dark:text-blue-300">
                {commentStartDate && (
                  <div>Start: {formatDate(commentStartDate)}</div>
                )}
                {commentEndDate && (
                  <div>End: {formatDate(commentEndDate)}</div>
                )}
                {openForComment !== undefined && (
                  <div className="mt-1">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                      openForComment 
                        ? 'bg-green-100 dark:bg-green-900/50 text-green-800 dark:text-green-200'
                        : 'bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-300'
                    }`}>
                      {openForComment ? 'Open for Comments' : 'Comment Period Closed'}
                    </span>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* File Formats / Attachments */}
        {(fileFormats.length > 0 || attachments.length > 0) && (
          <div className="mt-4">
            <div className="text-xs font-medium text-gray-600 dark:text-gray-400 mb-2">
              {isDocument ? 'Document Files' : 'Attachments'} ({fileFormats.length + attachments.length})
            </div>
            <div className="space-y-2">
              {fileFormats.map((file: any, idx: number) => (
                <div key={idx} className="flex items-center gap-2 p-2 bg-gray-50 dark:bg-gray-900/50 rounded border border-gray-200 dark:border-gray-700">
                  <svg className="w-4 h-4 text-gray-500 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
                  </svg>
                  <div className="flex-1 min-w-0">
                    <a
                      href={file.fileUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm text-blue-600 dark:text-blue-400 hover:underline font-medium"
                    >
                      {file.format.toUpperCase()} ({Math.round(file.size / 1024)}KB)
                    </a>
                  </div>
                </div>
              ))}
              {attachments.map((attachment: any, idx: number) => {
                const attAttrs = attachment.attributes || {};
                const attFileFormats = attAttrs.fileFormats || [];
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
                      {attFileFormats.length > 0 && (
                        <div className="flex gap-2 mt-1">
                          {attFileFormats.map((fmt: any, fmtIdx: number) => (
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
          <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-gray-500 dark:text-gray-400">
            {postedDate && (
              <span>Posted: {formatDate(postedDate)}</span>
            )}
            {modifyDate && (
              <span>Modified: {formatDate(modifyDate)}</span>
            )}
            {effectiveDate && (
              <span>Effective: {formatDate(effectiveDate)}</span>
            )}
            {pageCount !== undefined && pageCount !== null && (
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
                  // Filter out null, empty, or already displayed fields
                  if (!value || value === null) return false;
                  if (['dkAbstract', 'docAbstract', 'title', 'rin', 'modifyDate', 'docketType', 'documentType', 
                       'subject', 'postedDate', 'commentStartDate', 'commentEndDate', 'openForComment', 
                       'withinCommentPeriod', 'frDocNum', 'frVolNum', 'pageCount', 'fileFormats', 
                       'keywords', 'program', 'effectiveDate', 'agencyId', 'docketId', 'id', 'type'].includes(key)) return false;
                  if (Array.isArray(value) && value.length === 0) return false;
                  if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0) return false;
                  return true;
                })
                .slice(0, 10)
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
            {Object.keys(attributes).length > 10 && (
              <div className="text-xs text-gray-500 dark:text-gray-400 pt-2">
                +{Object.keys(attributes).length - 10} more fields available in raw JSON
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export function DataViewer({ agencyCode, dataType, docketId }: DataViewerProps) {
  const [data, setData] = useState<RegulationData[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!agencyCode) {
      setData([]);
      return;
    }

    async function loadData() {
      try {
        setLoading(true);
        setError(null);
        const result = await getRegulationData(agencyCode, dataType, docketId || undefined);
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
        setData([]);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, [agencyCode, dataType, docketId]);

  if (!agencyCode) {
    return (
      <div className="p-8 text-center text-gray-500 dark:text-gray-400">
        Select an agency to view data
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center p-12">
        <div className="flex flex-col items-center gap-3">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          <div className="text-sm text-gray-500 dark:text-gray-400">Loading data...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
        <p className="text-sm text-red-600 dark:text-red-400">Error: {error}</p>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="p-8 text-center text-gray-500 dark:text-gray-400">
        No data found
      </div>
    );
  }

  // Deduplicate based on data type
  const dataMap = new Map<string, RegulationData>();
  data.forEach(item => {
    // For comments, use comment_id; for others, use docket_id
    const key = dataType === 'comments' 
      ? (stripQuotes(item.comment_id) || item.docket_id)
      : item.docket_id;
    
    const existing = dataMap.get(key);
    if (!existing) {
      dataMap.set(key, item);
    } else {
      // Keep the one with the most recent cached_at or modify_date
      const existingDate = existing.cached_at || existing.modify_date || '';
      const currentDate = item.cached_at || item.modify_date || '';
      if (currentDate > existingDate) {
        dataMap.set(key, item);
      }
    }
  });
  const uniqueData = Array.from(dataMap.values());

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
            {dataType.charAt(0).toUpperCase() + dataType.slice(1)}
          </h3>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            {uniqueData.length} {uniqueData.length === 1 ? 'result' : 'results'}
            {docketId && ` for ${docketId}`}
          </p>
        </div>
      </div>
      <div className="space-y-4 max-h-[70vh] overflow-y-auto pr-2">
        {uniqueData.map((item, index) => {
          const key = dataType === 'comments' 
            ? (stripQuotes(item.comment_id) || item.docket_id)
            : item.docket_id;
          
          return dataType === 'comments' ? (
            <CommentCard key={`${key}-${index}`} item={item} />
          ) : (
            <DocketOrDocumentCard key={`${key}-${index}`} item={item} dataType={dataType} />
          );
        })}
      </div>
    </div>
  );
}
