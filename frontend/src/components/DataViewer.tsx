'use client';

import { useEffect, useState, useMemo, useCallback } from 'react';
import { RegulationData, DataType } from '@/lib/api';
import { CommentCard } from './data-viewer/CommentCard';
import { DocketOrDocumentCard } from './data-viewer/DocketOrDocumentCard';
import { stripQuotes } from './data-viewer/utils';
import { useMotherDuckService } from '@/lib/motherduck/hooks/useMotherDuckService';
import { RegulationsDataTypes } from '@/lib/db/models';
import { Virtuoso } from 'react-virtuoso';

interface DataViewerProps {
  agencyCode: string | null;
  dataType: DataType;
  docketId: string | null;
}

const PAGE_SIZE = 20;

export function DataViewer({ agencyCode, dataType, docketId }: DataViewerProps) {
  const [data, setData] = useState<RegulationData[]>([]);
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [totalCount, setTotalCount] = useState(0);
  const [offset, setOffset] = useState(0);
  
  const { getData, getDataCount } = useMotherDuckService();

  // Fetch bookmarks
  useEffect(() => {
    async function fetchBookmarks() {
      try {
        const res = await fetch('/api/bookmarks');
        if (res.ok) {
          const json = await res.json();
          const ids = new Set<string>(json.bookmarks.map((b: any) => b.resource_id));
          setBookmarks(ids);
        }
      } catch (e) {
        console.error("Failed to fetch bookmarks", e);
      }
    }
    fetchBookmarks();
  }, []);

  const handleToggleBookmark = async (item: RegulationData) => {
    const resourceId = dataType === 'comments' 
      ? (stripQuotes(item.comment_id) || item.docket_id)
      : item.docket_id;

    if (!resourceId) return;

    const isBookmarked = bookmarks.has(resourceId);
    
    // Optimistic update
    const newBookmarks = new Set(bookmarks);
    if (isBookmarked) {
      newBookmarks.delete(resourceId);
    } else {
      newBookmarks.add(resourceId);
    }
    setBookmarks(newBookmarks);

    try {
      if (isBookmarked) {
        await fetch(`/api/bookmarks?resource_id=${resourceId}`, { method: 'DELETE' });
      } else {
        const title = stripQuotes(item.title) || item.docket_id;
        await fetch('/api/bookmarks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            resource_id: resourceId,
            resource_type: dataType === 'dockets' ? 'docket' : (dataType === 'documents' ? 'document' : 'comment'),
            agency_code: agencyCode,
            title: title,
            metadata: {
                docket_id: item.docket_id,
                year: item.year
            }
          })
        });
      }
    } catch (e) {
      console.error("Failed to toggle bookmark", e);
      setBookmarks(bookmarks);
    }
  };

  // Reset data when filters change
  useEffect(() => {
    setData([]);
    setOffset(0);
    setTotalCount(0);
  }, [agencyCode, dataType, docketId]);

  // Initial load and load more
  useEffect(() => {
    if (!agencyCode) {
      setData([]);
      return;
    }

    async function loadData() {
      if (!agencyCode) return;
      
      const isInitialLoad = offset === 0;
      
      try {
        if (isInitialLoad) {
          setLoading(true);
        } else {
          setLoadingMore(true);
        }
        setError(null);
        
        // Fetch count on initial load
        if (isInitialLoad) {
          const count = await getDataCount(
            dataType as RegulationsDataTypes,
            agencyCode,
            docketId || undefined
          );
          setTotalCount(count);
        }
        
        const result = await getData(
          dataType as RegulationsDataTypes,
          agencyCode,
          docketId || undefined,
          Infinity,
          PAGE_SIZE,
          offset
        );
        
        if (isInitialLoad) {
          setData(result);
        } else {
          setData(prev => [...prev, ...result]);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
        if (offset === 0) {
          setData([]);
        }
      } finally {
        setLoading(false);
        setLoadingMore(false);
      }
    }
    loadData();
  }, [agencyCode, dataType, docketId, getData, getDataCount, offset]);

  const loadMore = useCallback(() => {
    if (!loadingMore && data.length < totalCount) {
      setOffset(prev => prev + PAGE_SIZE);
    }
  }, [loadingMore, data.length, totalCount]);

  // Deduplicate based on data type
  const uniqueData = useMemo(() => {
    const dataMap = new Map<string, RegulationData>();
    data.forEach(item => {
      const key = dataType === 'comments' 
        ? (stripQuotes(item.comment_id) || item.docket_id)
        : item.docket_id;
      
      const existing = dataMap.get(key);
      if (!existing) {
        dataMap.set(key, item);
      } else {
        const existingDate = existing.cached_at || existing.modify_date || '';
        const currentDate = item.cached_at || item.modify_date || '';
        if (currentDate > existingDate) {
          dataMap.set(key, item);
        }
      }
    });
    return Array.from(dataMap.values());
  }, [data, dataType]);

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

  if (uniqueData.length === 0) {
    return (
      <div className="p-8 text-center text-gray-500 dark:text-gray-400">
        No data found
      </div>
    );
  }

  const hasMore = uniqueData.length < totalCount;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
            {dataType.charAt(0).toUpperCase() + dataType.slice(1)}
          </h3>
          <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Showing {uniqueData.length} of {totalCount} {totalCount === 1 ? 'result' : 'results'}
            {docketId && ` for ${docketId}`}
          </p>
        </div>
      </div>
      
      <div style={{ height: '70vh' }}>
        <Virtuoso
          style={{ height: '100%' }}
          data={uniqueData}
          endReached={loadMore}
          overscan={200}
          itemContent={(index, item) => {
            const key = dataType === 'comments' 
              ? (stripQuotes(item.comment_id) || item.docket_id)
              : item.docket_id;
            
            const isBookmarked = bookmarks.has(key);

            return (
              <div className="pb-4">
                {dataType === 'comments' ? (
                  <CommentCard 
                      key={`${key}-${index}`} 
                      item={item} 
                      isBookmarked={isBookmarked}
                      onToggleBookmark={() => handleToggleBookmark(item)}
                  />
                ) : (
                  <DocketOrDocumentCard 
                      key={`${key}-${index}`} 
                      item={item} 
                      dataType={dataType}
                      isBookmarked={isBookmarked}
                      onToggleBookmark={() => handleToggleBookmark(item)}
                  />
                )}
              </div>
            );
          }}
          components={{
            Footer: () => {
              if (loadingMore) {
                return (
                  <div className="flex justify-center py-4">
                    <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
                  </div>
                );
              }
              if (hasMore) {
                return (
                  <div className="flex justify-center py-4">
                    <button 
                      onClick={loadMore}
                      className="px-4 py-2 text-sm font-medium text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                    >
                      Load more...
                    </button>
                  </div>
                );
              }
              return null;
            }
          }}
        />
      </div>
    </div>
  );
}
