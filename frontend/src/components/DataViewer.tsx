'use client';

import { useEffect, useState } from 'react';
import { getRegulationData, RegulationData, DataType } from '@/lib/api';
import { CommentCard } from './data-viewer/CommentCard';
import { DocketOrDocumentCard } from './data-viewer/DocketOrDocumentCard';
import { stripQuotes } from './data-viewer/utils';

interface DataViewerProps {
  agencyCode: string | null;
  dataType: DataType;
  docketId: string | null;
}

export function DataViewer({ agencyCode, dataType, docketId }: DataViewerProps) {
  const [data, setData] = useState<RegulationData[]>([]);
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch bookmarks
  useEffect(() => {
    async function fetchBookmarks() {
      try {
        const res = await fetch('/api/bookmarks');
        if (res.ok) {
          const json = await res.json();
          // Assuming json.bookmarks is array of objects with resource_id
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
        // Delete
        await fetch(`/api/bookmarks?resource_id=${resourceId}`, { method: 'DELETE' });
      } else {
        // Add
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
      // Revert on error
      setBookmarks(bookmarks);
    }
  };

  useEffect(() => {
    if (!agencyCode) {
      setData([]);
      return;
    }

    async function loadData() {
      if (!agencyCode) return;
      try {
        setLoading(true);
        setError(null);
        const result = await getRegulationData(agencyCode as string, dataType, docketId || undefined);
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
          
          const isBookmarked = bookmarks.has(key);

          return dataType === 'comments' ? (
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
          );
        })}
      </div>
    </div>
  );
}
