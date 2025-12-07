'use client';

import { useEffect, useState,  Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { RegulationData } from '@/lib/api';
import { CommentCard, DocketOrDocumentCard } from '@/components/DataViewer';
import { SearchBar } from '@/components/SearchBar';
import { SignOutButton } from '@/components/SignOutButton';
import Link from 'next/link';

function SearchResults() {
  const searchParams = useSearchParams();
  const query = searchParams.get('q');
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    async function performSearch() {
      if (!query) return;
      
      try {
        setLoading(true);
        const res = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
        if (res.ok) {
          const json = await res.json();
          setResults(json.results);
        }
      } catch (e) {
        console.error("Search failed", e);
      } finally {
        setLoading(false);
      }
    }
    performSearch();
  }, [query]);

  // Bookmarks logic for toggle (same as DataViewer, should refactor later)
  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  
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

  const handleToggleBookmark = async (item: any) => {
    // Determine ID based on item structure returned by search (which is raw_json + metadata columns)
    // Search returns: type, title, docket_id, agency_code, year, raw_json
    // We need to parse raw_json to get full ID if needed, or use docket_id
    // But wait, the search API returns `type` column.
    
    // Logic from DataViewer:
    // const resourceId = dataType === 'comments' ? (stripQuotes(item.comment_id) || item.docket_id) : item.docket_id;
    // But `item` here comes from search cache columns.
    // The `raw_json` has the full original data.
    
    // Let's parse raw_json to get the exact object RegulationData expects
    const raw = JSON.parse(item.raw_json); // It's a string in the DB result?
    // Wait, DuckDB `json_extract` returns string? or json val? 
    // In `clients.ts`: `getRowObjectsJS()` likely returns objects/strings depending on driver.
    // `raw_json` column in cache is TEXT/JSON.
    
    // In `searchResources`, we select `raw_json`.
    // Let's assume `item.raw_json` is the JSON string or object.
    
    // We can reconstruct the `RegulationData` object
    const regulationData: RegulationData = {
        ...item,
        // search returns flat columns: title, docket_id, agency_code, year, type
        // and raw_json
    };
    
    // However, the `DocketOrDocumentCard` expects `item` to have `raw_json` as string to parse it internally.
    // And `docket_id` etc.
    // So passing `item` directly might work if keys match.
    // `item` from search has: type, title, docket_id, agency_code, year, raw_json
    
    // We need to identify ID for bookmarking.
    let resourceId = item.docket_id;
    if (item.type === 'comments') {
        // Need to extract comment ID
        // It's in raw_json.data.id
         const parsed = typeof item.raw_json === 'string' ? JSON.parse(item.raw_json) : item.raw_json;
         resourceId = parsed.data.id;
    }

    if (!resourceId) return;

    const isBookmarked = bookmarks.has(resourceId);
    
    // Optimistic
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
        await fetch('/api/bookmarks', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            resource_id: resourceId,
            resource_type: item.type === 'dockets' ? 'docket' : (item.type === 'documents' ? 'document' : 'comment'),
            agency_code: item.agency_code,
            title: item.title,
            metadata: {
                docket_id: item.docket_id,
                year: item.year
            }
          })
        });
      }
    } catch (e) {
      console.error("Bookmark toggle failed", e);
      setBookmarks(bookmarks);
    }
  };

  if (!query) {
    return (
      <div className="text-center py-12 text-gray-500 dark:text-gray-400">
        Enter a search term to find regulations.
      </div>
    );
  }

  if (loading) {
     return (
        <div className="flex justify-center p-12">
             <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        </div>
    );
  }

  if (results.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500 dark:text-gray-400">
        No results found for "{query}". <br/>
        Try indexing an agency first by visiting its page.
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
        Results for "{query}"
      </h2>
      <div className="space-y-4">
        {results.map((result, idx) => {
           // We need to determine if it's comment or docket/doc
           const isComment = result.type === 'comments';
           // Determine Key
           let key = result.docket_id; // fallback
           try {
              if (isComment) {
                 const parsed = typeof result.raw_json === 'string' ? JSON.parse(result.raw_json) : result.raw_json;
                 key = parsed.data.id; 
              }
           } catch {}
           
           const isBookmarked = bookmarks.has(key);

           return (
             <div key={`${key}-${idx}`}>
               {isComment ? (
                 <CommentCard 
                    item={result} 
                    isBookmarked={isBookmarked} 
                    onToggleBookmark={() => handleToggleBookmark(result)} 
                 />
               ) : (
                 <DocketOrDocumentCard 
                    item={result} 
                    dataType={result.type === 'dockets' ? 'dockets' : 'documents'} 
                    isBookmarked={isBookmarked} 
                    onToggleBookmark={() => handleToggleBookmark(result)} 
                 />
               )}
             </div>
           );
        })}
      </div>
    </div>
  );
}

export default function SearchPage() {
  return (
    <main className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="flex justify-between items-center mb-8">
            <div className="flex items-center gap-4 flex-1">
                <Link href="/" className="text-2xl font-bold text-gray-900 dark:text-gray-100 shrink-0">
                    Spicy Regs
                </Link>
                <div className="flex-1 max-w-2xl">
                    <SearchBar />
                </div>
            </div>
            <div className="flex items-center gap-4">
                 <Link href="/bookmarks" className="text-gray-600 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400 font-medium">
                    Bookmarks
                 </Link>
                 <SignOutButton />
            </div>
        </div>

        <Suspense fallback={<div>Loading search...</div>}>
            <SearchResults />
        </Suspense>
      </div>
    </main>
  );
}
