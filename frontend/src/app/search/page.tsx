'use client';

import { useEffect, useState, Suspense } from 'react';
import { useSearchParams } from 'next/navigation';
import { RegulationData } from '@/lib/api';
import { CommentCard } from '@/components/data-viewer/CommentCard';
import { DocketOrDocumentCard } from '@/components/data-viewer/DocketOrDocumentCard';
import { SearchBar } from '@/components/SearchBar';
import { Header } from '@/components/Header';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';

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
  } catch (e) {
    console.error('Failed to save bookmarks', e);
  }
}

function SearchResults() {
  const searchParams = useSearchParams();
  const query = searchParams.get('q');
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  
  const { searchResources } = useDuckDBService();

  useEffect(() => {
    async function performSearch() {
      if (!query) return;
      
      try {
        setLoading(true);
        const searchResults = await searchResources(query);
        setResults(searchResults);
      } catch (e) {
        console.error("Search failed", e);
      } finally {
        setLoading(false);
      }
    }
    performSearch();
  }, [query, searchResources]);

  const [bookmarks, setBookmarks] = useState<Set<string>>(new Set());
  
  useEffect(() => {
    setBookmarks(getStoredBookmarks());
  }, []);

  const handleToggleBookmark = (item: any) => {
    let resourceId = item.docket_id;
    if (item.type === 'comments') {
      try {
        const parsed = typeof item.raw_json === 'string' ? JSON.parse(item.raw_json) : item.raw_json;
        resourceId = parsed.data.id;
      } catch {}
    }

    if (!resourceId) return;

    const newBookmarks = new Set(bookmarks);
    if (bookmarks.has(resourceId)) {
      newBookmarks.delete(resourceId);
    } else {
      newBookmarks.add(resourceId);
    }
    setBookmarks(newBookmarks);
    saveBookmarks(newBookmarks);
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
           const isComment = result.type === 'comments';
           let key = result.docket_id;
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
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <Header />
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-6">
          <SearchBar />
        </div>

        <Suspense fallback={<div>Loading search...</div>}>
          <SearchResults />
        </Suspense>
      </main>
    </div>
  );
}
