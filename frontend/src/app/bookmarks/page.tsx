'use client';

import { useEffect, useState } from 'react';
import { RegulationData } from '@/lib/api';
import { CommentCard } from '@/components/data-viewer/CommentCard';
import { DocketOrDocumentCard } from '@/components/data-viewer/DocketOrDocumentCard';
import { SignOutButton } from '@/components/SignOutButton';
import Link from 'next/link';

export default function BookmarksPage() {
  const [bookmarks, setBookmarks] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  async function fetchBookmarks() {
    try {
      setLoading(true);
      const res = await fetch('/api/bookmarks');
      if (res.ok) {
        const json = await res.json();
        setBookmarks(json.bookmarks);
      }
    } catch (e) {
      console.error("Failed to fetch bookmarks", e);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchBookmarks();
  }, []);

  const handleToggleBookmark = async (bookmark: any) => {
    // Optimistic remove
    setBookmarks(bookmarks.filter(b => b.id !== bookmark.id));

    try {
      await fetch(`/api/bookmarks?id=${bookmark.id}`, { method: 'DELETE' });
    } catch (e) {
      console.error("Failed to delete bookmark", e);
      // Revert if needed, but for MVP just console error
      fetchBookmarks();
    }
  };

  return (
    <main className="min-h-screen bg-gray-50 dark:bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
            <div className="flex justify-between items-center mb-8">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100">
                        My Bookmarks
                    </h1>
                    <div className="mt-2">
                         <Link href="/" className="text-blue-600 dark:text-blue-400 hover:underline">
                            &larr; Back to Dashboard
                         </Link>
                    </div>
                </div>
                <SignOutButton />
            </div>

            {loading ? (
                <div className="flex justify-center p-12">
                     <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
            ) : bookmarks.length === 0 ? (
                <div className="text-center py-12 text-gray-500 dark:text-gray-400 bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                    <p className="text-lg">No bookmarks yet.</p>
                    <p className="mt-2">Go exploring and save some regulations!</p>
                </div>
            ) : (
                <div className="space-y-4">
                    {bookmarks.map(bookmark => {
                        // Reconstruct RegulationData from bookmark metadata
                        // We need to map bookmark fields to RegulationData fields expected by the cards
                        // This is an approximation as we defined in the API route metadata
                        const item: RegulationData = {
                            agency_code: bookmark.agency_code,
                            docket_id: bookmark.metadata?.docket_id || bookmark.resource_id, // Fallback
                            year: bookmark.metadata?.year || '',
                            raw_json: JSON.stringify({
                                data: {
                                    attributes: {
                                        title: bookmark.title,
                                        ...bookmark.metadata
                                        // We might be missing full details here if we only saved title/id.
                                        // For MVP we display what we have.
                                        // Ideally we would fetch fresh data from API using ID.
                                        // But S3 doesn't support easy "get by ID" without known path? 
                                        // Actually api.ts `getRegulationData` takes docketId.
                                        // But that lists CONTENTS of a docket, not the docket metadata itself?
                                        // Wait, `getRegulationData` with docketId returns items IN that docket.
                                        // If we bookmarked a DOCKET, we want to show the docket card.
                                        // If we bookmarked a DOCUMENT, we want to show document card.
                                    }
                                }
                            }),
                            title: bookmark.title,
                            // Map other fields
                            ...bookmark.metadata
                        };

                        const isComment = bookmark.resource_type === 'comment';
                        
                        return (
                            <div key={bookmark.id}>
                                {isComment ? (
                                    <CommentCard 
                                        item={item} 
                                        isBookmarked={true} 
                                        onToggleBookmark={() => handleToggleBookmark(bookmark)} 
                                    />
                                ) : (
                                    <DocketOrDocumentCard 
                                        item={item} 
                                        dataType={bookmark.resource_type === 'docket' ? 'dockets' : 'documents'} 
                                        isBookmarked={true} 
                                        onToggleBookmark={() => handleToggleBookmark(bookmark)} 
                                    />
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    </main>
  );
}
