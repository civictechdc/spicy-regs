'use client';

import { useEffect, useState } from 'react';
import { RegulationData } from '@/lib/api';
import { CommentCard } from '@/components/data-viewer/CommentCard';
import { DocketOrDocumentCard } from '@/components/data-viewer/DocketOrDocumentCard';
import { Header } from '@/components/Header';

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

export default function BookmarksPage() {
  const [bookmarkIds, setBookmarkIds] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setBookmarkIds(getStoredBookmarks());
    setLoading(false);
  }, []);

  const handleToggleBookmark = (resourceId: string) => {
    const newBookmarks = new Set(bookmarkIds);
    if (bookmarkIds.has(resourceId)) {
      newBookmarks.delete(resourceId);
    } else {
      newBookmarks.add(resourceId);
    }
    setBookmarkIds(newBookmarks);
    saveBookmarks(newBookmarks);
  };

  const bookmarkArray = [...bookmarkIds];

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <Header />
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
            My Bookmarks
          </h1>
        </div>

            {loading ? (
                <div className="flex justify-center p-12">
                     <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
            ) : bookmarkArray.length === 0 ? (
                <div className="text-center py-12 text-gray-500 dark:text-gray-400 bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                    <p className="text-lg">No bookmarks yet.</p>
                    <p className="mt-2">Go exploring and save some regulations!</p>
                </div>
            ) : (
                <div className="space-y-4">
                    <p className="text-gray-600 dark:text-gray-400">
                        You have {bookmarkArray.length} bookmarked item{bookmarkArray.length !== 1 ? 's' : ''}.
                    </p>
                    <p className="text-sm text-gray-500 dark:text-gray-500">
                        Note: Bookmarks are stored locally. To see full details, search for these IDs in the dashboard.
                    </p>
                    <div className="space-y-2">
                        {bookmarkArray.map(resourceId => (
                            <div 
                                key={resourceId} 
                                className="flex items-center justify-between p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700"
                            >
                                <span className="font-mono text-sm text-gray-900 dark:text-gray-100">
                                    {resourceId}
                                </span>
                                <button
                                    onClick={() => handleToggleBookmark(resourceId)}
                                    className="text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 text-sm font-medium"
                                >
                                    Remove
                                </button>
                            </div>
                        ))}
                    </div>
                </div>
            )}
      </main>
    </div>
  );
}
