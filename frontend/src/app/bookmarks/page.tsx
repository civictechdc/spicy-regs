'use client';

import { useEffect, useState } from 'react';
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
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Hero */}
        <div className="text-center py-8 mb-8">
          <h1 className="text-4xl font-bold mb-4">
            <span className="gradient-text">My Bookmarks</span>
          </h1>
          <p className="text-[var(--muted)] text-lg">
            Your saved regulations and documents
          </p>
        </div>

        {loading ? (
          <div className="flex justify-center p-12">
            <div className="animate-spin h-8 w-8 border-2 border-[var(--accent-primary)] border-t-transparent rounded-full" />
          </div>
        ) : bookmarkArray.length === 0 ? (
          <div className="card-gradient p-12 text-center">
            <p className="text-lg text-[var(--foreground)]">No bookmarks yet.</p>
            <p className="mt-2 text-[var(--muted)]">Go exploring and save some regulations!</p>
          </div>
        ) : (
          <div className="space-y-4">
            <p className="text-[var(--muted)]">
              You have {bookmarkArray.length} bookmarked item{bookmarkArray.length !== 1 ? 's' : ''}.
            </p>
            <div className="space-y-2">
              {bookmarkArray.map(resourceId => (
                <div 
                  key={resourceId} 
                  className="card-gradient flex items-center justify-between p-4"
                >
                  <span className="font-mono text-sm">
                    {resourceId}
                  </span>
                  <button
                    onClick={() => handleToggleBookmark(resourceId)}
                    className="text-red-400 hover:text-red-300 text-sm font-medium transition-colors"
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
