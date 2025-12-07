import { createClient } from '@/utils/supabase/client';

interface BookmarkButtonProps {
  isBookmarked: boolean;
  onToggle: () => void;
  loading: boolean;
}

export function BookmarkButton({ 
  isBookmarked, 
  onToggle, 
  loading 
}: BookmarkButtonProps) {
  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        onToggle();
      }}
      disabled={loading}
      className={`p-2 rounded-full transition-colors ${
        isBookmarked 
          ? 'text-yellow-500 hover:bg-yellow-100 dark:hover:bg-yellow-900/30' 
          : 'text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700'
      }`}
      title={isBookmarked ? "Remove bookmark" : "Bookmark this item"}
    >
      <svg 
        xmlns="http://www.w3.org/2000/svg" 
        viewBox="0 0 24 24" 
        fill={isBookmarked ? "currentColor" : "none"} 
        stroke="currentColor" 
        strokeWidth="2" 
        strokeLinecap="round" 
        strokeLinejoin="round" 
        className="w-5 h-5"
      >
        <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"></path>
      </svg>
    </button>
  );
}
