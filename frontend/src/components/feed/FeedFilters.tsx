'use client';

import { useEffect } from 'react';
import { ChevronDown } from 'lucide-react';

type SortOption = 'recent' | 'popular' | 'open';
type DateRange = '' | '7d' | '30d' | '90d' | '365d';

interface FeedFiltersProps {
  selectedAgency: string;
  onAgencyChange: (agency: string) => void;
  sortBy: SortOption;
  onSortChange: (sort: SortOption) => void;
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
}

const PREFS_SORT_KEY = 'spicy-regs-sort-preference';
const PREFS_DATE_KEY = 'spicy-regs-date-preference';

const dateOptions: { key: DateRange; label: string }[] = [
  { key: '', label: 'All Time' },
  { key: '7d', label: 'Last 7 Days' },
  { key: '30d', label: 'Last Month' },
  { key: '90d', label: 'Last 3 Months' },
  { key: '365d', label: 'Last Year' },
];

export function FeedFilters({
  selectedAgency,
  onAgencyChange,
  sortBy,
  onSortChange,
  dateRange,
  onDateRangeChange,
}: FeedFiltersProps) {
  // Persist preferences
  useEffect(() => {
    try { localStorage.setItem(PREFS_SORT_KEY, sortBy); } catch {}
  }, [sortBy]);

  useEffect(() => {
    try { localStorage.setItem(PREFS_DATE_KEY, dateRange); } catch {}
  }, [dateRange]);

  const sortOptions: { key: SortOption; label: string }[] = [
    { key: 'recent', label: 'New' },
    { key: 'popular', label: 'Popular' },
    { key: 'open', label: 'Open for Comment' },
  ];

  return (
    <div className="flex items-center gap-3 flex-wrap">
      {/* Date Range Dropdown */}
      <div className="relative">
        <div className="flex items-center gap-1.5 px-1.5">
          <ChevronDown size={14} className="text-[var(--muted)] pointer-events-none absolute right-2" />
          <select
            value={dateRange}
            onChange={e => onDateRangeChange(e.target.value as DateRange)}
            className="filter-chip appearance-none pr-7 cursor-pointer bg-[var(--surface)] text-[var(--foreground)] border-none focus:outline-none"
          >
            {dateOptions.map(opt => (
              <option key={opt.key} value={opt.key}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Sort Chips */}
      <div className="flex items-center gap-1.5">
        {sortOptions.map(opt => (
          <button
            key={opt.key}
            onClick={() => onSortChange(opt.key)}
            className={`filter-chip ${sortBy === opt.key ? 'filter-chip-active' : ''}`}
          >
            {opt.label}
          </button>
        ))}
      </div>
    </div>
  );
}
