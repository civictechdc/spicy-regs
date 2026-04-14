'use client';

import { ChevronDown } from 'lucide-react';
import {
  DATE_OPTIONS,
  SORT_OPTIONS,
  type DateRange,
  type SortOption,
} from '@/lib/feedFilters';

interface FeedFiltersProps {
  selectedAgency: string;
  onAgencyChange: (agency: string) => void;
  sortBy: SortOption;
  onSortChange: (sort: SortOption) => void;
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
}

export function FeedFilters({
  sortBy,
  onSortChange,
  dateRange,
  onDateRangeChange,
}: FeedFiltersProps) {
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
            {DATE_OPTIONS.map(opt => (
              <option key={opt.key} value={opt.key}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Sort Chips */}
      <div className="flex items-center gap-1.5">
        {SORT_OPTIONS.map(opt => (
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
