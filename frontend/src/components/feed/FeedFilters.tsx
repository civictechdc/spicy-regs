'use client';

import { useEffect } from 'react';
import { ChevronDown } from 'lucide-react';

type SortOption = 'recent' | 'popular' | 'open' | 'closed';
type DateRange = '' | '7d' | '30d' | '90d' | '365d';
type DocketType = '' | 'rule' | 'nonrule' | 'other';

interface FeedFiltersProps {
  selectedAgency: string;
  onAgencyChange: (agency: string) => void;
  sortBy: SortOption;
  onSortChange: (sort: SortOption) => void;
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
  docketType: DocketType;
  onDocketTypeChange: (type: DocketType) => void;
}

const PREFS_SORT_KEY = 'spicy-regs-sort-preference';
const PREFS_DATE_KEY = 'spicy-regs-date-preference';
const PREFS_TYPE_KEY = 'spicy-regs-type-preference';

const dateOptions: { key: DateRange; label: string }[] = [
  { key: '', label: 'All Time' },
  { key: '7d', label: 'Last 7 Days' },
  { key: '30d', label: 'Last Month' },
  { key: '90d', label: 'Last 3 Months' },
  { key: '365d', label: 'Last Year' },
];

const typeOptions: { key: DocketType; label: string }[] = [
  { key: '', label: 'All Types' },
  { key: 'rule', label: 'Rulemaking' },
  { key: 'nonrule', label: 'Non-Rulemaking' },
  { key: 'other', label: 'Other' },
];

export function FeedFilters({
  selectedAgency,
  onAgencyChange,
  sortBy,
  onSortChange,
  dateRange,
  onDateRangeChange,
  docketType,
  onDocketTypeChange,
}: FeedFiltersProps) {
  // Persist preferences
  useEffect(() => {
    try { localStorage.setItem(PREFS_SORT_KEY, sortBy); } catch {}
  }, [sortBy]);

  useEffect(() => {
    try { localStorage.setItem(PREFS_DATE_KEY, dateRange); } catch {}
  }, [dateRange]);

  useEffect(() => {
    try { localStorage.setItem(PREFS_TYPE_KEY, docketType); } catch {}
  }, [docketType]);

  const sortOptions: { key: SortOption; label: string }[] = [
    { key: 'recent', label: 'New' },
    { key: 'popular', label: 'Popular' },
    { key: 'open', label: 'Open for Comment' },
    { key: 'closed', label: 'Recently Closed' },
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

      {/* Docket Type Dropdown */}
      <div className="relative">
        <div className="flex items-center gap-1.5 px-1.5">
          <ChevronDown size={14} className="text-[var(--muted)] pointer-events-none absolute right-2" />
          <select
            value={docketType}
            onChange={e => onDocketTypeChange(e.target.value as DocketType)}
            className="filter-chip appearance-none pr-7 cursor-pointer bg-[var(--surface)] text-[var(--foreground)] border-none focus:outline-none"
          >
            {typeOptions.map(opt => (
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
