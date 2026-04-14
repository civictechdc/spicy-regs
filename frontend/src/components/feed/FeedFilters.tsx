'use client';

import { ChevronDown } from 'lucide-react';
import {
  DATE_OPTIONS,
  DOCKET_TYPE_OPTIONS,
  SORT_CHIP_OPTIONS,
  STATUS_OPTIONS,
  type DateRange,
  type DocketType,
  type SortOption,
  type StatusOption,
  type TopicKey,
} from '@/lib/feedFilters';
import { TopicFilter } from './TopicFilter';

/**
 * Map UI Status selection to the underlying SortOption.
 * 'all' returns the chip-driven sort; 'open'/'closed' override it with the
 * status-specific ordering (deadline-asc / closed-desc respectively).
 */
function statusToSort(status: StatusOption, chipSort: SortOption): SortOption {
  if (status === 'open' || status === 'closed') return status;
  // When status is 'all', fall back to whatever sort chip is active —
  // but never to 'open'/'closed', since those mean "status is set."
  return chipSort === 'open' || chipSort === 'closed' ? 'recent' : chipSort;
}

function sortToStatus(sort: SortOption): StatusOption {
  return sort === 'open' || sort === 'closed' ? sort : 'all';
}

interface FeedFiltersProps {
  selectedAgency: string;
  onAgencyChange: (agency: string) => void;
  sortBy: SortOption;
  onSortChange: (sort: SortOption) => void;
  dateRange: DateRange;
  onDateRangeChange: (range: DateRange) => void;
  docketType: DocketType;
  onDocketTypeChange: (type: DocketType) => void;
  topic?: TopicKey;
  onTopicChange?: (topic: TopicKey) => void;
}

export function FeedFilters({
  sortBy,
  onSortChange,
  dateRange,
  onDateRangeChange,
  docketType,
  onDocketTypeChange,
  topic,
  onTopicChange,
}: FeedFiltersProps) {
  const showTopics = topic !== undefined && onTopicChange !== undefined;
  return (
    <div className="flex flex-col gap-3">
      {/* Row 1: dropdowns */}
      <div className="flex items-center gap-3 flex-wrap">
      {showTopics && <TopicFilter topic={topic} onTopicChange={onTopicChange} />}
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

      {/* Docket Type Dropdown */}
      <div className="relative">
        <div className="flex items-center gap-1.5 px-1.5">
          <ChevronDown size={14} className="text-[var(--muted)] pointer-events-none absolute right-2" />
          <select
            value={docketType}
            onChange={e => onDocketTypeChange(e.target.value as DocketType)}
            className="filter-chip appearance-none pr-7 cursor-pointer bg-[var(--surface)] text-[var(--foreground)] border-none focus:outline-none"
          >
            {DOCKET_TYPE_OPTIONS.map(opt => (
              <option key={opt.key} value={opt.key}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Status Dropdown — owns the comment-period filter (Open / Recently Closed) */}
      <div className="relative">
        <div className="flex items-center gap-1.5 px-1.5">
          <ChevronDown size={14} className="text-[var(--muted)] pointer-events-none absolute right-2" />
          <select
            value={sortToStatus(sortBy)}
            onChange={e => onSortChange(statusToSort(e.target.value as StatusOption, sortBy))}
            className="filter-chip appearance-none pr-7 cursor-pointer bg-[var(--surface)] text-[var(--foreground)] border-none focus:outline-none"
            aria-label="Filter by comment period status"
          >
            {STATUS_OPTIONS.map(opt => (
              <option key={opt.key} value={opt.key}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      </div>

      {/* Row 2: Sort Chips — only New / Popular; status dropdown handles Open / Closed */}
      <div className="flex items-center gap-1.5">
        {SORT_CHIP_OPTIONS.map(opt => (
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
