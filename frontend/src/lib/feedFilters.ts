// frontend/src/lib/feedFilters.ts

export type SortOption = 'recent' | 'popular' | 'open';
export type DateRange = '' | '7d' | '30d' | '90d' | '365d';

export const SORT_STORAGE_KEY = 'spicy-regs-sort-preference';
export const DATE_STORAGE_KEY = 'spicy-regs-date-preference';

export const DEFAULT_SORT: SortOption = 'recent';
export const DEFAULT_DATE: DateRange = '';

const SORT_VALUES: readonly SortOption[] = ['recent', 'popular', 'open'];
const DATE_VALUES: readonly DateRange[] = ['', '7d', '30d', '90d', '365d'];

export function isSortOption(raw: string): raw is SortOption {
  return (SORT_VALUES as readonly string[]).includes(raw);
}

export function isDateRange(raw: string): raw is DateRange {
  return (DATE_VALUES as readonly string[]).includes(raw);
}

export const SORT_OPTIONS: { key: SortOption; label: string }[] = [
  { key: 'recent', label: 'New' },
  { key: 'popular', label: 'Popular' },
  { key: 'open', label: 'Open for Comment' },
];

export const DATE_OPTIONS: { key: DateRange; label: string }[] = [
  { key: '', label: 'All Time' },
  { key: '7d', label: 'Last 7 Days' },
  { key: '30d', label: 'Last Month' },
  { key: '90d', label: 'Last 3 Months' },
  { key: '365d', label: 'Last Year' },
];
