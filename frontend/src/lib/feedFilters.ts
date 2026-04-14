// frontend/src/lib/feedFilters.ts

export type SortOption = 'recent' | 'popular' | 'open' | 'closed';
export type DateRange = '' | '7d' | '30d' | '90d' | '365d';
export type DocketType = '' | 'rule' | 'nonrule' | 'other';

export const SORT_STORAGE_KEY = 'spicy-regs-sort-preference';
export const DATE_STORAGE_KEY = 'spicy-regs-date-preference';
export const TYPE_STORAGE_KEY = 'spicy-regs-type-preference';

export const DEFAULT_SORT: SortOption = 'recent';
export const DEFAULT_DATE: DateRange = '';
export const DEFAULT_TYPE: DocketType = '';

const SORT_VALUES: readonly SortOption[] = ['recent', 'popular', 'open', 'closed'];
const DATE_VALUES: readonly DateRange[] = ['', '7d', '30d', '90d', '365d'];
const TYPE_VALUES: readonly DocketType[] = ['', 'rule', 'nonrule', 'other'];

export function isSortOption(raw: string): raw is SortOption {
  return (SORT_VALUES as readonly string[]).includes(raw);
}

export function isDateRange(raw: string): raw is DateRange {
  return (DATE_VALUES as readonly string[]).includes(raw);
}

export function isDocketType(raw: string): raw is DocketType {
  return (TYPE_VALUES as readonly string[]).includes(raw);
}

export const SORT_OPTIONS: { key: SortOption; label: string }[] = [
  { key: 'recent', label: 'New' },
  { key: 'popular', label: 'Popular' },
  { key: 'open', label: 'Open for Comment' },
  { key: 'closed', label: 'Recently Closed' },
];

export const DATE_OPTIONS: { key: DateRange; label: string }[] = [
  { key: '', label: 'All Time' },
  { key: '7d', label: 'Last 7 Days' },
  { key: '30d', label: 'Last Month' },
  { key: '90d', label: 'Last 3 Months' },
  { key: '365d', label: 'Last Year' },
];

export const DOCKET_TYPE_OPTIONS: { key: DocketType; label: string }[] = [
  { key: '', label: 'All Types' },
  { key: 'rule', label: 'Rulemaking' },
  { key: 'nonrule', label: 'Non-Rulemaking' },
  { key: 'other', label: 'Other' },
];
