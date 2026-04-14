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

/**
 * Sort chips shown in the UI. Excludes 'open' and 'closed' — those are now
 * surfaced through a Status dropdown since they're conceptually status filters
 * with an implied ordering, not pure sort orders.
 */
export const SORT_CHIP_OPTIONS: { key: SortOption; label: string }[] = [
  { key: 'recent', label: 'New' },
  { key: 'popular', label: 'Popular' },
];

/**
 * Status filter values map directly onto SortOption to avoid duplicating state.
 * 'all' falls back to the chip-driven sort (recent/popular).
 */
export type StatusOption = 'all' | 'open' | 'closed';

export const STATUS_OPTIONS: { key: StatusOption; label: string }[] = [
  { key: 'all', label: 'All Statuses' },
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

export type TopicKey = 'environment' | 'health' | 'finance' | 'transport' | 'labor' | 'energy' | 'security' | '';

export interface TopicDefinition {
  label: string;
  emoji: string;
  agencies: string[];
  keywords: string[];
}

export const TOPIC_MAPPINGS: Record<Exclude<TopicKey, ''>, TopicDefinition> = {
  environment: {
    label: 'Environment',
    emoji: '🌿',
    agencies: ['EPA', 'NOAA', 'DOI', 'COE', 'CEQ'],
    keywords: ['air', 'water', 'climate', 'emission', 'wildlife', 'pollution'],
  },
  health: {
    label: 'Health',
    emoji: '🏥',
    agencies: ['FDA', 'CDC', 'CMS', 'HHS', 'AHRQ', 'NIH'],
    keywords: ['drug', 'medicine', 'health', 'patient', 'hospital', 'vaccine'],
  },
  finance: {
    label: 'Finance',
    emoji: '💰',
    agencies: ['SEC', 'CFPB', 'OCC', 'FDIC', 'TREAS', 'IRS', 'FTC'],
    keywords: ['banking', 'credit', 'tax', 'financial', 'stock', 'audit'],
  },
  transport: {
    label: 'Transport',
    emoji: '✈️',
    agencies: ['DOT', 'FAA', 'FHWA', 'FRA', 'NHTSA', 'MARAD', 'TSA'],
    keywords: ['aviation', 'vehicle', 'highway', 'rail', 'bridge', 'safety'],
  },
  labor: {
    label: 'Labor',
    emoji: '👷',
    agencies: ['DOL', 'OSHA', 'EBSA', 'ETA', 'MSHA', 'VETS', 'WHD'],
    keywords: ['work', 'employee', 'wage', 'safety', 'union', 'pension'],
  },
  energy: {
    label: 'Energy',
    emoji: '⚡',
    agencies: ['DOE', 'NRC', 'FERC', 'NNSA', 'BOEM', 'BSEE'],
    keywords: ['nuclear', 'grid', 'energy', 'oil', 'gas', 'renewable'],
  },
  security: {
    label: 'Security',
    emoji: '🛡️',
    agencies: ['DHS', 'CISA', 'DOJ', 'FBI', 'ATF', 'DEA'],
    keywords: ['threat', 'cyber', 'defense', 'police', 'crime', 'border'],
  },
};

export const TOPIC_STORAGE_KEY = 'spicy-regs-topic-preference';
export const DEFAULT_TOPIC: TopicKey = '';

export function isTopicKey(raw: string): raw is TopicKey {
  return raw === '' || Object.keys(TOPIC_MAPPINGS).includes(raw);
}

export const TOPIC_OPTIONS: { key: TopicKey; label: string; emoji: string }[] = [
  { key: '', label: 'All Topics', emoji: '✨' },
  ...(Object.entries(TOPIC_MAPPINGS) as [Exclude<TopicKey, ''>, TopicDefinition][]).map(
    ([key, def]) => ({ key, label: def.label, emoji: def.emoji }),
  ),
];
