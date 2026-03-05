/**
 * Static metadata for federal agencies.
 * Maps agency codes to display info, accent colors, and links.
 *
 * Agency data loaded from generated agencies.json (scripts/generate_agencies.py).
 */

import agenciesData from './data/agencies.json';

export interface AgencyInfo {
  code: string;
  name: string;
  shortName: string;
  description: string;
  color: string;
  website: string | null;
  favicon: string | null;
  relatedAgencies: string[];
  links: { label: string; url: string }[];
}

/** Extra curated metadata for major agencies (colors, related, links) */
const CURATED_EXTRAS: Record<string, Partial<AgencyInfo>> = {
  EPA: {
    color: 'var(--agency-epa)',
    relatedAgencies: ['DOE', 'DOT', 'USDA', 'NOAA'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'epa.gov', url: 'https://www.epa.gov' },
      { label: 'Federal Register', url: 'https://www.federalregister.gov' },
    ],
  },
  FDA: {
    color: 'var(--agency-fda)',
    relatedAgencies: ['HHS', 'USDA', 'EPA'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'fda.gov', url: 'https://www.fda.gov' },
    ],
  },
  FCC: {
    color: 'var(--agency-fcc)',
    relatedAgencies: ['FTC', 'DOJ'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'fcc.gov', url: 'https://www.fcc.gov' },
    ],
  },
  DOT: {
    color: 'var(--agency-dot)',
    relatedAgencies: ['EPA', 'DOE', 'DHS'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'transportation.gov', url: 'https://www.transportation.gov' },
    ],
  },
  SEC: {
    color: 'var(--agency-sec)',
    relatedAgencies: ['CFTC', 'FTC', 'DOJ'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'sec.gov', url: 'https://www.sec.gov' },
    ],
  },
  USDA: {
    color: 'var(--agency-usda)',
    relatedAgencies: ['EPA', 'FDA', 'DOI'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'usda.gov', url: 'https://www.usda.gov' },
    ],
  },
  DOE: {
    color: 'var(--agency-doe)',
    relatedAgencies: ['EPA', 'DOT', 'NRC'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'energy.gov', url: 'https://www.energy.gov' },
    ],
  },
  HHS: {
    color: 'var(--agency-hhs)',
    relatedAgencies: ['FDA', 'CMS', 'CDC'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'hhs.gov', url: 'https://www.hhs.gov' },
    ],
  },
  DHS: {
    color: 'var(--agency-dhs)',
    relatedAgencies: ['DOJ', 'DOD', 'DOT'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'dhs.gov', url: 'https://www.dhs.gov' },
    ],
  },
  DOJ: {
    color: 'var(--agency-doj)',
    relatedAgencies: ['DHS', 'FTC', 'SEC'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'justice.gov', url: 'https://www.justice.gov' },
    ],
  },
  DOD: {
    color: 'var(--agency-dod)',
    relatedAgencies: ['DHS', 'DOE', 'VA'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'defense.gov', url: 'https://www.defense.gov' },
    ],
  },
  HUD: {
    color: 'var(--agency-hud)',
    relatedAgencies: ['EPA', 'DOT', 'ED'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'hud.gov', url: 'https://www.hud.gov' },
    ],
  },
  ED: {
    color: 'var(--agency-ed)',
    relatedAgencies: ['HHS', 'HUD'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'ed.gov', url: 'https://www.ed.gov' },
    ],
  },
  NOAA: {
    color: 'var(--agency-noaa)',
    relatedAgencies: ['EPA', 'USDA', 'DOI'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'noaa.gov', url: 'https://www.noaa.gov' },
    ],
  },
};

interface AgencyJson {
  code: string;
  name: string;
  description: string | null;
  website: string | null;
  favicon: string | null;
}

/** Build lookup map from agencies.json */
const AGENCY_MAP = new Map<string, AgencyInfo>();
for (const entry of agenciesData as AgencyJson[]) {
  const extras = CURATED_EXTRAS[entry.code] || {};
  AGENCY_MAP.set(entry.code, {
    code: entry.code,
    name: entry.name,
    shortName: entry.code,
    description: entry.description || 'Federal regulatory agency.',
    color: extras.color || 'var(--agency-default)',
    website: entry.website,
    favicon: entry.favicon,
    relatedAgencies: extras.relatedAgencies || [],
    links: extras.links || [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
    ],
  });
}

/**
 * Get metadata for an agency by code.
 * Returns a sensible fallback for unknown agencies.
 */
export function getAgencyInfo(code: string): AgencyInfo {
  const upper = code.toUpperCase();
  const info = AGENCY_MAP.get(upper);
  if (info) return info;

  return {
    code: upper,
    name: upper,
    shortName: upper,
    description: 'Federal regulatory agency.',
    color: 'var(--agency-default)',
    website: null,
    favicon: null,
    relatedAgencies: [],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
    ],
  };
}

/** Get all known agencies sorted by name */
export function getAllKnownAgencies(): AgencyInfo[] {
  return Array.from(AGENCY_MAP.values()).sort((a, b) => a.name.localeCompare(b.name));
}

/** Generate a deterministic color for avatars based on a string */
export function stringToColor(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 55%, 45%)`;
}

/** Get initials from a name (first letter of each word, max 2) */
export function getInitials(name: string): string {
  return name
    .split(/[\s-]+/)
    .filter(Boolean)
    .map(w => w[0])
    .slice(0, 2)
    .join('')
    .toUpperCase();
}

/** Format a number with K/M suffix */
export function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toString();
}

/** Format a date as relative time (e.g. "2d ago") */
export function timeAgo(date: string | Date): string {
  const d = typeof date === 'string' ? new Date(date) : date;
  const now = new Date();
  const diffMs = now.getTime() - d.getTime();
  const diffS = Math.floor(diffMs / 1000);
  const diffM = Math.floor(diffS / 60);
  const diffH = Math.floor(diffM / 60);
  const diffD = Math.floor(diffH / 24);
  const diffW = Math.floor(diffD / 7);
  const diffMo = Math.floor(diffD / 30);
  const diffY = Math.floor(diffD / 365);

  if (diffY > 0) return `${diffY}y ago`;
  if (diffMo > 0) return `${diffMo}mo ago`;
  if (diffW > 0) return `${diffW}w ago`;
  if (diffD > 0) return `${diffD}d ago`;
  if (diffH > 0) return `${diffH}h ago`;
  if (diffM > 0) return `${diffM}m ago`;
  return 'just now';
}
