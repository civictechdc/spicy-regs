/**
 * Static metadata for federal agencies.
 * Maps agency codes to display info, accent colors, and links.
 */

export interface AgencyInfo {
  code: string;
  name: string;
  shortName: string;
  description: string;
  color: string;
  relatedAgencies: string[];
  links: { label: string; url: string }[];
}

const AGENCY_MAP: Record<string, AgencyInfo> = {
  EPA: {
    code: 'EPA',
    name: 'Environmental Protection Agency',
    shortName: 'EPA',
    description: 'Protecting human health and the environment through regulations on air quality, water, chemicals, and waste.',
    color: 'var(--agency-epa)',
    relatedAgencies: ['DOE', 'DOT', 'USDA', 'NOAA'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'epa.gov', url: 'https://www.epa.gov' },
      { label: 'Federal Register', url: 'https://www.federalregister.gov' },
    ],
  },
  FDA: {
    code: 'FDA',
    name: 'Food and Drug Administration',
    shortName: 'FDA',
    description: 'Ensuring the safety of food, drugs, medical devices, cosmetics, and tobacco products.',
    color: 'var(--agency-fda)',
    relatedAgencies: ['HHS', 'USDA', 'EPA'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'fda.gov', url: 'https://www.fda.gov' },
    ],
  },
  FCC: {
    code: 'FCC',
    name: 'Federal Communications Commission',
    shortName: 'FCC',
    description: 'Regulating interstate and international communications by radio, television, wire, satellite, and cable.',
    color: 'var(--agency-fcc)',
    relatedAgencies: ['FTC', 'DOJ'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'fcc.gov', url: 'https://www.fcc.gov' },
    ],
  },
  DOT: {
    code: 'DOT',
    name: 'Department of Transportation',
    shortName: 'DOT',
    description: 'Ensuring a fast, safe, efficient, accessible, and convenient transportation system.',
    color: 'var(--agency-dot)',
    relatedAgencies: ['EPA', 'DOE', 'DHS'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'transportation.gov', url: 'https://www.transportation.gov' },
    ],
  },
  SEC: {
    code: 'SEC',
    name: 'Securities and Exchange Commission',
    shortName: 'SEC',
    description: 'Protecting investors and maintaining fair, orderly, and efficient markets.',
    color: 'var(--agency-sec)',
    relatedAgencies: ['CFTC', 'FTC', 'DOJ'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'sec.gov', url: 'https://www.sec.gov' },
    ],
  },
  USDA: {
    code: 'USDA',
    name: 'U.S. Department of Agriculture',
    shortName: 'USDA',
    description: 'Providing leadership on food, agriculture, natural resources, and related issues.',
    color: 'var(--agency-usda)',
    relatedAgencies: ['EPA', 'FDA', 'DOI'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'usda.gov', url: 'https://www.usda.gov' },
    ],
  },
  DOE: {
    code: 'DOE',
    name: 'Department of Energy',
    shortName: 'DOE',
    description: 'Addressing energy, environmental, and nuclear challenges through science and technology.',
    color: 'var(--agency-doe)',
    relatedAgencies: ['EPA', 'DOT', 'NRC'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'energy.gov', url: 'https://www.energy.gov' },
    ],
  },
  HHS: {
    code: 'HHS',
    name: 'Department of Health and Human Services',
    shortName: 'HHS',
    description: 'Enhancing the health and well-being of all Americans through effective health and human services.',
    color: 'var(--agency-hhs)',
    relatedAgencies: ['FDA', 'CMS', 'CDC'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'hhs.gov', url: 'https://www.hhs.gov' },
    ],
  },
  DHS: {
    code: 'DHS',
    name: 'Department of Homeland Security',
    shortName: 'DHS',
    description: 'Safeguarding the American people, the homeland, and our values.',
    color: 'var(--agency-dhs)',
    relatedAgencies: ['DOJ', 'DOD', 'DOT'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'dhs.gov', url: 'https://www.dhs.gov' },
    ],
  },
  DOJ: {
    code: 'DOJ',
    name: 'Department of Justice',
    shortName: 'DOJ',
    description: 'Enforcing the law and defending the interests of the United States.',
    color: 'var(--agency-doj)',
    relatedAgencies: ['DHS', 'FTC', 'SEC'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'justice.gov', url: 'https://www.justice.gov' },
    ],
  },
  DOD: {
    code: 'DOD',
    name: 'Department of Defense',
    shortName: 'DOD',
    description: 'Providing military forces needed to deter war and ensure national security.',
    color: 'var(--agency-dod)',
    relatedAgencies: ['DHS', 'DOE', 'VA'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'defense.gov', url: 'https://www.defense.gov' },
    ],
  },
  HUD: {
    code: 'HUD',
    name: 'Department of Housing and Urban Development',
    shortName: 'HUD',
    description: 'Creating strong, sustainable, inclusive communities and quality affordable homes for all.',
    color: 'var(--agency-hud)',
    relatedAgencies: ['EPA', 'DOT', 'ED'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'hud.gov', url: 'https://www.hud.gov' },
    ],
  },
  ED: {
    code: 'ED',
    name: 'Department of Education',
    shortName: 'ED',
    description: 'Promoting student achievement and preparation for global competitiveness.',
    color: 'var(--agency-ed)',
    relatedAgencies: ['HHS', 'HUD'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'ed.gov', url: 'https://www.ed.gov' },
    ],
  },
  NOAA: {
    code: 'NOAA',
    name: 'National Oceanic and Atmospheric Administration',
    shortName: 'NOAA',
    description: 'Understanding and predicting changes in climate, weather, oceans, and coasts.',
    color: 'var(--agency-noaa)',
    relatedAgencies: ['EPA', 'USDA', 'DOI'],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
      { label: 'noaa.gov', url: 'https://www.noaa.gov' },
    ],
  },
};

/**
 * Get metadata for an agency by code.
 * Returns a sensible fallback for unknown agencies.
 */
export function getAgencyInfo(code: string): AgencyInfo {
  const upper = code.toUpperCase();
  if (AGENCY_MAP[upper]) return AGENCY_MAP[upper];

  return {
    code: upper,
    name: upper,
    shortName: upper,
    description: `Federal regulatory agency.`,
    color: 'var(--agency-default)',
    relatedAgencies: [],
    links: [
      { label: 'regulations.gov', url: 'https://www.regulations.gov' },
    ],
  };
}

/** Get all known agencies sorted by name */
export function getAllKnownAgencies(): AgencyInfo[] {
  return Object.values(AGENCY_MAP).sort((a, b) => a.name.localeCompare(b.name));
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
