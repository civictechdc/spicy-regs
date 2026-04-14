'use client';

import Link from 'next/link';
import { getAgencyInfo } from '@/lib/agencyMetadata';
import type { Docket } from '@/lib/search/types';

interface MiniDocketCardProps {
  docket: Docket;
}

export function MiniDocketCard({ docket }: MiniDocketCardProps) {
  const agency = getAgencyInfo(docket.agencyCode);
  const href = `/sr/${docket.agencyCode}/${encodeURIComponent(docket.docketId)}`;

  return (
    <Link
      href={href}
      className="block rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 hover:border-[var(--accent-primary)] transition-colors min-w-0"
    >
      <div className="flex items-center gap-2 text-xs mb-1.5">
        <span className="font-semibold" style={{ color: agency.color }}>
          sr/{docket.agencyCode}
        </span>
        <span className="text-[var(--muted)]">·</span>
        <span className="font-mono-id text-[var(--muted)] truncate">{docket.docketId}</span>
      </div>
      <p className="text-sm font-medium text-[var(--foreground)] leading-snug line-clamp-3">
        {docket.title}
      </p>
    </Link>
  );
}
