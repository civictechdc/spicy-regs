'use client';

import Link from 'next/link';
import { MessageSquare, FileText } from 'lucide-react';
import { getAgencyInfo, formatCount } from '@/lib/agencyMetadata';

interface AgencyCardProps {
  code: string;
  docketCount?: number;
  commentCount?: number;
}

export function AgencyCard({ code, docketCount = 0, commentCount = 0 }: AgencyCardProps) {
  const agency = getAgencyInfo(code);

  return (
    <Link href={`/sr/${code}`} className="block">
      <div className="card-gradient p-5 hover:border-[var(--accent-primary)] transition-all duration-200 group">
        <div className="flex items-center gap-3 mb-3">
          <div
            className="agency-avatar agency-avatar-md"
            style={{ backgroundColor: agency.color }}
          >
            {agency.shortName}
          </div>
          <div className="min-w-0">
            <h3 className="font-semibold text-[var(--foreground)] group-hover:text-[var(--accent-primary)] transition-colors truncate">
              {agency.name}
            </h3>
            <span className="text-sm text-[var(--muted)]">sr/{code}</span>
          </div>
        </div>

        <p className="text-sm text-[var(--muted)] line-clamp-2 mb-3">
          {agency.description}
        </p>

        <div className="flex items-center gap-4 text-xs text-[var(--muted)]">
          <span className="metadata-pill">
            <FileText size={12} />
            {formatCount(docketCount)} dockets
          </span>
          <span className="metadata-pill">
            <MessageSquare size={12} />
            {formatCount(commentCount)} comments
          </span>
        </div>
      </div>
    </Link>
  );
}
