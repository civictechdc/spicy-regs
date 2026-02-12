'use client';

import { useMemo } from 'react';
import Link from 'next/link';
import { ExternalLink, Scale, BookOpen } from 'lucide-react';
import { getAgencyInfo, formatCount } from '@/lib/agencyMetadata';

interface AgencySidebarProps {
  agencyCode: string;
  stats?: {
    docketCount: number;
    documentCount: number;
    commentCount: number;
  };
}

export function AgencySidebar({ agencyCode, stats }: AgencySidebarProps) {
  const agency = useMemo(() => getAgencyInfo(agencyCode), [agencyCode]);

  return (
    <div className="space-y-4">
      {/* About Card */}
      <div className="card-gradient overflow-hidden">
        {/* Header */}
        <div
          className="h-16 relative"
          style={{
            background: `linear-gradient(135deg, ${agency.color}33, ${agency.color}11)`,
          }}
        />

        <div className="p-4 -mt-6">
          <div className="flex items-center gap-3 mb-3">
            <div
              className="agency-avatar agency-avatar-md border-2 border-[var(--surface)]"
              style={{ backgroundColor: agency.color }}
            >
              {agency.shortName}
            </div>
            <div>
              <h3 className="font-semibold text-sm text-[var(--foreground)]">
                About sr/{agencyCode}
              </h3>
              <span className="text-xs text-[var(--muted)]">{agency.name}</span>
            </div>
          </div>

          <p className="text-sm text-[var(--muted)] mb-4 leading-relaxed">
            {agency.description}
          </p>

          {/* Stats Grid */}
          {stats && (
            <div className="grid grid-cols-3 gap-2 mb-4">
              <div className="text-center p-2 bg-[var(--background)] rounded-lg">
                <div className="text-lg font-bold text-[var(--foreground)]">
                  {formatCount(stats.docketCount)}
                </div>
                <div className="text-[10px] text-[var(--muted)]">Dockets</div>
              </div>
              <div className="text-center p-2 bg-[var(--background)] rounded-lg">
                <div className="text-lg font-bold text-[var(--foreground)]">
                  {formatCount(stats.documentCount)}
                </div>
                <div className="text-[10px] text-[var(--muted)]">Docs</div>
              </div>
              <div className="text-center p-2 bg-[var(--background)] rounded-lg">
                <div className="text-lg font-bold text-[var(--foreground)]">
                  {formatCount(stats.commentCount)}
                </div>
                <div className="text-[10px] text-[var(--muted)]">Comments</div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Guidelines */}
      <div className="card-gradient p-4">
        <h4 className="font-semibold text-sm text-[var(--foreground)] mb-2 flex items-center gap-1.5">
          <Scale size={14} />
          Guidelines
        </h4>
        <ul className="space-y-1.5 text-xs text-[var(--muted)]">
          <li className="flex items-start gap-1.5">
            <span className="text-[var(--accent-green)] mt-0.5">✓</span>
            Review docket information before commenting
          </li>
          <li className="flex items-start gap-1.5">
            <span className="text-[var(--accent-green)] mt-0.5">✓</span>
            Focus on substantive policy issues
          </li>
          <li className="flex items-start gap-1.5">
            <span className="text-[var(--accent-green)] mt-0.5">✓</span>
            Submit official comments at regulations.gov
          </li>
        </ul>
      </div>

      {/* Related Agencies */}
      {agency.relatedAgencies.length > 0 && (
        <div className="card-gradient p-4">
          <h4 className="font-semibold text-sm text-[var(--foreground)] mb-2 flex items-center gap-1.5">
            <BookOpen size={14} />
            Related Agencies
          </h4>
          <div className="space-y-2">
            {agency.relatedAgencies.map(code => {
              const related = getAgencyInfo(code);
              return (
                <Link
                  key={code}
                  href={`/sr/${code}`}
                  className="flex items-center gap-2 text-sm text-[var(--muted)] hover:text-[var(--foreground)] transition-colors"
                >
                  <div
                    className="w-5 h-5 rounded-full flex items-center justify-center text-[8px] font-bold text-white"
                    style={{ backgroundColor: related.color }}
                  >
                    {code.slice(0, 2)}
                  </div>
                  {related.name}
                </Link>
              );
            })}
          </div>
        </div>
      )}

      {/* Useful Links */}
      <div className="card-gradient p-4">
        <h4 className="font-semibold text-sm text-[var(--foreground)] mb-2 flex items-center gap-1.5">
          <ExternalLink size={14} />
          Useful Links
        </h4>
        <div className="space-y-1.5">
          {agency.links.map((link, i) => (
            <a
              key={i}
              href={link.url}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-sm text-[var(--accent-primary)] hover:underline"
            >
              <ExternalLink size={12} />
              {link.label}
            </a>
          ))}
        </div>
      </div>
    </div>
  );
}
