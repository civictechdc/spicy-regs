'use client';

import { useState } from 'react';
import type { AgencyInfo } from '@/lib/agencyMetadata';

interface AgencyAvatarProps {
  agency: AgencyInfo;
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

export function AgencyAvatar({ agency, size = 'md', className = '' }: AgencyAvatarProps) {
  const [faviconError, setFaviconError] = useState(false);
  const sizeClass = `agency-avatar-${size}`;

  if (agency.favicon && !faviconError) {
    return (
      <img
        src={agency.favicon}
        alt={`${agency.shortName} icon`}
        className={`agency-avatar ${sizeClass} object-contain bg-white p-1 ${className}`}
        onError={() => setFaviconError(true)}
      />
    );
  }

  return (
    <div
      className={`agency-avatar ${sizeClass} ${className}`}
      style={{ backgroundColor: agency.color }}
    >
      {agency.shortName}
    </div>
  );
}
