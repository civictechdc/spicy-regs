'use client';

import { useState, useEffect, useMemo } from 'react';
import { X, ChevronDown } from 'lucide-react';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';

type SortOption = 'recent' | 'popular' | 'open';

interface FeedFiltersProps {
  selectedAgency: string;
  onAgencyChange: (agency: string) => void;
  sortBy: SortOption;
  onSortChange: (sort: SortOption) => void;
}

const PREFS_SORT_KEY = 'spicy-regs-sort-preference';

export function FeedFilters({
  selectedAgency,
  onAgencyChange,
  sortBy,
  onSortChange,
}: FeedFiltersProps) {
  const { getAgencies, isReady } = useDuckDBService();
  const [agencies, setAgencies] = useState<string[]>([]);
  const [showAgencyDropdown, setShowAgencyDropdown] = useState(false);
  const [agencySearch, setAgencySearch] = useState('');

  useEffect(() => {
    if (!isReady) return;
    getAgencies().then(setAgencies).catch(console.error);
  }, [isReady, getAgencies]);

  // Persist sort preference
  useEffect(() => {
    try {
      localStorage.setItem(PREFS_SORT_KEY, sortBy);
    } catch {}
  }, [sortBy]);

  const filteredAgencies = useMemo(() => {
    if (!agencySearch) return agencies;
    const q = agencySearch.toLowerCase();
    return agencies.filter(a => a.toLowerCase().includes(q));
  }, [agencies, agencySearch]);

  const sortOptions: { key: SortOption; label: string }[] = [
    { key: 'recent', label: 'Recent' },
    { key: 'popular', label: 'Popular' },
    { key: 'open', label: 'Open for Comment' },
  ];

  return (
    <div className="flex items-center gap-3 flex-wrap">
      {/* Agency Dropdown */}
      <div className="relative">
        <button
          onClick={() => setShowAgencyDropdown(!showAgencyDropdown)}
          className="filter-chip flex items-center gap-1.5"
        >
          {selectedAgency || 'All Agencies'}
          <ChevronDown size={14} />
        </button>

        {showAgencyDropdown && (
          <>
            <div
              className="fixed inset-0 z-40"
              onClick={() => setShowAgencyDropdown(false)}
            />
            <div className="absolute top-full mt-1 left-0 z-50 w-56 bg-[var(--surface)] border border-[var(--border)] rounded-lg shadow-xl overflow-hidden">
              <div className="p-2">
                <input
                  type="text"
                  placeholder="Search agencies..."
                  value={agencySearch}
                  onChange={e => setAgencySearch(e.target.value)}
                  className="w-full px-3 py-1.5 text-sm bg-[var(--background)] border border-[var(--border)] rounded-md text-[var(--foreground)] placeholder:text-[var(--muted)] focus:outline-none focus:border-[var(--accent-primary)]"
                  autoFocus
                />
              </div>
              <div className="max-h-60 overflow-y-auto">
                <button
                  onClick={() => { onAgencyChange(''); setShowAgencyDropdown(false); setAgencySearch(''); }}
                  className={`w-full text-left px-4 py-2 text-sm hover:bg-[var(--surface-elevated)] transition-colors ${
                    !selectedAgency ? 'text-[var(--accent-primary)] font-medium' : 'text-[var(--foreground)]'
                  }`}
                >
                  All Agencies
                </button>
                {filteredAgencies.map(a => (
                  <button
                    key={a}
                    onClick={() => { onAgencyChange(a); setShowAgencyDropdown(false); setAgencySearch(''); }}
                    className={`w-full text-left px-4 py-2 text-sm hover:bg-[var(--surface-elevated)] transition-colors ${
                      selectedAgency === a ? 'text-[var(--accent-primary)] font-medium' : 'text-[var(--foreground)]'
                    }`}
                  >
                    {a}
                  </button>
                ))}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Sort Chips */}
      <div className="flex items-center gap-1.5">
        {sortOptions.map(opt => (
          <button
            key={opt.key}
            onClick={() => onSortChange(opt.key)}
            className={`filter-chip ${sortBy === opt.key ? 'filter-chip-active' : ''}`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      {/* Active Filter Indicator */}
      {selectedAgency && (
        <button
          onClick={() => onAgencyChange('')}
          className="inline-flex items-center gap-1 px-2 py-1 rounded-lg text-xs font-medium bg-[rgba(99,102,241,0.1)] text-[var(--accent-primary)] hover:bg-[rgba(99,102,241,0.2)] transition-colors"
        >
          sr/{selectedAgency}
          <X size={12} />
        </button>
      )}
    </div>
  );
}
