'use client';

import { useState, useEffect, useMemo } from 'react';
import { Header } from '@/components/Header';
import { AgencyCard } from '@/components/feed/AgencyCard';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { getAllKnownAgencies, getAgencyInfo } from '@/lib/agencyMetadata';
import { Search, Loader2 } from 'lucide-react';

export default function AgenciesPage() {
  const { getPopularAgencies, getAllAgencyCounts, isReady } = useDuckDBService();
  const [popularAgencies, setPopularAgencies] = useState<string[]>([]);
  const [agencyCounts, setAgencyCounts] = useState<Record<string, { dockets: number; comments: number }>>({});
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');

  // All agencies from bundled JSON (instant, no DuckDB needed)
  const allAgencies = useMemo(() => getAllKnownAgencies(), []);

  useEffect(() => {
    if (!isReady) return;
    Promise.all([
      getPopularAgencies(4),
      getAllAgencyCounts(),
    ])
      .then(([popular, counts]) => {
        setPopularAgencies(popular.map(p => p.agency_code));
        setAgencyCounts(counts);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load agency counts:', err);
        setLoading(false);
      });
  }, [isReady, getPopularAgencies, getAllAgencyCounts]);

  const filteredAgencies = useMemo(() => {
    if (!searchQuery) return allAgencies;
    const q = searchQuery.toLowerCase();
    return allAgencies.filter(a =>
      a.code.toLowerCase().includes(q) ||
      a.name.toLowerCase().includes(q)
    );
  }, [allAgencies, searchQuery]);

  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-5xl mx-auto px-4 py-8">
        {/* Hero */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold mb-3">
            <span className="gradient-text">Browse Agencies</span>
          </h1>
          <p className="text-[var(--muted)] text-lg">
            Federal agencies as communities. Explore regulations by agency.
          </p>
        </div>

        {/* Search */}
        <div className="max-w-md mx-auto mb-8">
          <div className="relative">
            <Search
              size={16}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--muted)]"
            />
            <input
              type="text"
              placeholder="Search agencies by name or code..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2.5 bg-[var(--surface)] border border-[var(--border)] rounded-xl text-[var(--foreground)] placeholder:text-[var(--muted)] focus:outline-none focus:border-[var(--accent-primary)] transition-colors"
            />
          </div>
        </div>

        {/* Popular - show immediately since agency list is bundled */}
        {!searchQuery && popularAgencies.length > 0 && (
          <div className="mb-8">
            <h2 className="text-sm font-semibold text-[var(--muted)] uppercase tracking-wider mb-3">
              Popular Agencies
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
              {popularAgencies.map(code => (
                <AgencyCard
                  key={code}
                  code={code}
                  docketCount={agencyCounts[code]?.dockets}
                  commentCount={agencyCounts[code]?.comments}
                />
              ))}
            </div>
          </div>
        )}

        {/* All Agencies */}
        <div>
          <h2 className="text-sm font-semibold text-[var(--muted)] uppercase tracking-wider mb-3">
            {searchQuery ? `Results for "${searchQuery}"` : `All Agencies (${filteredAgencies.length})`}
          </h2>
          {filteredAgencies.length === 0 ? (
            <div className="text-center py-8 text-[var(--muted)]">
              No agencies found matching &quot;{searchQuery}&quot;
            </div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {filteredAgencies.map(a => (
                <AgencyCard
                  key={a.code}
                  code={a.code}
                  docketCount={agencyCounts[a.code]?.dockets}
                  commentCount={agencyCounts[a.code]?.comments}
                />
              ))}
            </div>
          )}
        </div>

        {loading && (
          <div className="flex justify-center py-4">
            <span className="text-sm text-[var(--muted)]">Loading counts...</span>
          </div>
        )}
      </main>
    </div>
  );
}
