'use client';

import { useState, useEffect, useMemo } from 'react';
import { Header } from '@/components/Header';
import { AgencyCard } from '@/components/feed/AgencyCard';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { Search, Loader2 } from 'lucide-react';

export default function AgenciesPage() {
  const { getAgencies, getPopularAgencies, isReady } = useDuckDBService();
  const [agencies, setAgencies] = useState<string[]>([]);
  const [popularAgencies, setPopularAgencies] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (!isReady) return;
    Promise.all([
      getAgencies(),
      getPopularAgencies(4),
    ])
      .then(([all, popular]) => {
        setAgencies(all);
        setPopularAgencies(popular.map(p => p.agency_code));
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load agencies:', err);
        setLoading(false);
      });
  }, [isReady, getAgencies, getPopularAgencies]);

  const filteredAgencies = useMemo(() => {
    if (!searchQuery) return agencies;
    const q = searchQuery.toLowerCase();
    return agencies.filter(a => a.toLowerCase().includes(q));
  }, [agencies, searchQuery]);

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
              placeholder="Search agencies..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2.5 bg-[var(--surface)] border border-[var(--border)] rounded-xl text-[var(--foreground)] placeholder:text-[var(--muted)] focus:outline-none focus:border-[var(--accent-primary)] transition-colors"
            />
          </div>
        </div>

        {loading ? (
          <div className="flex justify-center py-16">
            <Loader2 size={24} className="animate-spin text-[var(--accent-primary)]" />
          </div>
        ) : (
          <>
            {/* Popular */}
            {!searchQuery && popularAgencies.length > 0 && (
              <div className="mb-8">
                <h2 className="text-sm font-semibold text-[var(--muted)] uppercase tracking-wider mb-3">
                  Popular Agencies
                </h2>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
                  {popularAgencies.map(code => (
                    <AgencyCard key={code} code={code} />
                  ))}
                </div>
              </div>
            )}

            {/* All Agencies */}
            <div>
              <h2 className="text-sm font-semibold text-[var(--muted)] uppercase tracking-wider mb-3">
                {searchQuery ? `Results for "${searchQuery}"` : 'All Agencies'}
              </h2>
              {filteredAgencies.length === 0 ? (
                <div className="text-center py-8 text-[var(--muted)]">
                  No agencies found matching &quot;{searchQuery}&quot;
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  {filteredAgencies.map(code => (
                    <AgencyCard key={code} code={code} />
                  ))}
                </div>
              )}
            </div>
          </>
        )}
      </main>
    </div>
  );
}
