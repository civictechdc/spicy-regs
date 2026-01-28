'use client';

import { useEffect, useState } from 'react';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { SearchableSelect } from './SearchableSelect';

interface AgencySelectorProps {
  selectedAgency: string | null;
  onSelectAgency: (agency: string | null) => void;
}

export function AgencySelector({ selectedAgency, onSelectAgency }: AgencySelectorProps) {
  const [agencies, setAgencies] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  const { getAgencies, isReady } = useDuckDBService();

  useEffect(() => {
    if (!isReady) return;
    
    async function loadAgencies() {
      try {
        setLoading(true);
        const data = await getAgencies();
        setAgencies(data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load agencies');
      } finally {
        setLoading(false);
      }
    }
    loadAgencies();
  }, [isReady, getAgencies]);

  if (!isReady || loading) {
    return (
      <div className="flex items-center justify-center p-8">
        <div className="text-sm text-gray-500">Loading agencies...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
        <p className="text-sm text-red-600">Error: {error}</p>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <SearchableSelect
        label="Select Agency"
        options={agencies}
        value={selectedAgency}
        onChange={onSelectAgency}
        placeholder="Search agencies..."
        isLoading={loading}
      />
    </div>
  );
}

