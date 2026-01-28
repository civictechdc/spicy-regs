'use client';

import { useEffect, useState } from 'react';
import { useDuckDBService } from '@/lib/duckdb/useDuckDBService';
import { SearchableSelect } from './SearchableSelect';

interface DocketSelectorProps {
  agencyCode: string | null;
  selectedDocket: string | null;
  onSelectDocket: (docket: string | null) => void;
}

export function DocketSelector({ agencyCode, selectedDocket, onSelectDocket }: DocketSelectorProps) {
  const [dockets, setDockets] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  const { getDockets, isReady } = useDuckDBService();

  useEffect(() => {
    if (!agencyCode || !isReady) {
      setDockets([]);
      return;
    }

    async function loadDockets() {
      try {
        setLoading(true);
        const data = await getDockets(agencyCode as string);
        setDockets(['ALL', ...data]);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load dockets');
      } finally {
        setLoading(false);
      }
    }
    loadDockets();
  }, [agencyCode, isReady, getDockets]);

  if (!agencyCode) {
    return null;
  }

  return (
    <div className="space-y-2">
      <SearchableSelect
        label="Select Docket (Optional)"
        options={dockets}
        value={selectedDocket}
        onChange={onSelectDocket}
        placeholder="Search dockets..."
        isLoading={loading}
      />
      {error && (
        <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
      )}
    </div>
  );
}


