'use client';

import { useEffect, useState } from 'react';
import { getDockets } from '@/lib/api';

interface DocketSelectorProps {
  agencyCode: string | null;
  selectedDocket: string | null;
  onSelectDocket: (docket: string | null) => void;
}

export function DocketSelector({ agencyCode, selectedDocket, onSelectDocket }: DocketSelectorProps) {
  const [dockets, setDockets] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!agencyCode) {
      setDockets([]);
      return;
    }

    async function loadDockets() {
      try {
        setLoading(true);
        const data = await getDockets(agencyCode as string);
        setDockets(data);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load dockets');
      } finally {
        setLoading(false);
      }
    }
    loadDockets();
  }, [agencyCode]);

  if (!agencyCode) {
    return null;
  }

  return (
    <div className="space-y-2">
      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
        Select Docket (Optional)
      </label>
      <select
        value={selectedDocket || ''}
        onChange={(e) => onSelectDocket(e.target.value || null)}
        disabled={loading}
        className="w-full px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:opacity-50"
      >
        <option value="">-- All dockets --</option>
        {loading ? (
          <option disabled>Loading...</option>
        ) : (
          dockets.map((docket) => (
            <option key={docket} value={docket}>
              {docket}
            </option>
          ))
        )}
      </select>
      {error && (
        <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
      )}
    </div>
  );
}

