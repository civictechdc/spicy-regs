'use client';

import { useEffect, useState } from 'react';
import { getAgencies } from '@/lib/api';

interface AgencySelectorProps {
  selectedAgency: string | null;
  onSelectAgency: (agency: string) => void;
}

export function AgencySelector({ selectedAgency, onSelectAgency }: AgencySelectorProps) {
  const [agencies, setAgencies] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
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
  }, []);

  if (loading) {
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
      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
        Select Agency
      </label>
      <select
        value={selectedAgency || ''}
        onChange={(e) => onSelectAgency(e.target.value)}
        className="w-full px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-blue-500 focus:border-transparent"
      >
        <option value="">-- Select an agency --</option>
        {agencies.map((agency) => (
          <option key={agency} value={agency}>
            {agency}
          </option>
        ))}
      </select>
    </div>
  );
}

