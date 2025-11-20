'use client';

import { DataType } from '@/lib/api';

interface DataTypeSelectorProps {
  selectedType: DataType;
  onSelectType: (type: DataType) => void;
}

export function DataTypeSelector({ selectedType, onSelectType }: DataTypeSelectorProps) {
  const types: { value: DataType; label: string }[] = [
    { value: 'dockets', label: 'Dockets' },
    { value: 'documents', label: 'Documents' },
    { value: 'comments', label: 'Comments' },
  ];

  return (
    <div className="space-y-2">
      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300">
        Data Type
      </label>
      <div className="flex gap-2">
        {types.map((type) => (
          <button
            key={type.value}
            onClick={() => onSelectType(type.value)}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              selectedType === type.value
                ? 'bg-blue-600 text-white'
                : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
            }`}
          >
            {type.label}
          </button>
        ))}
      </div>
    </div>
  );
}

