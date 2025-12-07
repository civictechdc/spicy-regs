"use strict";

import { ChevronDownIcon, ChevronRightIcon } from "lucide-react";
import { useState } from "react";

interface ThinkingProcessProps {
  logs: string[];
  isOpen?: boolean;
}

export function ThinkingProcess({ logs, isOpen = false }: ThinkingProcessProps) {
  const [isExpanded, setIsExpanded] = useState(isOpen);

  if (!logs || logs.length === 0) {
    return null;
  }

  return (
    <div className="mb-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 overflow-hidden">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center gap-2 w-full px-3 py-2 text-xs font-medium text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
      >
        {isExpanded ? (
          <ChevronDownIcon className="w-3 h-3" />
        ) : (
          <ChevronRightIcon className="w-3 h-3" />
        )}
        <span>Debug Event Messages</span>
      </button>
      
      {isExpanded && (
        <div className="p-3 border-t border-gray-200 dark:border-gray-700 text-xs font-mono bg-white dark:bg-gray-900 overflow-x-auto">
          <ul className="space-y-1">
            {logs.map((log, index) => (
              <li key={index} className="text-gray-600 dark:text-gray-300 whitespace-pre-wrap">
                {log}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
