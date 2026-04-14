'use client';

import { ChevronDown } from 'lucide-react';
import { TOPIC_OPTIONS, isTopicKey, type TopicKey } from '@/lib/feedFilters';

interface TopicFilterProps {
  topic: TopicKey;
  onTopicChange: (topic: TopicKey) => void;
}

export function TopicFilter({ topic, onTopicChange }: TopicFilterProps) {
  return (
    <div className="relative inline-block">
      <div className="flex items-center gap-1.5 px-1.5">
        <ChevronDown
          size={14}
          className="text-[var(--muted)] pointer-events-none absolute right-2"
        />
        <select
          value={topic}
          onChange={e => {
            const next = e.target.value;
            if (isTopicKey(next)) onTopicChange(next);
          }}
          className="filter-chip appearance-none pr-7 cursor-pointer bg-[var(--surface)] text-[var(--foreground)] border-none focus:outline-none"
          aria-label="Filter by topic"
        >
          {TOPIC_OPTIONS.map(opt => (
            <option key={opt.key || 'all'} value={opt.key}>
              {opt.emoji} {opt.label}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
