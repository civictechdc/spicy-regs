"use client";

import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Search } from "lucide-react";

import { useDocketSearch } from "@/lib/search/useDocketSearch";
import type { SearchResult, SuggestResult } from "@/lib/search/types";

const RECENT_KEY = "spicy-regs.recent-search";
const RECENT_MAX = 5;
const SUGGEST_DEBOUNCE_MS = 180;
const PREVIEW_LIMIT = 5;

type InputProps = {
  initialQuery?: string;
  autoFocus?: boolean;
  /** Callback when the user commits a query (Enter / suggestion click). */
  onCommit?: (q: string) => void;
  placeholder?: string;
  className?: string;
};

function readRecents(): string[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(RECENT_KEY);
    return raw ? (JSON.parse(raw) as string[]) : [];
  } catch {
    return [];
  }
}

function writeRecent(q: string) {
  if (typeof window === "undefined") return;
  try {
    const existing = readRecents().filter((x) => x !== q);
    const next = [q, ...existing].slice(0, RECENT_MAX);
    window.localStorage.setItem(RECENT_KEY, JSON.stringify(next));
  } catch {
    /* ignore */
  }
}

type DropdownItem =
  | { kind: "recent"; text: string }
  | { kind: "suggest"; text: string }
  | { kind: "preview"; result: SearchResult };

export function SearchInput({
  initialQuery = "",
  autoFocus = false,
  onCommit,
  placeholder = "Search regulations...",
  className = "",
}: InputProps) {
  const router = useRouter();
  const inputRef = useRef<HTMLInputElement>(null);
  const rootRef = useRef<HTMLDivElement>(null);
  const [value, setValue] = useState(initialQuery);
  const [open, setOpen] = useState(false);
  const [activeIdx, setActiveIdx] = useState(-1);
  const [suggestions, setSuggestions] = useState<SuggestResult[]>([]);
  const [previews, setPreviews] = useState<SearchResult[]>([]);
  const { ensure, status, search } = useDocketSearch();

  // Load recents once on mount (avoids hydration mismatch by doing it
  // client-only after first render).
  const [recents, setRecents] = useState<string[]>([]);
  useEffect(() => setRecents(readRecents()), []);

  // `/` shortcut to focus
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "/" || e.metaKey || e.ctrlKey || e.altKey) return;
      const t = e.target as HTMLElement | null;
      const tag = t?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || t?.isContentEditable) return;
      e.preventDefault();
      inputRef.current?.focus();
      inputRef.current?.select();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Close dropdown on outside click
  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (!rootRef.current?.contains(e.target as Node)) setOpen(false);
    };
    window.addEventListener("mousedown", onClick);
    return () => window.removeEventListener("mousedown", onClick);
  }, [open]);

  // Debounced suggest lookup
  useEffect(() => {
    if (!open) return;
    if (value.trim().length < 2) {
      setSuggestions([]);
      setPreviews([]);
      return;
    }
    const handle = setTimeout(async () => {
      try {
        const s = search ?? (await ensure());
        setSuggestions(s.suggest(value, 5));
        setPreviews(s.search(value, { limit: PREVIEW_LIMIT }));
      } catch {
        setSuggestions([]);
        setPreviews([]);
      }
    }, SUGGEST_DEBOUNCE_MS);
    return () => clearTimeout(handle);
  }, [value, open, ensure, search]);

  const commit = useCallback(
    (q: string) => {
      const trimmed = q.trim();
      if (!trimmed) return;
      writeRecent(trimmed);
      setOpen(false);
      setValue(trimmed);
      onCommit?.(trimmed);
      router.push(`/search?q=${encodeURIComponent(trimmed)}`);
    },
    [onCommit, router],
  );

  const items = useMemo<DropdownItem[]>(() => {
    if (value.trim().length < 2) {
      return recents.map((r) => ({ kind: "recent", text: r }));
    }
    const out: DropdownItem[] = [];
    for (const p of previews) out.push({ kind: "preview", result: p });
    for (const s of suggestions) {
      if (previews.length === 0 || !out.some((o) => o.kind === "suggest" && o.text === s.suggestion)) {
        out.push({ kind: "suggest", text: s.suggestion });
      }
    }
    return out;
  }, [value, recents, previews, suggestions]);

  const onKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setOpen(true);
      setActiveIdx((i) => Math.min(items.length - 1, i + 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActiveIdx((i) => Math.max(-1, i - 1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      const active = activeIdx >= 0 ? items[activeIdx] : null;
      if (active?.kind === "preview") {
        setOpen(false);
        writeRecent(value.trim());
        router.push(`/sr/${active.result.docket.agencyCode}/${active.result.docket.docketId}`);
      } else if (active && "text" in active) {
        commit(active.text);
      } else {
        commit(value);
      }
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  };

  const hint =
    status === "loading"
      ? "Loading search…"
      : status === "error"
        ? "Search unavailable"
        : null;

  return (
    <div ref={rootRef} className={`relative ${className}`}>
      <div className="relative">
        <Search
          size={14}
          className="absolute left-3 top-1/2 -translate-y-1/2 text-[var(--muted)] pointer-events-none"
        />
        <input
          ref={inputRef}
          type="text"
          role="combobox"
          aria-expanded={open}
          aria-autocomplete="list"
          aria-controls="search-dropdown"
          value={value}
          placeholder={placeholder}
          autoFocus={autoFocus}
          onFocus={() => {
            setOpen(true);
            void ensure().catch(() => {});
          }}
          onChange={(e) => {
            setValue(e.target.value);
            setOpen(true);
            setActiveIdx(-1);
          }}
          onKeyDown={onKey}
          className="w-full pl-9 pr-3 py-1.5 text-sm bg-[var(--background)] border border-[var(--border)] rounded-full text-[var(--foreground)] placeholder:text-[var(--muted)] focus:outline-none focus:border-[var(--accent-primary)] transition-colors"
        />
      </div>

      {open && (items.length > 0 || hint) && (
        <div
          id="search-dropdown"
          role="listbox"
          className="absolute left-0 right-0 top-full mt-1 bg-[var(--surface)] border border-[var(--border)] rounded-xl shadow-lg z-50 overflow-hidden"
        >
          {hint && items.length === 0 && (
            <div className="px-3 py-2 text-xs text-[var(--muted)]">{hint}</div>
          )}
          {items.map((item, i) => {
            const isActive = i === activeIdx;
            const baseCls = `block w-full text-left px-3 py-2 text-xs transition-colors ${
              isActive
                ? "bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]"
                : "text-[var(--foreground)] hover:bg-[var(--surface-elevated)]"
            }`;
            if (item.kind === "preview") {
              const d = item.result.docket;
              return (
                <button
                  key={`p-${i}`}
                  onMouseEnter={() => setActiveIdx(i)}
                  onClick={() => {
                    setOpen(false);
                    writeRecent(value.trim());
                    router.push(`/sr/${d.agencyCode}/${d.docketId}`);
                  }}
                  className={baseCls}
                >
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-[10px] px-1 py-0.5 rounded bg-[var(--surface-raised)] text-[var(--muted)]">
                      {d.agencyCode}
                    </span>
                    <span className="truncate">{d.title}</span>
                  </div>
                </button>
              );
            }
            if (item.kind === "suggest") {
              return (
                <button
                  key={`s-${i}`}
                  onMouseEnter={() => setActiveIdx(i)}
                  onClick={() => commit(item.text)}
                  className={baseCls}
                >
                  <span className="text-[var(--muted)] mr-1">Search:</span>
                  {item.text}
                </button>
              );
            }
            return (
              <button
                key={`r-${i}`}
                onMouseEnter={() => setActiveIdx(i)}
                onClick={() => commit(item.text)}
                className={baseCls}
              >
                <span className="text-[var(--muted)] mr-1">Recent:</span>
                {item.text}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
