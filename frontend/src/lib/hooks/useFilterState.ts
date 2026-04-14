'use client';

import { useCallback, useEffect, useState } from 'react';
import { usePathname, useRouter, useSearchParams } from 'next/navigation';

/**
 * Two-way binding between a piece of filter state, a URL query param,
 * and a localStorage key.
 *
 * Initial value precedence: URL param > localStorage > defaultValue.
 *
 * User-driven setValue() writes to BOTH the URL (via router.replace,
 * scroll preserved) AND localStorage. URL-driven changes (back/forward,
 * shared link load) update internal state but DO NOT touch localStorage —
 * a recipient opening a shared link must not have their saved preference
 * overwritten.
 *
 * When `next === defaultValue`, the param is removed from the URL so the
 * default state produces a clean URL with no query string clutter.
 *
 * Invalid URL values (failing `isValid`) are ignored at read time and
 * left in the URL until the user changes a filter — we don't aggressively
 * rewrite hand-edited URLs.
 */
export function useFilterState<T extends string>(
  paramKey: string,
  storageKey: string,
  defaultValue: T,
  isValid: (raw: string) => raw is T,
): [T, (next: T) => void] {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [value, setInternalValue] = useState<T>(() => {
    const fromUrl = searchParams.get(paramKey);
    if (fromUrl != null && isValid(fromUrl)) return fromUrl;
    if (typeof window !== 'undefined') {
      try {
        const fromStorage = window.localStorage.getItem(storageKey);
        if (fromStorage != null && isValid(fromStorage)) return fromStorage;
      } catch {
        // localStorage unavailable (private mode etc.) — fall through.
      }
    }
    return defaultValue;
  });

  // Sync from URL on external navigation. No localStorage write here.
  useEffect(() => {
    const fromUrl = searchParams.get(paramKey);
    const next = fromUrl != null && isValid(fromUrl) ? fromUrl : defaultValue;
    setInternalValue((current) => (current === next ? current : next));
  }, [searchParams, paramKey, defaultValue, isValid]);

  const setValue = useCallback(
    (next: T) => {
      setInternalValue(next);

      const params = new URLSearchParams(searchParams.toString());
      if (next === defaultValue) {
        params.delete(paramKey);
      } else {
        params.set(paramKey, next);
      }
      const qs = params.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });

      if (typeof window !== 'undefined') {
        try {
          window.localStorage.setItem(storageKey, next);
        } catch {
          // localStorage unavailable — silently skip persistence.
        }
      }
    },
    [searchParams, defaultValue, paramKey, pathname, router, storageKey],
  );

  return [value, setValue];
}
