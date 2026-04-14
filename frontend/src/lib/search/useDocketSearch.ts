"use client";

import { useCallback, useState } from "react";

import { DocketSearch } from "./index";

type Status = "idle" | "loading" | "ready" | "error";

let _singleton: DocketSearch | null = null;
let _loadPromise: Promise<DocketSearch> | null = null;

/**
 * Lazy-load the search index exactly once per browser session.
 * Call `ensure()` to kick off the load (safe to call repeatedly).
 */
export function useDocketSearch() {
  const [search, setSearch] = useState<DocketSearch | null>(() => _singleton);
  const [status, setStatus] = useState<Status>(() =>
    _singleton ? "ready" : "idle",
  );
  const [error, setError] = useState<Error | null>(null);

  const ensure = useCallback(async (): Promise<DocketSearch> => {
    if (_singleton) return _singleton;
    if (!_loadPromise) {
      setStatus("loading");
      _loadPromise = DocketSearch.load()
        .then((s) => {
          _singleton = s;
          return s;
        })
        .catch((e) => {
          _loadPromise = null;
          throw e;
        });
    }
    try {
      const s = await _loadPromise;
      setSearch(s);
      setStatus("ready");
      return s;
    } catch (e) {
      setError(e as Error);
      setStatus("error");
      throw e;
    }
  }, []);

  return { ensure, status, error, search };
}
