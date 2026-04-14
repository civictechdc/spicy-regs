"use client";

import { Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";

import { Header } from "@/components/Header";
import { SearchInput } from "@/components/SearchInput";
import {
  SearchResultCard,
  SearchResultCardSkeleton,
} from "@/components/SearchResultCard";
import { useDocketSearch } from "@/lib/search/useDocketSearch";
import type { SearchResult, SearchSort } from "@/lib/search/types";

const PAGE_SIZE = 20;

function SkeletonList({ n = 5 }: { n?: number }) {
  return (
    <div className="space-y-3">
      {Array.from({ length: n }).map((_, i) => (
        <SearchResultCardSkeleton key={i} />
      ))}
    </div>
  );
}

function SearchBody() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const query = searchParams.get("q") ?? "";
  const agency = searchParams.get("agency") ?? "";
  const sortParam = (searchParams.get("sort") as SearchSort | null) ?? "relevance";

  const { ensure, status, search, error } = useDocketSearch();

  const [pageCount, setPageCount] = useState(1);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [total, setTotal] = useState(0);
  const [searching, setSearching] = useState(false);

  // Reset pagination when query/sort/agency changes
  useEffect(() => {
    setPageCount(1);
  }, [query, sortParam, agency]);

  // Kick off index load as soon as the page mounts with a query
  useEffect(() => {
    if (query) void ensure().catch(() => {});
  }, [query, ensure]);

  // Run the search when query/sort/agency/pageCount changes
  useEffect(() => {
    if (!query || !search) {
      setResults([]);
      setTotal(0);
      return;
    }
    setSearching(true);
    // Defer to next tick so the skeleton flickers don't stutter on fast queries
    const id = setTimeout(() => {
      const limit = pageCount * PAGE_SIZE;
      const hits = search.search(query, {
        limit,
        offset: 0,
        sort: sortParam,
        agency: agency || undefined,
      });
      const t = search.searchTotal(query, { agency: agency || undefined });
      setResults(hits);
      setTotal(t);
      setSearching(false);
    }, 0);
    return () => clearTimeout(id);
  }, [query, sortParam, agency, pageCount, search]);

  const setSort = useCallback(
    (s: SearchSort) => {
      const params = new URLSearchParams(searchParams);
      if (s === "relevance") params.delete("sort");
      else params.set("sort", s);
      router.replace(`/search?${params.toString()}`);
    },
    [router, searchParams],
  );

  const clearAgency = useCallback(() => {
    const params = new URLSearchParams(searchParams);
    params.delete("agency");
    router.replace(`/search?${params.toString()}`);
  }, [router, searchParams]);

  const hasMore = results.length < total;

  const emptyHint = useMemo(() => {
    if (!query) return "Use the search bar above to find regulations.";
    if (status === "error") return "Search is temporarily unavailable.";
    if (status !== "ready") return null;
    return `No dockets matched “${query}”.`;
  }, [query, status]);

  if (!query) {
    return (
      <div className="text-center py-16 text-[var(--muted)] text-sm">
        {emptyHint}
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* Search input mirror for on-page refinement */}
      <div>
        <SearchInput initialQuery={query} />
      </div>

      {/* Query echo + controls */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="text-base font-semibold">
          Results for <span className="gradient-text">&ldquo;{query}&rdquo;</span>
          {status === "ready" && total > 0 && (
            <span className="ml-2 text-xs font-normal text-[var(--muted)]">
              {total.toLocaleString()} {total === 1 ? "docket" : "dockets"}
            </span>
          )}
        </h2>

        <div className="flex items-center gap-2 text-xs">
          {agency && (
            <button
              onClick={clearAgency}
              className="px-2 py-1 rounded-full border border-[var(--accent-primary)]/40 bg-[var(--accent-primary)]/10 text-[var(--accent-primary)] hover:bg-[var(--accent-primary)]/20 transition-colors"
              title="Clear agency filter"
            >
              Agency: {agency} ✕
            </button>
          )}
          <label className="flex items-center gap-1.5 text-[var(--muted)]">
            Sort
            <select
              value={sortParam}
              onChange={(e) => setSort(e.target.value as SearchSort)}
              className="bg-[var(--background)] border border-[var(--border)] rounded-md px-2 py-1 text-[var(--foreground)] focus:outline-none focus:border-[var(--accent-primary)]"
            >
              <option value="relevance">Most relevant</option>
              <option value="recency">Most recently updated</option>
            </select>
          </label>
        </div>
      </div>

      {/* Loading / error / empty / results */}
      {status === "loading" && results.length === 0 && <SkeletonList />}

      {status === "error" && (
        <div className="card p-6 text-sm text-[var(--muted)]">
          Couldn&rsquo;t load the search index.
          {error?.message && (
            <span className="block mt-1 font-mono text-xs opacity-70">
              {error.message}
            </span>
          )}
        </div>
      )}

      {status === "ready" && results.length === 0 && !searching && (
        <div className="text-center py-16 text-[var(--muted)] text-sm">
          {emptyHint}
        </div>
      )}

      {results.length > 0 && (
        <>
          <div className="space-y-3">
            {results.map((r) => (
              <SearchResultCard key={r.docket.docketId} result={r} />
            ))}
          </div>

          <div className="flex items-center justify-between pt-2">
            <span className="text-xs text-[var(--muted)]">
              Showing {results.length.toLocaleString()} of {total.toLocaleString()}
            </span>
            {hasMore && (
              <button
                onClick={() => setPageCount((n) => n + 1)}
                className="text-xs px-3 py-1.5 rounded-full border border-[var(--border)] text-[var(--foreground)] hover:border-[var(--accent-primary)]/50 hover:text-[var(--accent-primary)] transition-colors"
              >
                Load more
              </button>
            )}
          </div>
        </>
      )}
    </div>
  );
}

export default function SearchPage() {
  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <Suspense fallback={<SkeletonList />}>
          <SearchBody />
        </Suspense>
      </main>
    </div>
  );
}
