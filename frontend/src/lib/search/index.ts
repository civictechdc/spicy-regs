import MiniSearch, { SearchResult as MSResult } from "minisearch";
import { get, set } from "idb-keyval";

import type {
  Docket,
  SearchOptions,
  SearchResult,
  SuggestResult,
} from "./types";

const R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev";
const INDEX_FILENAME = "docket_search.json.gz";
const CACHE_KEY = "docket-search-v1";

/**
 * Resolve the URL of the search index. In dev, dropping the file at
 * `frontend/public/docket_search.json.gz` works without touching R2; in
 * prod the R2 URL is used. Override with NEXT_PUBLIC_SEARCH_INDEX_URL.
 */
function resolveIndexUrl(baseUrl?: string): string {
  if (baseUrl) return `${baseUrl}/${INDEX_FILENAME}`;
  const override = process.env.NEXT_PUBLIC_SEARCH_INDEX_URL;
  if (override) return override;
  return `${R2_BASE_URL}/${INDEX_FILENAME}`;
}

type RawDoc = {
  id: string;
  a: string;
  t: string;
  x: string;
  d: string;
  s?: string;
};

type RawPayload = {
  version: string;
  count: number;
  docs: RawDoc[];
};

type CachedIndex = {
  version: string;
  serialized: string;
  docs: Record<string, Docket>;
};

type MSDoc = {
  id: string;
  title: string;
  abstract: string;
  agency: string;
};

const MS_CONFIG = {
  fields: ["title", "abstract"],
  storeFields: ["id"],
  idField: "id",
  searchOptions: {
    boost: { title: 3 },
    prefix: true,
    fuzzy: 0.2,
  },
};

function expandDoc(raw: RawDoc): Docket {
  return {
    docketId: raw.id,
    agencyCode: raw.a,
    title: raw.t,
    docketType: raw.x,
    modifyDate: raw.d,
    abstract: raw.s ?? "",
  };
}

async function fetchAndDecompress(url: string): Promise<RawPayload> {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to fetch search index: ${res.status}`);
  }

  // R2 often auto-sets Content-Encoding: gzip for .gz files, in which
  // case fetch decompresses transparently and res.json() just works.
  // When it doesn't (e.g. local static serving), decompress manually.
  const contentEncoding = res.headers.get("content-encoding");
  if (contentEncoding?.includes("gzip")) {
    return (await res.json()) as RawPayload;
  }

  const buf = await res.arrayBuffer();
  const stream = new Response(buf).body!.pipeThrough(
    new DecompressionStream("gzip"),
  );
  const text = await new Response(stream).text();
  return JSON.parse(text) as RawPayload;
}

export class DocketSearch {
  private ms: MiniSearch<MSDoc>;
  private docs: Map<string, Docket>;
  readonly version: string;

  private constructor(
    ms: MiniSearch<MSDoc>,
    docs: Map<string, Docket>,
    version: string,
  ) {
    this.ms = ms;
    this.docs = docs;
    this.version = version;
  }

  static async load(baseUrl?: string): Promise<DocketSearch> {
    const url = resolveIndexUrl(baseUrl);

    // Try cache first — if anything is broken we fall through to a fresh fetch
    try {
      const cached = await get<CachedIndex>(CACHE_KEY);
      if (cached) {
        const ms = MiniSearch.loadJSON<MSDoc>(cached.serialized, MS_CONFIG);
        const docs = new Map(Object.entries(cached.docs));
        // Kick off a background version check so a subsequent page load
        // picks up a fresh index, but don't block returning the cached one.
        void refreshIfStale(url, cached.version);
        return new DocketSearch(ms, docs, cached.version);
      }
    } catch (e) {
      console.warn("[search] cache read failed, fetching fresh", e);
    }

    const payload = await fetchAndDecompress(url);
    const { ms, docs } = buildIndex(payload);
    // Fire-and-forget cache write
    void set(CACHE_KEY, {
      version: payload.version,
      serialized: JSON.stringify(ms),
      docs: Object.fromEntries(docs),
    } satisfies CachedIndex).catch((e) =>
      console.warn("[search] cache write failed", e),
    );

    return new DocketSearch(ms, docs, payload.version);
  }

  get size(): number {
    return this.docs.size;
  }

  search(query: string, opts: SearchOptions = {}): SearchResult[] {
    const { limit = 20, offset = 0, sort = "relevance", agency } = opts;
    if (!query.trim()) return [];

    const hits = this.ms.search(query) as MSResult[];

    const filtered: Array<{ hit: MSResult; docket: Docket }> = [];
    for (const hit of hits) {
      const docket = this.docs.get(String(hit.id));
      if (!docket) continue;
      if (agency && docket.agencyCode !== agency) continue;
      filtered.push({ hit, docket });
    }

    if (sort === "recency") {
      filtered.sort((a, b) =>
        (b.docket.modifyDate || "").localeCompare(a.docket.modifyDate || ""),
      );
    }

    return filtered.slice(offset, offset + limit).map(({ hit, docket }) => ({
      docket,
      score: hit.score,
      matchedTerms: hit.terms ?? [],
    }));
  }

  searchTotal(query: string, opts: { agency?: string } = {}): number {
    if (!query.trim()) return 0;
    const hits = this.ms.search(query) as MSResult[];
    if (!opts.agency) return hits.length;
    let n = 0;
    for (const hit of hits) {
      const docket = this.docs.get(String(hit.id));
      if (docket && docket.agencyCode === opts.agency) n++;
    }
    return n;
  }

  suggest(query: string, limit = 5): SuggestResult[] {
    const q = query.trim();
    if (q.length < 2) return [];
    return this.ms
      .autoSuggest(q, { fuzzy: 0.2, prefix: true })
      .slice(0, limit)
      .map((s) => ({ suggestion: s.suggestion, score: s.score }));
  }
}

function buildIndex(payload: RawPayload): {
  ms: MiniSearch<MSDoc>;
  docs: Map<string, Docket>;
} {
  const ms = new MiniSearch<MSDoc>(MS_CONFIG);
  const docs = new Map<string, Docket>();

  const msDocs: MSDoc[] = [];
  for (const raw of payload.docs) {
    const docket = expandDoc(raw);
    docs.set(docket.docketId, docket);
    msDocs.push({
      id: docket.docketId,
      title: docket.title,
      abstract: docket.abstract,
      agency: docket.agencyCode,
    });
  }
  ms.addAll(msDocs);
  return { ms, docs };
}

async function refreshIfStale(url: string, knownVersion: string): Promise<void> {
  try {
    const res = await fetch(url, { cache: "no-cache" });
    if (!res.ok) return;
    // Peek at just the first ~200 bytes to read the version field without
    // downloading the whole index.
    const contentEncoding = res.headers.get("content-encoding");
    let text: string;
    if (contentEncoding?.includes("gzip")) {
      const body = await res.text();
      text = body.slice(0, 200);
    } else {
      const buf = await res.arrayBuffer();
      const stream = new Response(buf).body!.pipeThrough(
        new DecompressionStream("gzip"),
      );
      const reader = stream.getReader();
      const chunk = await reader.read();
      text = new TextDecoder().decode(chunk.value);
    }
    const match = text.match(/"version":"([^"]+)"/);
    if (match && match[1] !== knownVersion) {
      console.info(
        `[search] newer index available (${match[1]} vs cached ${knownVersion}); will refresh on next load`,
      );
      // Invalidate the cache; next load() call rebuilds from scratch.
      void set(CACHE_KEY, undefined).catch(() => {});
    }
  } catch (e) {
    console.warn("[search] version check failed", e);
  }
}
