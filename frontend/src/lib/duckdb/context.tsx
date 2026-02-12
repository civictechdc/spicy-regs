"use client";

import * as duckdb from "@duckdb/duckdb-wasm";
import {
  createContext,
  useContext,
  useEffect,
  useState,
  useCallback,
  useRef,
  type ReactNode,
} from "react";

const R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev";

// Iceberg catalog config (Worker proxy injects auth server-side)
const ICEBERG_PROXY_URL =
  process.env.NEXT_PUBLIC_ICEBERG_PROXY_URL ||
  "https://iceberg-proxy.simplyeugene94.workers.dev";
const ICEBERG_WAREHOUSE =
  process.env.NEXT_PUBLIC_ICEBERG_WAREHOUSE ||
  "a18589c7a7a0fc4febecadfc9c71b105_spicy-regs";

interface DuckDBContextValue {
  db: duckdb.AsyncDuckDB | null;
  conn: duckdb.AsyncDuckDBConnection | null;
  isReady: boolean;
  error: Error | null;
  runQuery: <T = Record<string, unknown>>(sql: string) => Promise<T[]>;
}

const DuckDBContext = createContext<DuckDBContextValue | null>(null);

async function initDuckDB(): Promise<{
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
}> {
  // Use CDN bundles for reliability
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

  // Select the best bundle for this browser
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: "text/javascript",
    })
  );

  // Create the worker and logger
  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();

  // Instantiate DuckDB
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  // Install and load httpfs for remote Parquet access
  await db.open({
    path: ":memory:",
    query: {
      castBigIntToDouble: true,
    },
  });

  const conn = await db.connect();

  // Install extensions
  await conn.query("INSTALL httpfs; LOAD httpfs;");
  await conn.query("INSTALL iceberg; LOAD iceberg;");

  // Set S3 region (required for httpfs even with public URLs)
  await conn.query("SET s3_region='auto';");

  // Attach Iceberg catalog via CORS proxy (auth is injected by the Worker)
  try {
    await conn.query(`
      ATTACH '${ICEBERG_WAREHOUSE}' AS spicy_regs (
        TYPE ICEBERG,
        ENDPOINT '${ICEBERG_PROXY_URL}',
        AUTHORIZATION_TYPE 'none'
      );
    `);
    await conn.query("USE spicy_regs.regulations;");
    console.log("[DuckDB-WASM] Iceberg catalog attached");
  } catch (err) {
    console.warn("[DuckDB-WASM] Iceberg attach failed, falling back to Parquet:", err);
  }

  console.log("[DuckDB-WASM] Initialized successfully");

  return { db, conn };
}

export function DuckDBProvider({ children }: { children: ReactNode }) {
  const [db, setDb] = useState<duckdb.AsyncDuckDB | null>(null);
  const [conn, setConn] = useState<duckdb.AsyncDuckDBConnection | null>(null);
  const [isReady, setIsReady] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const initRef = useRef(false);

  useEffect(() => {
    if (initRef.current) return;
    initRef.current = true;

    initDuckDB()
      .then(({ db, conn }) => {
        setDb(db);
        setConn(conn);
        setIsReady(true);
      })
      .catch((err) => {
        console.error("[DuckDB-WASM] Initialization failed:", err);
        setError(err);
      });
  }, []);

  const runQuery = useCallback(
    async <T = Record<string, unknown>>(sql: string): Promise<T[]> => {
      if (!conn) {
        throw new Error("DuckDB connection not ready");
      }

      try {
        const result = await conn.query(sql);
        return result.toArray().map((row) => row.toJSON() as T);
      } catch (err) {
        console.error("[DuckDB-WASM] Query failed:", sql, err);
        throw err;
      }
    },
    [conn]
  );

  return (
    <DuckDBContext.Provider value={{ db, conn, isReady, error, runQuery }}>
      {children}
    </DuckDBContext.Provider>
  );
}

export function useDuckDB() {
  const context = useContext(DuckDBContext);
  if (!context) {
    throw new Error("useDuckDB must be used within a DuckDBProvider");
  }
  return context;
}

// Export R2 URL for use in queries
export { R2_BASE_URL };
