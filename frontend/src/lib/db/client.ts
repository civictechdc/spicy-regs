import duckdb from 'duckdb';

const DB_PATH = process.env.NODE_ENV === 'production' ? ':memory:' : 'main.db';
// Note: On Vercel, writing to disk is limited to /tmp. 
// For now using :memory: to avoid complex path handling, 
// but this means cache is only per-request/warm-instance.
// If persistence is needed, we should point to /tmp/main.db and ensure directory exists.

let dbInstance: duckdb.Database | null = null;

export function getDb(): duckdb.Database {
  if (!dbInstance) {
    dbInstance = new duckdb.Database(DB_PATH);
    
    // We need to ensure extensions are loaded.
    // This is async in nature but the constructor is sync.
    // Usage usually requires waiting for connection or running a query.
    // We'll wrap initialization in a way that ensures these run.
  }
  return dbInstance;
}

// Wrapper to run queries with Promises
export function runQuery<T = any>(sql: string): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const db = getDb();
    // we use 'all' for SELECTs. 
    // duckdb-node also has 'exec' for statements, 'run' for inserts?
    // 'all' works for most things returning rows.
    db.all(sql, (err, res) => {
      if (err) {
        reject(err);
      } else {
        resolve(res as T[]);
      }
    });
  });
}

// Helper to initialize extensions
let initialized = false;
export async function initDb() {
  if (initialized) return;
  
  // Install and load httpfs for S3 access
  // Note: This might timeout or fail if network is blocked, but Vercel allows outbound.
  try {
    await runQuery("INSTALL httpfs;");
    await runQuery("LOAD httpfs;");
    initialized = true;
    console.log("DuckDB initialized with httpfs");
  } catch (err) {
    console.error("Failed to initialize DuckDB extensions:", err);
    throw err;
  }
}
