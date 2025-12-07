import * as duckdb from '@duckdb/duckdb-wasm';
import { AsyncDuckDB, ConsoleLogger } from '@duckdb/duckdb-wasm';

let dbInstance: AsyncDuckDB | null = null;
let connInstance: duckdb.AsyncDuckDBConnection | null = null;

const isBrowser = typeof window !== 'undefined';

export async function getDb(): Promise<AsyncDuckDB> {
  if (dbInstance) return dbInstance;

  const logger = new ConsoleLogger();
  let worker: any; 
  let mainModule: string;
  let pthreadWorker: string | null | undefined;

  if (isBrowser) {
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    
    worker = new Worker(bundle.mainWorker!);
    mainModule = bundle.mainModule;
    pthreadWorker = bundle.pthreadWorker;
  } else {
    // Node.js configuration
    const { createRequire } = await import('module');
    const require = createRequire(import.meta.url);
    const { Worker } = require('worker_threads');
    const path = require('path');
    
    // Resolve path to the package distribution
    // Using process.cwd() to locate node_modules in the project root
    const distPath = path.resolve(process.cwd(), 'node_modules/@duckdb/duckdb-wasm/dist');
    
    // Use MVP bundle for compatibility in Node
    const workerPath = path.join(distPath, 'duckdb-node-mvp.worker.cjs');
    const wasmPath = path.join(distPath, 'duckdb-mvp.wasm');
    
    mainModule = wasmPath;
    
    // Wrapper to adapt Node worker to Browser Worker API
    class NodeWorkerWrapper {
      private worker: any;
      
      constructor(workerPath: string) {
        this.worker = new Worker(workerPath);
      }
      
      postMessage(message: any, transfer?: any[]) {
        this.worker.postMessage(message, transfer);
      }
      
      addEventListener(type: string, listener: any) {
        this.worker.on(type, (data: any) => {
           // Adapt 'message' event data structure if needed
           if (type === 'message') {
             listener({ data });
           } else {
             listener(data);
           }
        });
      }
      
      removeEventListener(type: string, listener: any) {
        this.worker.off(type, listener);
      }
      
      terminate() {
        this.worker.terminate();
      }
    }
    
    worker = new NodeWorkerWrapper(workerPath);
  }

  dbInstance = new AsyncDuckDB(logger, worker);
  await dbInstance.instantiate(mainModule, pthreadWorker);

  return dbInstance;
}

export async function runQuery<T = any>(sql: string): Promise<T[]> {
  const db = await getDb();
  
  // Re-use connection or create new one.
  // For better concurrency safety in simple apps, we can just create/close or keep one.
  // WASM supports concurrent connections but single threaded execution usually (in MVP).
  if (!connInstance) {
     connInstance = await db.connect();
  }
  
  const result = await connInstance.query(sql);
  
  // Convert Arrow Table to JS objects
  // result.toArray() returns an array of StructRowProxy (or similar)
  // We map them to clean JSON objects
  const rows = result.toArray().map((row: any) => row.toJSON());
  
  return rows as T[];
}

export async function initDb() {
  const db = await getDb();
  // Ensure httpfs if needed, though often included in bundles
  // await db.registerFileURL(...);
  console.log("DuckDB WASM initialized");
}
