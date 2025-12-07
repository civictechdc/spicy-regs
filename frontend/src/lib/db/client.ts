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
    const { createRequire } = await import('module');
    const require = createRequire(import.meta.url);
    const { Worker } = require('worker_threads');
    const path = require('path');
    const distPath = path.resolve(process.cwd(), 'node_modules/@duckdb/duckdb-wasm/dist');
    const workerPath = path.join(distPath, 'duckdb-node-mvp.worker.cjs');
    const wasmPath = path.join(distPath, 'duckdb-mvp.wasm');
    
    mainModule = wasmPath;
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
  
  if (!connInstance) {
     connInstance = await db.connect();
  }
  
  const result = await connInstance.query(sql);
  const rows = result.toArray().map((row: any) => row.toJSON());
  
  return rows as T[];
}

export async function initDb() {
  const db = await getDb();
  console.log("DuckDB WASM initialized");
}
