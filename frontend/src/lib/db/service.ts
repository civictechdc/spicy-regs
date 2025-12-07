import { runQuery, initDb, getDb } from "./client";
import { RegulationsDataTypes } from "./models";
import { buildBaseQuery, buildWhereClause } from "./utils";
import { initializeDatabase } from "./config";

// Helper to refresh cache
async function refreshCache(
  agencyCode: string,
  docketId: string | undefined, // docketId can be undefined/null
  dataType: RegulationsDataTypes
) {
  if (!agencyCode) {
    throw new Error("agency_code is required");
  }

  const tableName = `${dataType}_cache`;
  const whereClause = buildWhereClause(agencyCode, docketId);

  console.log(`Refreshing ${dataType} cache for agency code: ${agencyCode}, docket id: ${docketId}`);

  // Delete existing
  await runQuery(`DELETE FROM ${tableName} WHERE ${whereClause}`);

  // Insert new data
  // Note: We need to handle the case where fetching from S3 fails or returns empty.
  // The python code calculates query, then inserts.
  const query = buildBaseQuery(dataType, agencyCode, docketId);
  
  // Check if any records exist first (optimization)
  const countResult = await runQuery<{ count: number }>(`SELECT COUNT(*) as count FROM (${query}) q`);
  // DuckDB might return BigInt or similar, usually it returns an object per row
  // e.g. [{ count: 123 }]
  // Note: The count parsing might vary depending on driver version, checking...
  // Assuming standard return
  // Duckdb node returns { "count": bigint } usually if using 'count(*)'?
  // Or just values.
  
  // Safe extraction
  const countRow = countResult[0];
  const count = countRow ? Object.values(countRow)[0] : 0; // flexible access

  if (Number(count) === 0) {
     // In Python code it raises 404. We can throw error or return empty.
     // Throwing error allows upstream to handle 404
     console.warn(`No records found for agency ${agencyCode} to cache.`);
     // If we throw here, we stop.
     // throw new Error("No records found");
     return; // Just don't cache anything
  }

  console.log(`Inserting ${count} records into ${tableName}`);
  
  await runQuery(`
    INSERT INTO ${tableName}
    SELECT *, CURRENT_TIMESTAMP as cached_at
    FROM (${query}) q
  `);
  
  console.log(`Refreshed ${dataType} cache successfully`);
}

/**
 * Get data with smart caching based on age
 */
export async function getData(
  dataType: RegulationsDataTypes,
  agencyCode: string,
  docketId?: string,
  maxCacheAgeHours = Infinity
): Promise<any[]> {
  // Ensure DB and tables are ready
  await initDb();
  // We should ideally ensure tables exist only once, but `CREATE TABLE IF NOT EXISTS` is cheap.
  // For better performance, we might want a global initialized state for tables too.
  await initializeDatabase(); 

  const tableName = `${dataType}_cache`;
  const whereClause = buildWhereClause(agencyCode, docketId);

  // Check cache age
  // Result is [{ count: ..., last_updated: ..., age_hours: ... }]
  const cacheAgeResult = await runQuery(`
    SELECT 
        COUNT(*) as count,
        MAX(cached_at) as last_updated,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(cached_at)))/3600 as age_hours
    FROM ${tableName} 
    WHERE ${whereClause}
  `);

  const row = cacheAgeResult[0];
  const count = Number(Object.values(row)[0]);
  const ageHours = row.age_hours !== null ? Number(row.age_hours) : null;
  
  if (count > 0 && ageHours !== null && ageHours < maxCacheAgeHours) {
    // Use cached data
    console.log("Using cached data");
    const query = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
    return runQuery(query);
  } else {
    // Refresh cache
    try {
        await refreshCache(agencyCode, docketId, dataType);
        // Recursively call? Or just fetch what we just cached.
        // If refresh worked, we fetch from cache.
        const query = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
        return runQuery(query);
    } catch (e) {
        console.error("Error refreshing cache, falling back to live query", e);
        // Fallback to live query
        let query = buildBaseQuery(dataType, agencyCode, docketId);
        // We need to apply WHERE filter to the raw query result?
        // buildBaseQuery handles the S3 filtering via glob path if agency/docket are provided.
        // But if filtering within the file content is needed (DB utils did this? Python Utils line 53)
        // Python utils: if agency_code or docket_id: query += WHERE ...
        // buildBaseQuery constructs the inner select.
        // We might need to filter again if the glob pattern isn't precise enough?
        // Actually buildBaseQuery selects from read_text.
        // We should add the where clause if needed.
        if (agencyCode || docketId) {
             // We need variables to use aliases 'f.agency_code' etc which buildBaseQuery provides in the outer SELECT.
             const wc = buildWhereClause(agencyCode, docketId).replace(/agency_code/g, 'f.agency_code').replace(/docket_id/g, 'f.docket_id');
             query += ` WHERE ${wc}`;
        }
        return runQuery(query);
    }
  }
}
