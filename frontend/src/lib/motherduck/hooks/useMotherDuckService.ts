"use client";

import { useCallback, useRef, useState } from 'react';
import { useMotherDuckClientState } from '@/lib/motherduck/context/motherduckClientContext';
import { RegulationsDataTypes } from '@/lib/db/models';
import { buildBaseQuery, buildWhereClause } from '@/lib/db/utils';

/**
 * Hook that provides the same getData and searchResources logic from service.ts
 * but uses the MotherDuck context for query execution.
 * 
 * This reuses the existing query builders and table schemas from /lib/db.
 */
export function useMotherDuckService() {
  const { evaluateQuery: contextEvaluateQuery } = useMotherDuckClientState();
  const [isInitialized, setIsInitialized] = useState(false);
  const initPromiseRef = useRef<Promise<void> | null>(null);

  /**
   * Execute a query and return rows
   */
  const runQuery = useCallback(async <T = any>(sql: string): Promise<T[]> => {
    try {
      const result = await contextEvaluateQuery(sql);
      return result.data.toRows() as T[];
    } catch (error) {
      console.error('[MotherDuck] Query failed:', sql, error);
      throw error;
    }
  }, [contextEvaluateQuery]);

  /**
   * Create empty tables (same logic as config.ts createEmptyTables)
   */
  const createEmptyTables = useCallback(async () => {
    console.log("[MotherDuck] Creating empty tables...");
    
    for (const dataType of Object.values(RegulationsDataTypes)) {
      const tableName = `${dataType}_cache`;
      
      const columns = [
        "agency_code VARCHAR",
        "docket_id VARCHAR",
        "year VARCHAR",
        "raw_json TEXT",
      ];

      if (dataType === RegulationsDataTypes.Dockets) {
        columns.push("docket_type VARCHAR", "modify_date TIMESTAMP", "title TEXT");
      } else if (dataType === RegulationsDataTypes.Comments) {
        columns.push(
          "comment_id VARCHAR",
          "category VARCHAR",
          "comment TEXT",
          "document_type VARCHAR",
          "modify_date TIMESTAMP",
          "posted_date TIMESTAMP",
          "receive_date TIMESTAMP",
          "subtype VARCHAR",
          "title TEXT",
          "withdrawn BOOLEAN"
        );
      } else if (dataType === RegulationsDataTypes.Documents) {
        columns.push(
          "document_id VARCHAR",
          "category VARCHAR",
          "document_type VARCHAR",
          "comment_start_date TIMESTAMP",
          "comment_end_date TIMESTAMP",
          "modify_date TIMESTAMP",
          "posted_date TIMESTAMP",
          "receive_date TIMESTAMP",
          "page_count INTEGER",
          "withdrawn BOOLEAN",
          "title TEXT"
        );
      }

      columns.push("cached_at TIMESTAMP");
      const columnDefs = columns.join(",\n      ");
      
      const query = `
        CREATE TABLE IF NOT EXISTS ${tableName} (
          ${columnDefs}
        )
      `;
      
      await runQuery(query);
    }
    console.log("[MotherDuck] Tables created successfully!");
  }, [runQuery]);

  /**
   * Create indexes (same logic as config.ts createIndexes)
   */
  const createIndexes = useCallback(async () => {
    console.log("[MotherDuck] Creating indexes...");
    for (const dataType of Object.values(RegulationsDataTypes)) {
      const tableName = `${dataType}_cache`;
      try {
        await runQuery(`CREATE INDEX IF NOT EXISTS idx_${dataType}_agency ON ${tableName}(agency_code)`);
        await runQuery(`CREATE INDEX IF NOT EXISTS idx_${dataType}_docket_id ON ${tableName}(docket_id)`);
      } catch (e) {
        // Indexes might already exist
        console.log(`[MotherDuck] Index creation skipped for ${tableName}:`, e);
      }
    }
    console.log("[MotherDuck] Indexes created successfully!");
  }, [runQuery]);

  /**
   * Initialize database (same logic as config.ts initializeDatabase)
   */
  const initializeDatabase = useCallback(async () => {
    // Prevent multiple concurrent initializations
    if (initPromiseRef.current) {
      return initPromiseRef.current;
    }

    if (isInitialized) {
      return;
    }

    initPromiseRef.current = (async () => {
      try {
        await createEmptyTables();
        await createIndexes();
        
        setIsInitialized(true);
        console.log("[MotherDuck] Database initialized successfully!");
      } finally {
        initPromiseRef.current = null;
      }
    })();

    return initPromiseRef.current;
  }, [createEmptyTables, createIndexes, runQuery, isInitialized]);

  /**
   * Refresh cache (same logic as service.ts refreshCache)
   */
  const refreshCache = useCallback(async (
    agencyCode: string,
    docketId: string | undefined,
    dataType: RegulationsDataTypes
  ) => {
    if (!agencyCode) {
      throw new Error("agency_code is required");
    }

    const tableName = `${dataType}_cache`;
    const whereClause = buildWhereClause(agencyCode, docketId);

    console.log(`[MotherDuck] Refreshing ${dataType} cache for agency: ${agencyCode}, docket: ${docketId}`);

    // Delete existing
    await runQuery(`DELETE FROM ${tableName} WHERE ${whereClause}`);

    // Build query to get data from S3
    const query = buildBaseQuery(dataType, agencyCode, docketId);
    
    // Check if any records exist
    const countResult = await runQuery<{ count: number }>(`SELECT COUNT(*) as count FROM (${query}) q`);
    const countRow = countResult[0];
    const count = countRow ? Object.values(countRow)[0] : 0;

    if (Number(count) === 0) {
      console.warn(`[MotherDuck] No records found for agency ${agencyCode}`);
      return;
    }

    console.log(`[MotherDuck] Inserting ${count} records into ${tableName}`);
    
    await runQuery(`
      INSERT INTO ${tableName}
      SELECT *, CURRENT_TIMESTAMP as cached_at
      FROM (${query}) q
    `);
    
    console.log(`[MotherDuck] Refreshed ${dataType} cache successfully`);
  }, [runQuery]);

  /**
   * Get data with smart caching (same logic as service.ts getData)
   */
  const getData = useCallback(async (
    dataType: RegulationsDataTypes,
    agencyCode: string,
    docketId?: string,
    maxCacheAgeHours = Infinity
  ): Promise<any[]> => {
    // Ensure tables exist
    await initializeDatabase();

    const tableName = `${dataType}_cache`;
    const whereClause = buildWhereClause(agencyCode, docketId);

    // Check cache age
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
      console.log("[MotherDuck] Using cached data");
      const query = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
      return runQuery(query);
    } else {
      // Refresh cache
      try {
        await refreshCache(agencyCode, docketId, dataType);
        const query = `SELECT * FROM ${tableName} WHERE ${whereClause}`;
        return runQuery(query);
      } catch (e) {
        console.error("[MotherDuck] Error refreshing cache, falling back to live query", e);
        // Fallback to live query from S3
        let query = buildBaseQuery(dataType, agencyCode, docketId);
        if (agencyCode || docketId) {
          const wc = buildWhereClause(agencyCode, docketId)
            .replace(/agency_code/g, 'f.agency_code')
            .replace(/docket_id/g, 'f.docket_id');
          query += ` WHERE ${wc}`;
        }
        return runQuery(query);
      }
    }
  }, [initializeDatabase, refreshCache, runQuery]);

  /**
   * Search resources (same logic as service.ts searchResources)
   */
  const searchResources = useCallback(async (
    query: string,
    agencyCode?: string
  ): Promise<any[]> => {
    await initializeDatabase();
    
    const tables = ['dockets_cache', 'documents_cache', 'comments_cache'];
    const escapedQuery = query.replace(/'/g, "''");
    
    const unionQueries = tables.map(table => {
      let where = `title ILIKE '%${escapedQuery}%'`;
      if (agencyCode) {
        where += ` AND agency_code = '${agencyCode}'`;
      }
      const type = table.split('_')[0];
      return `
        SELECT 
          '${type}' as type,
          title, 
          docket_id, 
          agency_code,
          year,
          raw_json
        FROM ${table}
        WHERE ${where}
      `;
    });
    
    const fullQuery = unionQueries.join(" UNION ALL ") + " LIMIT 50";
    
    try {
      return await runQuery(fullQuery);
    } catch (e) {
      console.error("[MotherDuck] Search query failed", e);
      return [];
    }
  }, [initializeDatabase, runQuery]);

  return {
    runQuery,
    initializeDatabase,
    getData,
    searchResources,
    refreshCache,
    isInitialized,
  };
}
