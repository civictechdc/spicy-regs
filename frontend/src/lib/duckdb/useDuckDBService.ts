"use client";

import { useCallback } from "react";
import { useDuckDB, R2_BASE_URL } from "./context";
import { RegulationsDataTypes } from "@/lib/db/models";

/**
 * Hook that provides data fetching from R2 Parquet files via DuckDB-WASM.
 * Replaces the MotherDuckService hook.
 */
export function useDuckDBService() {
  const { runQuery, isReady } = useDuckDB();

  /**
   * Get data from R2 Parquet files
   */
  const getData = useCallback(
    async (
      dataType: RegulationsDataTypes,
      agencyCode: string,
      docketId?: string,
      _maxCacheAgeHours = Infinity,
      limit?: number,
      offset?: number
    ): Promise<any[]> => {
      if (!isReady) {
        throw new Error("DuckDB not ready");
      }

      const parquetFile = `${R2_BASE_URL}/${dataType}.parquet`;

      let whereClause = "";
      const conditions: string[] = [];

      if (agencyCode) {
        conditions.push(`agency_code = '${agencyCode.toUpperCase()}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      if (conditions.length > 0) {
        whereClause = `WHERE ${conditions.join(" AND ")}`;
      }

      let query = `SELECT * FROM read_parquet('${parquetFile}') ${whereClause}`;

      if (limit !== undefined) {
        query += ` LIMIT ${limit}`;
      }
      if (offset !== undefined) {
        query += ` OFFSET ${offset}`;
      }

      return runQuery(query);
    },
    [runQuery, isReady]
  );

  /**
   * Get count of records
   */
  const getDataCount = useCallback(
    async (
      dataType: RegulationsDataTypes,
      agencyCode: string,
      docketId?: string
    ): Promise<number> => {
      if (!isReady) {
        throw new Error("DuckDB not ready");
      }

      const parquetFile = `${R2_BASE_URL}/${dataType}.parquet`;

      let whereClause = "";
      const conditions: string[] = [];

      if (agencyCode) {
        conditions.push(`agency_code = '${agencyCode.toUpperCase()}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      if (conditions.length > 0) {
        whereClause = `WHERE ${conditions.join(" AND ")}`;
      }

      const query = `SELECT COUNT(*) as count FROM read_parquet('${parquetFile}') ${whereClause}`;
      const result = await runQuery<{ count: number }>(query);
      return Number(result[0]?.count ?? 0);
    },
    [runQuery, isReady]
  );

  /**
   * Search across all resource types
   */
  const searchResources = useCallback(
    async (searchQuery: string, agencyCode?: string): Promise<any[]> => {
      if (!isReady) {
        throw new Error("DuckDB not ready");
      }

      const escapedQuery = searchQuery.replace(/'/g, "''");

      const tables = ["dockets", "documents", "comments"] as const;

      const unionQueries = tables.map((table) => {
        let where = `title ILIKE '%${escapedQuery}%'`;
        if (agencyCode) {
          where += ` AND agency_code = '${agencyCode}'`;
        }
        return `
          SELECT 
            '${table}' as type,
            title, 
            docket_id, 
            agency_code
          FROM read_parquet('${R2_BASE_URL}/${table}.parquet')
          WHERE ${where}
        `;
      });

      const fullQuery = unionQueries.join(" UNION ALL ") + " LIMIT 50";
      return runQuery(fullQuery);
    },
    [runQuery, isReady]
  );

  /**
   * Get list of agencies from R2 Parquet files
   */
  const getAgencies = useCallback(async (): Promise<string[]> => {
    if (!isReady) {
      throw new Error("DuckDB not ready");
    }

    const parquetFile = `${R2_BASE_URL}/dockets.parquet`;
    const query = `SELECT DISTINCT agency_code FROM read_parquet('${parquetFile}') ORDER BY agency_code`;
    const result = await runQuery<{ agency_code: string }>(query);
    return result.map((r) => r.agency_code);
  }, [runQuery, isReady]);

  /**
   * Get list of dockets for an agency from R2 Parquet files
   */
  const getDockets = useCallback(
    async (agencyCode: string): Promise<string[]> => {
      if (!isReady) {
        throw new Error("DuckDB not ready");
      }

      const parquetFile = `${R2_BASE_URL}/dockets.parquet`;
      const query = `SELECT DISTINCT docket_id FROM read_parquet('${parquetFile}') WHERE agency_code = '${agencyCode.toUpperCase()}' ORDER BY docket_id DESC`;
      const result = await runQuery<{ docket_id: string }>(query);
      return result.map((r) => r.docket_id);
    },
    [runQuery, isReady]
  );

  return {
    getData,
    getDataCount,
    searchResources,
    getAgencies,
    getDockets,
    isReady,
  };
}
