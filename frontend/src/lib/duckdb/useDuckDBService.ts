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

  return {
    getData,
    getDataCount,
    searchResources,
    isReady,
  };
}
