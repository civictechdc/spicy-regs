"use client";

import { useCallback } from "react";
import { useDuckDB, R2_BASE_URL } from "./context";
import { RegulationsDataTypes } from "@/lib/db/models";

/** Default row limit to prevent OOM in the browser */
const DEFAULT_LIMIT = 1000;

/**
 * Hook that provides data fetching via DuckDB-WASM.
 *
 * When the Iceberg catalog is attached (via `USE spicy_regs.regulations`),
 * queries reference table names directly. Falls back to read_parquet() if
 * the catalog isn't available.
 */
export function useDuckDBService() {
  const { runQuery, isReady } = useDuckDB();

  /**
   * Build a table source — Iceberg table name if catalog is attached,
   * otherwise read_parquet() URL. The context sets USE so bare table names
   * resolve to the Iceberg catalog.
   */
  const tableRef = (dataType: RegulationsDataTypes) => dataType;

  /**
   * Fallback to raw Parquet if Iceberg isn't available.
   * Components can call this explicitly if needed.
   */
  const parquetRef = (dataType: RegulationsDataTypes) =>
    `read_parquet('${R2_BASE_URL}/${dataType}.parquet')`;

  /**
   * Get data with filters and pagination.
   * Enforces a default LIMIT to prevent large result sets from OOM.
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

      const conditions: string[] = [];
      if (agencyCode) {
        conditions.push(`agency_code = '${agencyCode.toUpperCase()}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const effectiveLimit = limit ?? DEFAULT_LIMIT;

      let query = `SELECT * FROM ${tableRef(dataType)} ${whereClause} LIMIT ${effectiveLimit}`;
      if (offset !== undefined) {
        query += ` OFFSET ${offset}`;
      }

      try {
        return await runQuery(query);
      } catch (err) {
        console.warn("[DuckDB] Iceberg query failed, falling back to Parquet:", err);
        let fallback = `SELECT * FROM ${parquetRef(dataType)} ${whereClause} LIMIT ${effectiveLimit}`;
        if (offset !== undefined) fallback += ` OFFSET ${offset}`;
        return runQuery(fallback);
      }
    },
    [runQuery, isReady]
  );

  /**
   * Get count of records (never materializes rows).
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

      const conditions: string[] = [];
      if (agencyCode) {
        conditions.push(`agency_code = '${agencyCode.toUpperCase()}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const query = `SELECT COUNT(*) as count FROM ${tableRef(dataType)} ${whereClause}`;

      try {
        const result = await runQuery<{ count: number }>(query);
        return Number(result[0]?.count ?? 0);
      } catch {
        const fallback = `SELECT COUNT(*) as count FROM ${parquetRef(dataType)} ${whereClause}`;
        const result = await runQuery<{ count: number }>(fallback);
        return Number(result[0]?.count ?? 0);
      }
    },
    [runQuery, isReady]
  );

  /**
   * Search across all resource types (capped at 50 results).
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
          FROM ${table}
          WHERE ${where}
        `;
      });

      const fullQuery = unionQueries.join(" UNION ALL ") + " LIMIT 50";

      try {
        return await runQuery(fullQuery);
      } catch {
        // Fallback: use read_parquet()
        const fallbackQueries = tables.map((table) => {
          let where = `title ILIKE '%${escapedQuery}%'`;
          if (agencyCode) where += ` AND agency_code = '${agencyCode}'`;
          return `
            SELECT '${table}' as type, title, docket_id, agency_code
            FROM ${parquetRef(table as RegulationsDataTypes)}
            WHERE ${where}
          `;
        });
        return runQuery(fallbackQueries.join(" UNION ALL ") + " LIMIT 50");
      }
    },
    [runQuery, isReady]
  );

  /**
   * Get list of agencies.
   */
  const getAgencies = useCallback(async (): Promise<string[]> => {
    if (!isReady) {
      throw new Error("DuckDB not ready");
    }

    const query = `SELECT DISTINCT agency_code FROM ${tableRef("dockets" as RegulationsDataTypes)} ORDER BY agency_code`;

    try {
      const result = await runQuery<{ agency_code: string }>(query);
      return result.map((r) => r.agency_code);
    } catch {
      const fallback = `SELECT DISTINCT agency_code FROM ${parquetRef("dockets" as RegulationsDataTypes)} ORDER BY agency_code`;
      const result = await runQuery<{ agency_code: string }>(fallback);
      return result.map((r) => r.agency_code);
    }
  }, [runQuery, isReady]);

  /**
   * Get list of dockets for an agency.
   */
  const getDockets = useCallback(
    async (agencyCode: string): Promise<string[]> => {
      if (!isReady) {
        throw new Error("DuckDB not ready");
      }

      const query = `SELECT DISTINCT docket_id FROM ${tableRef("dockets" as RegulationsDataTypes)} WHERE agency_code = '${agencyCode.toUpperCase()}' ORDER BY docket_id DESC`;

      try {
        const result = await runQuery<{ docket_id: string }>(query);
        return result.map((r) => r.docket_id);
      } catch {
        const fallback = `SELECT DISTINCT docket_id FROM ${parquetRef("dockets" as RegulationsDataTypes)} WHERE agency_code = '${agencyCode.toUpperCase()}' ORDER BY docket_id DESC`;
        const result = await runQuery<{ docket_id: string }>(fallback);
        return result.map((r) => r.docket_id);
      }
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
