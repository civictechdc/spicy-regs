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

  /**
   * Get recent dockets across all agencies (or filtered by agency).
   * Powers the main feed — ordered by modify_date DESC.
   */
  const getRecentDockets = useCallback(
    async (
      limit: number = 20,
      offset: number = 0,
      agencyCode?: string,
      sortBy: 'recent' | 'popular' | 'open' = 'recent'
    ): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const conditions: string[] = [];
      if (agencyCode) {
        conditions.push(`agency_code = '${agencyCode.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      let orderClause = 'ORDER BY modify_date DESC';
      if (sortBy === 'popular') {
        orderClause = 'ORDER BY modify_date DESC'; // same for now; could sort by comment count with a JOIN
      }

      const query = `SELECT * FROM ${tableRef("dockets" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;

      try {
        return await runQuery(query);
      } catch (err) {
        console.warn("[DuckDB] Iceberg query failed, falling back to Parquet:", err);
        const fallback = `SELECT * FROM ${parquetRef("dockets" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
        return runQuery(fallback);
      }
    },
    [runQuery, isReady]
  );

  /**
   * Get comments for a specific docket.
   * Powers the threaded comments section below each docket post.
   */
  const getCommentsForDocket = useCallback(
    async (
      docketId: string,
      limit: number = 10,
      offset: number = 0,
      sortBy: 'recent' | 'oldest' = 'recent'
    ): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const orderClause = sortBy === 'oldest'
        ? 'ORDER BY posted_date ASC'
        : 'ORDER BY posted_date DESC';

      const query = `SELECT * FROM ${tableRef("comments" as RegulationsDataTypes)} WHERE docket_id = '${docketId.toUpperCase()}' ${orderClause} LIMIT ${limit} OFFSET ${offset}`;

      try {
        return await runQuery(query);
      } catch {
        const fallback = `SELECT * FROM ${parquetRef("comments" as RegulationsDataTypes)} WHERE docket_id = '${docketId.toUpperCase()}' ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
        return runQuery(fallback);
      }
    },
    [runQuery, isReady]
  );

  /**
   * Get aggregate stats for an agency (docket, document, comment counts).
   * Powers the agency subreddit sidebar.
   */
  const getAgencyStats = useCallback(
    async (agencyCode: string): Promise<{ docketCount: number; documentCount: number; commentCount: number }> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const upper = agencyCode.toUpperCase();
      const tables = ["dockets", "documents", "comments"] as const;
      const counts: number[] = [];

      for (const table of tables) {
        const query = `SELECT COUNT(*) as count FROM ${tableRef(table as RegulationsDataTypes)} WHERE agency_code = '${upper}'`;
        try {
          const result = await runQuery<{ count: number }>(query);
          counts.push(Number(result[0]?.count ?? 0));
        } catch {
          const fallback = `SELECT COUNT(*) as count FROM ${parquetRef(table as RegulationsDataTypes)} WHERE agency_code = '${upper}'`;
          const result = await runQuery<{ count: number }>(fallback);
          counts.push(Number(result[0]?.count ?? 0));
        }
      }

      return {
        docketCount: counts[0],
        documentCount: counts[1],
        commentCount: counts[2],
      };
    },
    [runQuery, isReady]
  );

  return {
    getData,
    getDataCount,
    searchResources,
    getAgencies,
    getDockets,
    getRecentDockets,
    getCommentsForDocket,
    getAgencyStats,
    isReady,
  };
}
