"use client";

import { useCallback, useRef } from "react";
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

  /** Track whether Iceberg catalog is available — skip after first failure */
  const icebergAvailable = useRef(true);

  const tableRef = (dataType: RegulationsDataTypes) => dataType;

  const parquetRef = (dataType: RegulationsDataTypes) =>
    `read_parquet('${R2_BASE_URL}/${dataType}.parquet')`;

  /**
   * Try a query against Iceberg tables first; fall back to Parquet silently.
   * Once Iceberg fails once, all future queries go straight to Parquet.
   */
  async function queryWithFallback<T>(
    icebergQuery: string,
    parquetQuery: string
  ): Promise<T[]> {
    if (icebergAvailable.current) {
      try {
        return await runQuery<T>(icebergQuery);
      } catch {
        icebergAvailable.current = false;
      }
    }
    return runQuery<T>(parquetQuery);
  }

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

      let fallback = `SELECT * FROM ${parquetRef(dataType)} ${whereClause} LIMIT ${effectiveLimit}`;
      if (offset !== undefined) fallback += ` OFFSET ${offset}`;

      return queryWithFallback(query, fallback);
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
      const fallback = `SELECT COUNT(*) as count FROM ${parquetRef(dataType)} ${whereClause}`;

      const result = await queryWithFallback<{ count: number }>(query, fallback);
      return Number(result[0]?.count ?? 0);
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

      const fallbackQueries = tables.map((table) => {
        let where = `title ILIKE '%${escapedQuery}%'`;
        if (agencyCode) where += ` AND agency_code = '${agencyCode}'`;
        return `
          SELECT '${table}' as type, title, docket_id, agency_code
          FROM ${parquetRef(table as RegulationsDataTypes)}
          WHERE ${where}
        `;
      });
      const fallbackQuery = fallbackQueries.join(" UNION ALL ") + " LIMIT 50";

      return queryWithFallback(fullQuery, fallbackQuery);
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
    const fallback = `SELECT DISTINCT agency_code FROM ${parquetRef("dockets" as RegulationsDataTypes)} ORDER BY agency_code`;

    const result = await queryWithFallback<{ agency_code: string }>(query, fallback);
    return result.map((r) => r.agency_code);
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
      const fallback = `SELECT DISTINCT docket_id FROM ${parquetRef("dockets" as RegulationsDataTypes)} WHERE agency_code = '${agencyCode.toUpperCase()}' ORDER BY docket_id DESC`;

      const result = await queryWithFallback<{ docket_id: string }>(query, fallback);
      return result.map((r) => r.docket_id);
    },
    [runQuery, isReady]
  );

  /**
   * Get a single docket by its ID.
   */
  const getDocketById = useCallback(
    async (docketId: string): Promise<Record<string, any> | null> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const escaped = docketId.replace(/'/g, "''");
      const query = `SELECT * FROM ${tableRef("dockets" as RegulationsDataTypes)} WHERE docket_id = '${escaped}' LIMIT 1`;
      const fallback = `SELECT * FROM ${parquetRef("dockets" as RegulationsDataTypes)} WHERE docket_id = '${escaped}' LIMIT 1`;

      const results = await queryWithFallback<Record<string, any>>(query, fallback);
      return results[0] || null;
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
      const fallback = `SELECT * FROM ${parquetRef("dockets" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;

      return queryWithFallback(query, fallback);
    },
    [runQuery, isReady]
  );

  /**
   * Get comments for a specific docket.
   * Powers the threaded comments section below each docket post.
   * Handles both quoted and unquoted docket_id values in the data.
   */
  const getCommentsForDocket = useCallback(
    async (
      docketId: string,
      limit: number = 10,
      offset: number = 0,
      sortBy: 'recent' | 'oldest' = 'recent'
    ): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      // Strip any existing quotes from input
      const cleanId = docketId.replace(/^"|"$/g, '').toUpperCase();

      const orderClause = sortBy === 'oldest'
        ? 'ORDER BY posted_date ASC'
        : 'ORDER BY posted_date DESC';

      // Match both quoted ("EPA-HQ-...") and unquoted (EPA-HQ-...) forms
      const whereClause = `WHERE REPLACE(docket_id, '"', '') = '${cleanId}'`;

      const query = `SELECT * FROM ${tableRef("comments" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
      const fallback = `SELECT * FROM ${parquetRef("comments" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;

      return queryWithFallback(query, fallback);
    },
    [runQuery, isReady]
  );

  /**
   * Get comment counts for a batch of docket IDs.
   */
  const getCommentCounts = useCallback(
    async (docketIds: string[]): Promise<Record<string, number>> => {
      if (!isReady || docketIds.length === 0) return {};

      const cleanIds = docketIds.map(id => id.replace(/^"|"$/g, '').toUpperCase());
      const inClause = cleanIds.map(id => `'${id}'`).join(',');
      const query = `SELECT REPLACE(docket_id, '"', '') as did, COUNT(*) as cnt FROM ${tableRef("comments" as RegulationsDataTypes)} WHERE REPLACE(docket_id, '"', '') IN (${inClause}) GROUP BY did`;
      const fb = `SELECT REPLACE(docket_id, '"', '') as did, COUNT(*) as cnt FROM ${parquetRef("comments" as RegulationsDataTypes)} WHERE REPLACE(docket_id, '"', '') IN (${inClause}) GROUP BY did`;

      const rows = await queryWithFallback<{ did: string; cnt: number }>(query, fb);

      const result: Record<string, number> = {};
      for (const r of rows) {
        result[r.did] = Number(r.cnt);
      }
      return result;
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
        const fallback = `SELECT COUNT(*) as count FROM ${parquetRef(table as RegulationsDataTypes)} WHERE agency_code = '${upper}'`;
        const result = await queryWithFallback<{ count: number }>(query, fallback);
        counts.push(Number(result[0]?.count ?? 0));
      }

      return {
        docketCount: counts[0],
        documentCount: counts[1],
        commentCount: counts[2],
      };
    },
    [runQuery, isReady]
  );

  /**
   * Get agencies ranked by docket count (most dockets first).
   * Powers the "Popular Agencies" section on /agencies.
   */
  const getPopularAgencies = useCallback(
    async (limit: number = 4): Promise<{ agency_code: string; docket_count: number }[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const query = `SELECT agency_code, COUNT(*) as docket_count FROM ${tableRef("dockets" as RegulationsDataTypes)} GROUP BY agency_code ORDER BY docket_count DESC LIMIT ${limit}`;
      const fallback = `SELECT agency_code, COUNT(*) as docket_count FROM ${parquetRef("dockets" as RegulationsDataTypes)} GROUP BY agency_code ORDER BY docket_count DESC LIMIT ${limit}`;

      return queryWithFallback<{ agency_code: string; docket_count: number }>(query, fallback);
    },
    [runQuery, isReady]
  );

  /**
   * Get docket and comment counts for all agencies in one batch.
   */
  const getAllAgencyCounts = useCallback(
    async (): Promise<Record<string, { dockets: number; comments: number }>> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const docketQ = `SELECT agency_code, COUNT(*) as cnt FROM ${tableRef("dockets" as RegulationsDataTypes)} GROUP BY agency_code`;
      const commentQ = `SELECT agency_code, COUNT(*) as cnt FROM ${tableRef("comments" as RegulationsDataTypes)} GROUP BY agency_code`;
      const dFb = `SELECT agency_code, COUNT(*) as cnt FROM ${parquetRef("dockets" as RegulationsDataTypes)} GROUP BY agency_code`;
      const cFb = `SELECT agency_code, COUNT(*) as cnt FROM ${parquetRef("comments" as RegulationsDataTypes)} GROUP BY agency_code`;

      const [docketRows, commentRows] = await Promise.all([
        queryWithFallback<{ agency_code: string; cnt: number }>(docketQ, dFb),
        queryWithFallback<{ agency_code: string; cnt: number }>(commentQ, cFb),
      ]);

      const result: Record<string, { dockets: number; comments: number }> = {};
      for (const r of docketRows) {
        const code = r.agency_code?.replace(/^"|"$/g, '') || '';
        if (!result[code]) result[code] = { dockets: 0, comments: 0 };
        result[code].dockets = Number(r.cnt);
      }
      for (const r of commentRows) {
        const code = r.agency_code?.replace(/^"|"$/g, '') || '';
        if (!result[code]) result[code] = { dockets: 0, comments: 0 };
        result[code].comments = Number(r.cnt);
      }
      return result;
    },
    [runQuery, isReady]
  );

  return {
    getData,
    getDataCount,
    searchResources,
    getAgencies,
    getDockets,
    getDocketById,
    getRecentDockets,
    getCommentsForDocket,
    getCommentCounts,
    getAgencyStats,
    getPopularAgencies,
    getAllAgencyCounts,
    isReady,
  };
}
