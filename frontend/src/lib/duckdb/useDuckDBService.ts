"use client";

import { useCallback, useRef } from "react";
import { useDuckDB, R2_BASE_URL } from "./context";
import { RegulationsDataTypes } from "@/lib/db/models";

/** Default row limit to prevent OOM in the browser */
const DEFAULT_LIMIT = 1000;

/** Build a read_parquet() reference for a given data type */
const parquetRef = (dataType: RegulationsDataTypes) =>
  `read_parquet('${R2_BASE_URL}/${dataType}.parquet')`;

/**
 * Hook that provides data fetching via DuckDB-WASM + Parquet on R2.
 */
export function useDuckDBService() {
  const { runQuery, isReady } = useDuckDB();

  /** Cache agency list to avoid re-fetching dockets.parquet just for DISTINCT agency_code */
  const agencyCache = useRef<string[] | null>(null);

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
      if (!isReady) throw new Error("DuckDB not ready");

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

      let query = `SELECT * FROM ${parquetRef(dataType)} ${whereClause} LIMIT ${effectiveLimit}`;
      if (offset !== undefined) query += ` OFFSET ${offset}`;

      return runQuery(query);
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
      if (!isReady) throw new Error("DuckDB not ready");

      const conditions: string[] = [];
      if (agencyCode) {
        conditions.push(`agency_code = '${agencyCode.toUpperCase()}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const query = `SELECT COUNT(*) as count FROM ${parquetRef(dataType)} ${whereClause}`;
      const result = await runQuery<{ count: number }>(query);
      return Number(result[0]?.count ?? 0);
    },
    [runQuery, isReady]
  );

  /**
   * Search across all resource types (capped at 50 results).
   */
  const searchResources = useCallback(
    async (searchQuery: string, agencyCode?: string): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

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
          FROM ${parquetRef(table as RegulationsDataTypes)}
          WHERE ${where}
        `;
      });

      return runQuery(unionQueries.join(" UNION ALL ") + " LIMIT 50");
    },
    [runQuery, isReady]
  );

  /**
   * Get list of agencies.
   */
  const getAgencies = useCallback(async (): Promise<string[]> => {
    if (!isReady) throw new Error("DuckDB not ready");

    // Return cached list if available (populated by getRecentDocketsWithCounts)
    if (agencyCache.current) return agencyCache.current;

    const query = `SELECT DISTINCT agency_code FROM ${parquetRef("dockets" as RegulationsDataTypes)} ORDER BY agency_code`;
    const result = await runQuery<{ agency_code: string }>(query);
    agencyCache.current = result.map((r) => r.agency_code);
    return agencyCache.current;
  }, [runQuery, isReady]);

  /**
   * Get list of dockets for an agency.
   */
  const getDockets = useCallback(
    async (agencyCode: string): Promise<string[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const query = `SELECT DISTINCT docket_id FROM ${parquetRef("dockets" as RegulationsDataTypes)} WHERE agency_code = '${agencyCode.toUpperCase()}' ORDER BY docket_id DESC`;
      const result = await runQuery<{ docket_id: string }>(query);
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
      const query = `SELECT * FROM ${parquetRef("dockets" as RegulationsDataTypes)} WHERE docket_id = '${escaped}' LIMIT 1`;
      const results = await runQuery<Record<string, any>>(query);
      return results[0] || null;
    },
    [runQuery, isReady]
  );

  /**
   * Get recent dockets with comment counts in a single query.
   * Uses LEFT JOIN to combine dockets + comments, avoiding a second Parquet fetch.
   * Returns { dockets, commentCounts } so the page doesn't need a separate call.
   */
  const getRecentDocketsWithCounts = useCallback(
    async (
      limit: number = 20,
      offset: number = 0,
      agencyCode?: string,
      sortBy: 'recent' | 'popular' | 'open' = 'recent'
    ): Promise<{ dockets: any[]; commentCounts: Record<string, number> }> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const conditions: string[] = [];
      if (agencyCode) {
        conditions.push(`d.agency_code = '${agencyCode.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      const orderClause = 'ORDER BY d.modify_date DESC';

      const cols = 'd.docket_id, d.agency_code, d.title, d.abstract, d.docket_type, d.modify_date';
      const query = `
        SELECT ${cols}, COALESCE(c.cnt, 0) AS comment_count
        FROM ${parquetRef("dockets" as RegulationsDataTypes)} d
        LEFT JOIN (
          SELECT REPLACE(docket_id, '"', '') AS did, COUNT(*) AS cnt
          FROM ${parquetRef("comments" as RegulationsDataTypes)}
          GROUP BY did
        ) c ON REPLACE(d.docket_id, '"', '') = c.did
        ${whereClause} ${orderClause}
        LIMIT ${limit} OFFSET ${offset}
      `;

      const rows = await runQuery<any>(query);

      const commentCounts: Record<string, number> = {};
      const dockets = rows.map((row: any) => {
        const id = String(row.docket_id).replace(/^"|"$/g, '').toUpperCase();
        commentCounts[id] = Number(row.comment_count || 0);
        const { comment_count, ...docket } = row;
        return docket;
      });

      return { dockets, commentCounts };
    },
    [runQuery, isReady]
  );

  /** Legacy single-query version (used by detail pages) */
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

      const orderClause = 'ORDER BY modify_date DESC';
      const cols = 'docket_id, agency_code, title, abstract, docket_type, modify_date';
      const query = `SELECT ${cols} FROM ${parquetRef("dockets" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
      return runQuery(query);
    },
    [runQuery, isReady]
  );

  /**
   * Get comments for a specific docket.
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

      const cleanId = docketId.replace(/^"|"$/g, '').toUpperCase();
      const orderClause = sortBy === 'oldest'
        ? 'ORDER BY posted_date ASC'
        : 'ORDER BY posted_date DESC';
      const whereClause = `WHERE REPLACE(docket_id, '"', '') = '${cleanId}'`;

      const query = `SELECT * FROM ${parquetRef("comments" as RegulationsDataTypes)} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
      return runQuery(query);
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
      const query = `SELECT REPLACE(docket_id, '"', '') as did, COUNT(*) as cnt FROM ${parquetRef("comments" as RegulationsDataTypes)} WHERE REPLACE(docket_id, '"', '') IN (${inClause}) GROUP BY did`;

      const rows = await runQuery<{ did: string; cnt: number }>(query);
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
   */
  const getAgencyStats = useCallback(
    async (agencyCode: string): Promise<{ docketCount: number; documentCount: number; commentCount: number }> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const upper = agencyCode.toUpperCase();
      const tables = ["dockets", "documents", "comments"] as const;
      const counts: number[] = [];

      for (const table of tables) {
        const query = `SELECT COUNT(*) as count FROM ${parquetRef(table as RegulationsDataTypes)} WHERE agency_code = '${upper}'`;
        const result = await runQuery<{ count: number }>(query);
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
   */
  const getPopularAgencies = useCallback(
    async (limit: number = 4): Promise<{ agency_code: string; docket_count: number }[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const query = `SELECT agency_code, COUNT(*) as docket_count FROM ${parquetRef("dockets" as RegulationsDataTypes)} GROUP BY agency_code ORDER BY docket_count DESC LIMIT ${limit}`;
      return runQuery<{ agency_code: string; docket_count: number }>(query);
    },
    [runQuery, isReady]
  );

  /**
   * Get docket and comment counts for all agencies in one batch.
   */
  const getAllAgencyCounts = useCallback(
    async (): Promise<Record<string, { dockets: number; comments: number }>> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const docketQ = `SELECT agency_code, COUNT(*) as cnt FROM ${parquetRef("dockets" as RegulationsDataTypes)} GROUP BY agency_code`;
      const commentQ = `SELECT agency_code, COUNT(*) as cnt FROM ${parquetRef("comments" as RegulationsDataTypes)} GROUP BY agency_code`;

      const [docketRows, commentRows] = await Promise.all([
        runQuery<{ agency_code: string; cnt: number }>(docketQ),
        runQuery<{ agency_code: string; cnt: number }>(commentQ),
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
    getRecentDocketsWithCounts,
    getCommentsForDocket,
    getCommentCounts,
    getAgencyStats,
    getPopularAgencies,
    getAllAgencyCounts,
    isReady,
  };
}
