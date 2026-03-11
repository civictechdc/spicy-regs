"use client";

import { useCallback, useRef } from "react";
import { useDuckDB, R2_BASE_URL } from "./context";
import { RegulationsDataTypes } from "@/lib/db/models";

/** Default row limit to prevent OOM in the browser */
const DEFAULT_LIMIT = 1000;

/**
 * Build a read_parquet() reference for a given data type.
 * For comments, uses the flat comments.parquet (best for full scans).
 * For agency-scoped comment queries, use commentsForAgency() instead.
 */
const parquetRef = (dataType: RegulationsDataTypes) => {
  return `read_parquet('${R2_BASE_URL}/${dataType}.parquet')`;
};

/**
 * Build a read_parquet() reference for a single agency's comment partition.
 * Uses agency-based Hive partitioning: comments/agency/agency_code={X}/part-0.parquet
 */
const commentsForAgency = (agencyCode: string) => {
  return `read_parquet('${R2_BASE_URL}/comments/agency/agency_code=${agencyCode}/part-0.parquet')`;
};

/**
 * Build a read_parquet() reference for the pre-computed feed summary.
 * Contains dockets with pre-joined comment_count and comment_end_date.
 */
const feedSummaryRef = () => {
  return `read_parquet('${R2_BASE_URL}/feed_summary.parquet')`;
};

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
      const upperAgency = agencyCode?.toUpperCase();
      if (upperAgency) {
        conditions.push(`agency_code = '${upperAgency}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
      const effectiveLimit = limit ?? DEFAULT_LIMIT;

      // Use agency partition for comment queries filtered by agency
      const source = dataType === 'comments' && upperAgency
        ? commentsForAgency(upperAgency)
        : parquetRef(dataType);

      let query = `SELECT * FROM ${source} ${whereClause} LIMIT ${effectiveLimit}`;
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

      const upperAgency = agencyCode?.toUpperCase();
      const conditions: string[] = [];
      if (upperAgency) {
        conditions.push(`agency_code = '${upperAgency}'`);
      }
      if (docketId) {
        conditions.push(`docket_id = '${docketId.toUpperCase()}'`);
      }

      const whereClause =
        conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

      // Use agency partition for comment queries filtered by agency
      const source = dataType === 'comments' && upperAgency
        ? commentsForAgency(upperAgency)
        : parquetRef(dataType);

      const query = `SELECT COUNT(*) as count FROM ${source} ${whereClause}`;
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

      const upperAgency = agencyCode?.toUpperCase();
      const unionQueries = tables.map((table) => {
        let where = `title ILIKE '%${escapedQuery}%'`;
        if (upperAgency) {
          where += ` AND agency_code = '${upperAgency}'`;
        }
        // Use agency partition for comments when filtered by agency
        const source = table === 'comments' && upperAgency
          ? commentsForAgency(upperAgency)
          : parquetRef(table as RegulationsDataTypes);
        return `
          SELECT
            '${table}' as type,
            title,
            docket_id,
            agency_code
          FROM ${source}
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
   * Get recent dockets with comment counts.
   * Uses pre-computed feed_summary.parquet — no JOINs needed.
   */
  const getRecentDocketsWithCounts = useCallback(
    async (
      limit: number = 20,
      offset: number = 0,
      agencyCode?: string,
      sortBy: 'recent' | 'popular' | 'open' = 'recent',
      dateRange?: '7d' | '30d' | '90d' | '365d'
    ): Promise<{ dockets: any[]; commentCounts: Record<string, number> }> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const source = feedSummaryRef();
      const upperAgency = agencyCode?.toUpperCase();

      // Build conditions
      const conditions: string[] = [];
      if (upperAgency) conditions.push(`agency_code = '${upperAgency}'`);

      const dateMap: Record<string, number> = { '7d': 7, '30d': 30, '90d': 90, '365d': 365 };
      const dateDays = dateRange ? dateMap[dateRange] : undefined;
      if (dateDays) {
        conditions.push(`TRY_CAST(modify_date AS TIMESTAMP) >= CAST(NOW() AS TIMESTAMP) - INTERVAL '${dateDays}' DAY`);
      }

      // Sort + filter logic
      let orderClause: string;
      let extraCondition = '';

      if (sortBy === 'popular') {
        orderClause = 'ORDER BY comment_count DESC, modify_date DESC';
      } else if (sortBy === 'open') {
        extraCondition = `TRY_CAST(comment_end_date AS TIMESTAMP) > CAST(NOW() AS TIMESTAMP)`;
        orderClause = 'ORDER BY comment_end_date ASC';
      } else {
        orderClause = 'ORDER BY modify_date DESC';
      }

      if (extraCondition) conditions.push(extraCondition);
      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

      const query = `
        SELECT docket_id, agency_code, title, abstract, docket_type, modify_date,
               comment_count, comment_end_date
        FROM ${source}
        ${whereClause}
        ${orderClause}
        LIMIT ${limit} OFFSET ${offset}
      `;

      const rows = await runQuery<any>(query);

      const commentCounts: Record<string, number> = {};
      const dockets = rows.map((row: any) => {
        const id = String(row.docket_id).toUpperCase();
        commentCounts[id] = Number(row.comment_count || 0);
        return row;
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
   * Extracts agency code from docket_id prefix to read only that agency's
   * partition file instead of scanning the full 3GB+ comments dataset.
   */
  const getCommentsForDocket = useCallback(
    async (
      docketId: string,
      limit: number = 10,
      offset: number = 0,
      sortBy: 'recent' | 'oldest' = 'recent',
      modifyDate?: string
    ): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const cleanId = docketId.replace(/^"|"$/g, '').toUpperCase();
      const orderClause = sortBy === 'oldest'
        ? 'ORDER BY posted_date ASC'
        : 'ORDER BY posted_date DESC';
      const whereClause = `WHERE REPLACE(docket_id, '"', '') = '${cleanId}'`;

      // Extract agency code from docket_id (e.g. "EPA" from "EPA-HQ-OAR-2021-0317")
      const agency = cleanId.split('-')[0];
      const commentsSource = agency
        ? commentsForAgency(agency)
        : parquetRef("comments" as RegulationsDataTypes);

      const query = `SELECT * FROM ${commentsSource} ${whereClause} ${orderClause} LIMIT ${limit} OFFSET ${offset}`;
      return runQuery(query);
    },
    [runQuery, isReady]
  );

  /**
   * Get documents for a specific docket.
   * Reads from documents.parquet (~57MB) with a docket_id filter.
   */
  const getDocumentsForDocket = useCallback(
    async (
      docketId: string,
      limit: number = 50
    ): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const cleanId = docketId.replace(/^"|"$/g, '').toUpperCase();
      const query = `
        SELECT document_id, title, document_type, posted_date,
               comment_start_date, comment_end_date, file_url
        FROM ${parquetRef("documents" as RegulationsDataTypes)}
        WHERE REPLACE(docket_id, '"', '') = '${cleanId}'
        ORDER BY posted_date DESC
        LIMIT ${limit}
      `;
      return runQuery(query);
    },
    [runQuery, isReady]
  );

  /**
   * Get comment counts for a batch of docket IDs.
   * Groups docket IDs by agency prefix and queries each agency's comment partition.
   */
  const getCommentCounts = useCallback(
    async (docketIds: string[]): Promise<Record<string, number>> => {
      if (!isReady || docketIds.length === 0) return {};

      const cleanIds = docketIds.map(id => id.replace(/^"|"$/g, '').toUpperCase());

      // Group docket IDs by agency prefix (e.g. "EPA" from "EPA-HQ-OAR-2021-0317")
      const byAgency: Record<string, string[]> = {};
      for (const id of cleanIds) {
        const agency = id.split('-')[0];
        if (!byAgency[agency]) byAgency[agency] = [];
        byAgency[agency].push(id);
      }

      // Query each agency's comment partition in parallel
      const promises = Object.entries(byAgency).map(async ([agency, ids]) => {
        const inClause = ids.map(id => `'${id}'`).join(',');
        const source = commentsForAgency(agency);
        const query = `SELECT REPLACE(docket_id, '"', '') as did, COUNT(*) as cnt FROM ${source} WHERE REPLACE(docket_id, '"', '') IN (${inClause}) GROUP BY did`;
        return runQuery<{ did: string; cnt: number }>(query);
      });

      const results = await Promise.all(promises);
      const result: Record<string, number> = {};
      // Initialize all requested IDs to 0
      for (const id of cleanIds) result[id] = 0;
      for (const rows of results) {
        for (const r of rows) {
          result[r.did] = Number(r.cnt);
        }
      }
      return result;
    },
    [runQuery, isReady]
  );

  /**
   * Get aggregate stats for an agency (docket, document, comment counts).
   * Queries each parquet source separately: dockets, documents, and the agency comment partition.
   */
  const getAgencyStats = useCallback(
    async (agencyCode: string): Promise<{ docketCount: number; documentCount: number; commentCount: number }> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const upper = agencyCode.toUpperCase();

      const [docketResult, docResult, commentResult] = await Promise.all([
        runQuery<{ count: number }>(`
          SELECT COUNT(*) as count
          FROM ${parquetRef("dockets" as RegulationsDataTypes)}
          WHERE agency_code = '${upper}'
        `),
        runQuery<{ count: number }>(`SELECT COUNT(*) as count FROM ${parquetRef("documents" as RegulationsDataTypes)} WHERE agency_code = '${upper}'`),
        runQuery<{ count: number }>(`SELECT COUNT(*) as count FROM ${commentsForAgency(upper)}`),
      ]);

      return {
        docketCount: Number(docketResult[0]?.count ?? 0),
        documentCount: Number(docResult[0]?.count ?? 0),
        commentCount: Number(commentResult[0]?.count ?? 0),
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
   * Get docket counts for all agencies.
   * Only counts dockets (from dockets.parquet). Comment counts are omitted
   * since they'd require scanning 3GB+ of comment partitions.
   */
  const getAllAgencyCounts = useCallback(
    async (): Promise<Record<string, { dockets: number; comments: number }>> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const query = `
        SELECT agency_code, COUNT(*) as docket_cnt
        FROM ${parquetRef("dockets" as RegulationsDataTypes)}
        GROUP BY agency_code
      `;

      const rows = await runQuery<{ agency_code: string; docket_cnt: number }>(query);
      const result: Record<string, { dockets: number; comments: number }> = {};
      for (const r of rows) {
        const code = r.agency_code?.replace(/^"|"$/g, '') || '';
        result[code] = {
          dockets: Number(r.docket_cnt),
          comments: 0,
        };
      }
      return result;
    },
    [runQuery, isReady]
  );

  /**
   * Get ALL comments for a docket (for export). No pagination, capped at 50k rows.
   */
  const getAllCommentsForDocket = useCallback(
    async (docketId: string): Promise<any[]> => {
      if (!isReady) throw new Error("DuckDB not ready");

      const cleanId = docketId.replace(/^"|"$/g, '').toUpperCase();
      const agency = cleanId.split('-')[0];
      const commentsSource = agency
        ? commentsForAgency(agency)
        : parquetRef("comments" as RegulationsDataTypes);

      const query = `SELECT * FROM ${commentsSource} WHERE REPLACE(docket_id, '"', '') = '${cleanId}' ORDER BY posted_date DESC LIMIT 50000`;
      return runQuery(query);
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
    getAllCommentsForDocket,
    getDocumentsForDocket,
    getCommentCounts,
    getAgencyStats,
    getPopularAgencies,
    getAllAgencyCounts,
    isReady,
  };
}
