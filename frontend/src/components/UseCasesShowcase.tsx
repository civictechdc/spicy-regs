"use client";

import { useState, useEffect } from "react";
import { useDuckDB, R2_BASE_URL } from "@/lib/duckdb/context";

type TabId = "campaigns" | "organizations" | "statistics";

interface CampaignResult {
  docket_id: string;
  agency_code: string;
  total_comments: number;
  unique_texts: number;
  duplicate_percentage: number;
}

interface OrganizationResult {
  title: string;
  comment_count: number;
  docket_count: number;
}

interface StatisticsResult {
  total_dockets: number;
  total_documents: number;
  total_comments: number;
  top_agency: string;
  top_agency_comments: number;
}

const QUERIES = {
  campaigns: `
    SELECT 
      docket_id,
      agency_code,
      COUNT(*) as total_comments,
      COUNT(DISTINCT comment) as unique_texts,
      ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT comment)) / COUNT(*), 1) as duplicate_percentage
    FROM read_parquet('${R2_BASE_URL}/comments.parquet')
    WHERE comment IS NOT NULL
    GROUP BY docket_id, agency_code
    HAVING COUNT(*) > 1000 AND COUNT(*) > COUNT(DISTINCT comment)
    ORDER BY duplicate_percentage DESC
    LIMIT 10
  `,
  organizations: `
    SELECT 
      title,
      COUNT(*) as comment_count,
      COUNT(DISTINCT docket_id) as docket_count
    FROM read_parquet('${R2_BASE_URL}/comments.parquet')
    WHERE title IS NOT NULL
      AND title NOT LIKE 'Comment%'
      AND title NOT LIKE 'Anonymous%'
      AND LENGTH(title) > 5
    GROUP BY title
    HAVING COUNT(DISTINCT docket_id) > 50
    ORDER BY docket_count DESC
    LIMIT 15
  `,
  statistics: `
    WITH stats AS (
      SELECT 
        (SELECT COUNT(*) FROM read_parquet('${R2_BASE_URL}/dockets.parquet')) as total_dockets,
        (SELECT COUNT(*) FROM read_parquet('${R2_BASE_URL}/documents.parquet')) as total_documents,
        (SELECT COUNT(*) FROM read_parquet('${R2_BASE_URL}/comments.parquet')) as total_comments
    ),
    top_agency AS (
      SELECT agency_code, COUNT(*) as cnt
      FROM read_parquet('${R2_BASE_URL}/comments.parquet')
      GROUP BY agency_code
      ORDER BY cnt DESC
      LIMIT 1
    )
    SELECT 
      s.total_dockets,
      s.total_documents,
      s.total_comments,
      t.agency_code as top_agency,
      t.cnt as top_agency_comments
    FROM stats s, top_agency t
  `,
};

const TABS: { id: TabId; label: string; description: string }[] = [
  {
    id: "campaigns",
    label: "Campaign Detection",
    description: "Dockets with highest duplicate comment rates",
  },
  {
    id: "organizations",
    label: "Top Organizations",
    description: "Most active commenters across dockets",
  },
  {
    id: "statistics",
    label: "Dataset Statistics",
    description: "Overview of available data",
  },
];

export function UseCasesShowcase() {
  const { runQuery, isReady, error } = useDuckDB();
  const [activeTab, setActiveTab] = useState<TabId>("statistics");
  const [data, setData] = useState<Record<TabId, unknown[] | null>>({
    campaigns: null,
    organizations: null,
    statistics: null,
  });
  const [loading, setLoading] = useState<Record<TabId, boolean>>({
    campaigns: false,
    organizations: false,
    statistics: false,
  });
  const [queryError, setQueryError] = useState<string | null>(null);

  // Load data when tab changes and DuckDB is ready
  useEffect(() => {
    if (!isReady || data[activeTab] !== null || loading[activeTab]) return;

    const loadData = async () => {
      setLoading((prev) => ({ ...prev, [activeTab]: true }));
      setQueryError(null);

      try {
        const result = await runQuery(QUERIES[activeTab]);
        setData((prev) => ({ ...prev, [activeTab]: result }));
      } catch (err) {
        console.error(`[UseCases] Query failed for ${activeTab}:`, err);
        setQueryError(err instanceof Error ? err.message : "Query failed");
      } finally {
        setLoading((prev) => ({ ...prev, [activeTab]: false }));
      }
    };

    loadData();
  }, [activeTab, isReady, data, loading, runQuery]);

  if (error) {
    return (
      <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
        <p className="text-red-700 dark:text-red-400">
          Failed to initialize DuckDB: {error.message}
        </p>
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
      <div className="p-6 border-b border-gray-200 dark:border-gray-700">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
          Analysis Use Cases
        </h2>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Live queries against 27M+ records on Cloudflare R2
        </p>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-gray-200 dark:border-gray-700">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-4 py-3 text-sm font-medium transition-colors ${
              activeTab === tab.id
                ? "border-b-2 border-blue-600 text-blue-600 dark:text-blue-400"
                : "text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="p-6">
        <p className="text-sm text-gray-500 dark:text-gray-400 mb-4">
          {TABS.find((t) => t.id === activeTab)?.description}
        </p>

        {!isReady ? (
          <div className="flex items-center gap-2 text-gray-500">
            <div className="animate-spin h-4 w-4 border-2 border-blue-600 border-t-transparent rounded-full" />
            Initializing DuckDB...
          </div>
        ) : loading[activeTab] ? (
          <div className="flex items-center gap-2 text-gray-500">
            <div className="animate-spin h-4 w-4 border-2 border-blue-600 border-t-transparent rounded-full" />
            Running query...
          </div>
        ) : queryError ? (
          <div className="text-red-600 dark:text-red-400">{queryError}</div>
        ) : (
          <div className="overflow-x-auto">
            {activeTab === "campaigns" && (
              <CampaignsTable data={data.campaigns as CampaignResult[] | null} />
            )}
            {activeTab === "organizations" && (
              <OrganizationsTable
                data={data.organizations as OrganizationResult[] | null}
              />
            )}
            {activeTab === "statistics" && (
              <StatisticsCards
                data={data.statistics as StatisticsResult[] | null}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function CampaignsTable({ data }: { data: CampaignResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-gray-500">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-200 dark:border-gray-700">
          <th className="text-left py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Docket ID
          </th>
          <th className="text-left py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Agency
          </th>
          <th className="text-right py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Total
          </th>
          <th className="text-right py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Unique
          </th>
          <th className="text-right py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Duplicates %
          </th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr
            key={i}
            className="border-b border-gray-100 dark:border-gray-800"
          >
            <td className="py-2 px-3 font-mono text-xs">{row.docket_id}</td>
            <td className="py-2 px-3">{row.agency_code}</td>
            <td className="py-2 px-3 text-right">
              {Number(row.total_comments).toLocaleString()}
            </td>
            <td className="py-2 px-3 text-right">
              {Number(row.unique_texts).toLocaleString()}
            </td>
            <td className="py-2 px-3 text-right font-medium text-orange-600 dark:text-orange-400">
              {row.duplicate_percentage}%
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function OrganizationsTable({ data }: { data: OrganizationResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-gray-500">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-200 dark:border-gray-700">
          <th className="text-left py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Organization
          </th>
          <th className="text-right py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Comments
          </th>
          <th className="text-right py-2 px-3 font-medium text-gray-700 dark:text-gray-300">
            Dockets
          </th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr
            key={i}
            className="border-b border-gray-100 dark:border-gray-800"
          >
            <td className="py-2 px-3 max-w-md truncate">{row.title}</td>
            <td className="py-2 px-3 text-right">
              {Number(row.comment_count).toLocaleString()}
            </td>
            <td className="py-2 px-3 text-right font-medium text-blue-600 dark:text-blue-400">
              {Number(row.docket_count).toLocaleString()}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function StatisticsCards({ data }: { data: StatisticsResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-gray-500">No data available</p>;
  }

  const stats = data[0];

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <StatCard
        label="Dockets"
        value={Number(stats.total_dockets).toLocaleString()}
      />
      <StatCard
        label="Documents"
        value={Number(stats.total_documents).toLocaleString()}
      />
      <StatCard
        label="Comments"
        value={Number(stats.total_comments).toLocaleString()}
      />
      <StatCard
        label={`Top Agency (${stats.top_agency})`}
        value={Number(stats.top_agency_comments).toLocaleString()}
        sublabel="comments"
      />
    </div>
  );
}

function StatCard({
  label,
  value,
  sublabel,
}: {
  label: string;
  value: string;
  sublabel?: string;
}) {
  return (
    <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-4">
      <p className="text-sm text-gray-500 dark:text-gray-400">{label}</p>
      <p className="text-2xl font-bold text-gray-900 dark:text-gray-100 mt-1">
        {value}
      </p>
      {sublabel && (
        <p className="text-xs text-gray-400 dark:text-gray-500">{sublabel}</p>
      )}
    </div>
  );
}
