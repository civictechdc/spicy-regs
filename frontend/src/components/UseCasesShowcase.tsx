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
    id: "statistics",
    label: "Dataset Overview",
    description: "Total counts across all data",
  },
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
      <div className="card-gradient p-6">
        <p className="text-red-400">
          Failed to initialize DuckDB: {error.message}
        </p>
      </div>
    );
  }

  return (
    <div className="card-gradient overflow-hidden">
      {/* Tabs */}
      <div className="flex border-b border-[var(--border)]">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-6 py-4 text-sm font-medium transition-all ${
              activeTab === tab.id
                ? "border-b-2 border-[var(--accent-primary)] text-[var(--accent-primary)] bg-[var(--accent-primary)]/5"
                : "text-[var(--muted)] hover:text-[var(--foreground)] hover:bg-[var(--surface-elevated)]"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Content */}
      <div className="p-6">
        <p className="text-[var(--muted)] mb-6">
          {TABS.find((t) => t.id === activeTab)?.description}
        </p>

        {!isReady ? (
          <div className="flex items-center gap-3 text-[var(--muted)]">
            <div className="animate-spin h-5 w-5 border-2 border-[var(--accent-primary)] border-t-transparent rounded-full" />
            Initializing DuckDB...
          </div>
        ) : loading[activeTab] ? (
          <div className="flex items-center gap-3 text-[var(--muted)]">
            <div className="animate-spin h-5 w-5 border-2 border-[var(--accent-primary)] border-t-transparent rounded-full" />
            Running query...
          </div>
        ) : queryError ? (
          <div className="text-red-400">{queryError}</div>
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
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <th className="text-left py-3 px-4 font-medium text-[var(--muted)]">Docket ID</th>
          <th className="text-left py-3 px-4 font-medium text-[var(--muted)]">Agency</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Total</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Unique</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Duplicates</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 font-mono text-xs">{row.docket_id}</td>
            <td className="py-3 px-4">{row.agency_code}</td>
            <td className="py-3 px-4 text-right">{Number(row.total_comments).toLocaleString()}</td>
            <td className="py-3 px-4 text-right">{Number(row.unique_texts).toLocaleString()}</td>
            <td className="py-3 px-4 text-right">
              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-orange-500/10 text-orange-400">
                {row.duplicate_percentage}%
              </span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function OrganizationsTable({ data }: { data: OrganizationResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <th className="text-left py-3 px-4 font-medium text-[var(--muted)]">Organization</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Comments</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Dockets</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 max-w-md truncate">{row.title}</td>
            <td className="py-3 px-4 text-right">{Number(row.comment_count).toLocaleString()}</td>
            <td className="py-3 px-4 text-right">
              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-[var(--accent-primary)]/10 text-[var(--accent-primary)]">
                {Number(row.docket_count).toLocaleString()}
              </span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function StatisticsCards({ data }: { data: StatisticsResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  const stats = data[0];

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <StatCard label="Dockets" value={Number(stats.total_dockets).toLocaleString()} />
      <StatCard label="Documents" value={Number(stats.total_documents).toLocaleString()} />
      <StatCard label="Comments" value={Number(stats.total_comments).toLocaleString()} />
      <StatCard
        label={`Top: ${stats.top_agency}`}
        value={Number(stats.top_agency_comments).toLocaleString()}
        sublabel="comments"
      />
    </div>
  );
}

function StatCard({ label, value, sublabel }: { label: string; value: string; sublabel?: string }) {
  return (
    <div className="bg-[var(--surface-elevated)] rounded-xl p-5 border border-[var(--border)]">
      <p className="text-sm text-[var(--muted)]">{label}</p>
      <p className="text-3xl font-bold gradient-text mt-1">{value}</p>
      {sublabel && <p className="text-xs text-[var(--muted)] mt-1">{sublabel}</p>}
    </div>
  );
}
