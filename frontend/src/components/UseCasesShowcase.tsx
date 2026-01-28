"use client";

import { useState, useEffect } from "react";

const R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev";

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

// Pre-computed analytics JSON endpoints
const ANALYTICS_URLS: Record<TabId, string> = {
  statistics: `${R2_BASE_URL}/statistics.json`,
  campaigns: `${R2_BASE_URL}/campaigns.json`,
  organizations: `${R2_BASE_URL}/organizations.json`,
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
    if (data[activeTab] !== null || loading[activeTab]) return;

    const loadData = async () => {
      setLoading((prev) => ({ ...prev, [activeTab]: true }));
      setQueryError(null);

      try {
        const response = await fetch(ANALYTICS_URLS[activeTab]);
        if (!response.ok) {
          throw new Error(`Failed to fetch ${activeTab} analytics`);
        }
        const result = await response.json();
        setData((prev) => ({ ...prev, [activeTab]: result }));
      } catch (err) {
        console.error(`[UseCases] Fetch failed for ${activeTab}:`, err);
        setQueryError(err instanceof Error ? err.message : "Fetch failed");
      } finally {
        setLoading((prev) => ({ ...prev, [activeTab]: false }));
      }
    };

    loadData();
  }, [activeTab, data, loading]);

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

        {loading[activeTab] ? (
          <div className="flex items-center gap-3 text-[var(--muted)]">
            <div className="animate-spin h-5 w-5 border-2 border-[var(--accent-primary)] border-t-transparent rounded-full" />
            Loading analytics...
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
