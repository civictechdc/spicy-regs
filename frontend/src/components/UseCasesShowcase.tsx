"use client";

import { useState, useEffect } from "react";

const R2_BASE_URL = "https://pub-5fc11ad134984edf8d9af452dd1849d6.r2.dev";

type TabId = "statistics" | "campaigns" | "organizations" | "agencyActivity" | "commentTrends" | "frequentCommenters";

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

interface AgencyActivityResult {
  agency_code: string;
  comment_count: number;
  docket_count: number;
}

interface CommentTrendResult {
  year: number;
  month: number;
  comment_count: number;
}

interface FrequentCommenterResult {
  commenter: string;
  total_comments: number;
  agencies_count: number;
  dockets_count: number;
}

// Pre-computed analytics JSON endpoints
const ANALYTICS_URLS: Record<TabId, string> = {
  statistics: `${R2_BASE_URL}/statistics.json`,
  campaigns: `${R2_BASE_URL}/campaigns.json`,
  organizations: `${R2_BASE_URL}/organizations.json`,
  agencyActivity: `${R2_BASE_URL}/agency_activity.json`,
  commentTrends: `${R2_BASE_URL}/comment_trends.json`,
  frequentCommenters: `${R2_BASE_URL}/frequent_commenters.json`,
};

const TABS: { id: TabId; label: string; description: string }[] = [
  {
    id: "statistics",
    label: "Overview",
    description: "Total counts across all data",
  },
  {
    id: "agencyActivity",
    label: "Agency Activity",
    description: "Most active agencies by comment volume",
  },
  {
    id: "campaigns",
    label: "Campaigns",
    description: "Dockets with highest duplicate comment rates",
  },
  {
    id: "organizations",
    label: "Organizations",
    description: "Most active commenters across dockets",
  },
  {
    id: "frequentCommenters",
    label: "Cross-Agency",
    description: "Entities commenting across multiple agencies",
  },
  {
    id: "commentTrends",
    label: "Trends",
    description: "Monthly comment volumes over time",
  },
];

const initialData: Record<TabId, unknown[] | null> = {
  campaigns: null,
  organizations: null,
  statistics: null,
  agencyActivity: null,
  commentTrends: null,
  frequentCommenters: null,
};

const initialLoading: Record<TabId, boolean> = {
  campaigns: false,
  organizations: false,
  statistics: false,
  agencyActivity: false,
  commentTrends: false,
  frequentCommenters: false,
};

export function UseCasesShowcase() {
  const [activeTab, setActiveTab] = useState<TabId>("statistics");
  const [data, setData] = useState<Record<TabId, unknown[] | null>>(initialData);
  const [loading, setLoading] = useState<Record<TabId, boolean>>(initialLoading);
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
      <div className="flex flex-wrap border-b border-[var(--border)]">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-4 py-3 text-sm font-medium transition-all ${
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
            {activeTab === "statistics" && (
              <StatisticsCards data={data.statistics as StatisticsResult[] | null} />
            )}
            {activeTab === "agencyActivity" && (
              <AgencyActivityTable data={data.agencyActivity as AgencyActivityResult[] | null} />
            )}
            {activeTab === "campaigns" && (
              <CampaignsTable data={data.campaigns as CampaignResult[] | null} />
            )}
            {activeTab === "organizations" && (
              <OrganizationsTable data={data.organizations as OrganizationResult[] | null} />
            )}
            {activeTab === "frequentCommenters" && (
              <FrequentCommentersTable data={data.frequentCommenters as FrequentCommenterResult[] | null} />
            )}
            {activeTab === "commentTrends" && (
              <CommentTrendsChart data={data.commentTrends as CommentTrendResult[] | null} />
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

function AgencyActivityTable({ data }: { data: AgencyActivityResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  const maxComments = Math.max(...data.map(d => d.comment_count));

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <th className="text-left py-3 px-4 font-medium text-[var(--muted)]">Agency</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Comments</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Dockets</th>
          <th className="w-32 py-3 px-4"></th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 font-medium">{row.agency_code}</td>
            <td className="py-3 px-4 text-right">{Number(row.comment_count).toLocaleString()}</td>
            <td className="py-3 px-4 text-right">{Number(row.docket_count).toLocaleString()}</td>
            <td className="py-3 px-4">
              <div className="w-full bg-[var(--surface)] rounded-full h-2">
                <div 
                  className="bg-gradient-to-r from-[var(--accent-primary)] to-[var(--accent-secondary)] h-2 rounded-full"
                  style={{ width: `${(row.comment_count / maxComments) * 100}%` }}
                />
              </div>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function FrequentCommentersTable({ data }: { data: FrequentCommenterResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <th className="text-left py-3 px-4 font-medium text-[var(--muted)]">Commenter</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Comments</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Agencies</th>
          <th className="text-right py-3 px-4 font-medium text-[var(--muted)]">Dockets</th>
        </tr>
      </thead>
      <tbody>
        {data.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 max-w-md truncate">{row.commenter}</td>
            <td className="py-3 px-4 text-right">{Number(row.total_comments).toLocaleString()}</td>
            <td className="py-3 px-4 text-right">
              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-500/10 text-purple-400">
                {row.agencies_count}
              </span>
            </td>
            <td className="py-3 px-4 text-right">{Number(row.dockets_count).toLocaleString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CommentTrendsChart({ data }: { data: CommentTrendResult[] | null }) {
  if (!data || data.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  // Group by year for a simpler visualization
  const yearlyData = data.reduce((acc, row) => {
    const year = row.year;
    acc[year] = (acc[year] || 0) + row.comment_count;
    return acc;
  }, {} as Record<number, number>);

  const years = Object.keys(yearlyData).map(Number).sort();
  const maxCount = Math.max(...Object.values(yearlyData));

  return (
    <div className="space-y-4">
      <div className="text-sm text-[var(--muted)] mb-4">Yearly comment volumes</div>
      <div className="space-y-2">
        {years.map((year) => (
          <div key={year} className="flex items-center gap-4">
            <span className="w-12 text-sm font-medium">{year}</span>
            <div className="flex-1 bg-[var(--surface)] rounded-full h-6 relative overflow-hidden">
              <div 
                className="bg-gradient-to-r from-[var(--accent-primary)] to-[var(--accent-secondary)] h-full rounded-full flex items-center justify-end pr-2"
                style={{ width: `${(yearlyData[year] / maxCount) * 100}%` }}
              >
                <span className="text-xs font-medium text-white">
                  {yearlyData[year] > 100000 ? `${(yearlyData[year] / 1000000).toFixed(1)}M` : Number(yearlyData[year]).toLocaleString()}
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
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
