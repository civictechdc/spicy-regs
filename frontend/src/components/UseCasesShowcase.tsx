"use client";

import { useState, useEffect, useMemo } from "react";
import Link from "next/link";

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
  { id: "statistics", label: "Overview", description: "Total counts across all data" },
  { id: "agencyActivity", label: "Agency Activity", description: "Most active agencies by comment volume" },
  { id: "campaigns", label: "Campaigns", description: "Dockets with highest duplicate comment rates" },
  { id: "organizations", label: "Organizations", description: "Most active commenters across dockets" },
  { id: "frequentCommenters", label: "Cross-Agency", description: "Entities commenting across multiple agencies" },
  { id: "commentTrends", label: "Trends", description: "Monthly comment volumes over time" },
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

// Sortable table hook
function useSortableData<T>(items: T[] | null, defaultKey: keyof T, defaultDir: "asc" | "desc" = "desc") {
  const [sortKey, setSortKey] = useState<keyof T>(defaultKey);
  const [sortDir, setSortDir] = useState<"asc" | "desc">(defaultDir);

  const sortedItems = useMemo(() => {
    if (!items) return null;
    return [...items].sort((a, b) => {
      const aVal = a[sortKey];
      const bVal = b[sortKey];
      if (aVal < bVal) return sortDir === "asc" ? -1 : 1;
      if (aVal > bVal) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
  }, [items, sortKey, sortDir]);

  const requestSort = (key: keyof T) => {
    if (sortKey === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  return { sortedItems, sortKey, sortDir, requestSort };
}

// Sortable header component
function SortableHeader<T>({ 
  label, 
  sortKey, 
  currentKey, 
  currentDir, 
  onSort,
  align = "left" 
}: { 
  label: string; 
  sortKey: keyof T; 
  currentKey: keyof T; 
  currentDir: "asc" | "desc";
  onSort: (key: keyof T) => void;
  align?: "left" | "right";
}) {
  const isActive = currentKey === sortKey;
  return (
    <th 
      className={`py-3 px-4 font-medium text-[var(--muted)] cursor-pointer hover:text-[var(--foreground)] transition-colors select-none ${align === "right" ? "text-right" : "text-left"}`}
      onClick={() => onSort(sortKey)}
    >
      <span className="inline-flex items-center gap-1">
        {label}
        <span className={`text-xs ${isActive ? "text-[var(--accent-primary)]" : "opacity-30"}`}>
          {isActive ? (currentDir === "asc" ? "↑" : "↓") : "↕"}
        </span>
      </span>
    </th>
  );
}

// Tooltip wrapper
function Tooltip({ text, children }: { text: string; children: React.ReactNode }) {
  return (
    <span className="group relative">
      {children}
      <span className="absolute z-10 hidden group-hover:block bottom-full left-0 mb-1 px-2 py-1 text-xs bg-[var(--surface-elevated)] border border-[var(--border)] rounded shadow-lg whitespace-nowrap max-w-xs truncate">
        {text}
      </span>
    </span>
  );
}

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
        if (!response.ok) throw new Error(`Failed to fetch ${activeTab} analytics`);
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
            {activeTab === "statistics" && <StatisticsCards data={data.statistics as StatisticsResult[] | null} />}
            {activeTab === "agencyActivity" && <AgencyActivityTable data={data.agencyActivity as AgencyActivityResult[] | null} />}
            {activeTab === "campaigns" && <CampaignsTable data={data.campaigns as CampaignResult[] | null} />}
            {activeTab === "organizations" && <OrganizationsTable data={data.organizations as OrganizationResult[] | null} />}
            {activeTab === "frequentCommenters" && <FrequentCommentersTable data={data.frequentCommenters as FrequentCommenterResult[] | null} />}
            {activeTab === "commentTrends" && <CommentTrendsChart data={data.commentTrends as CommentTrendResult[] | null} />}
          </div>
        )}
      </div>
    </div>
  );
}

function CampaignsTable({ data }: { data: CampaignResult[] | null }) {
  const { sortedItems, sortKey, sortDir, requestSort } = useSortableData(data, "duplicate_percentage");

  if (!sortedItems || sortedItems.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <SortableHeader<CampaignResult> label="Docket ID" sortKey="docket_id" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} />
          <SortableHeader<CampaignResult> label="Agency" sortKey="agency_code" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} />
          <SortableHeader<CampaignResult> label="Total" sortKey="total_comments" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <SortableHeader<CampaignResult> label="Unique" sortKey="unique_texts" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <SortableHeader<CampaignResult> label="Duplicates" sortKey="duplicate_percentage" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
        </tr>
      </thead>
      <tbody>
        {sortedItems.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 font-mono text-xs">
              <Link 
                href={`/?agency=${row.agency_code}&docket=${row.docket_id}`}
                className="text-[var(--accent-primary)] hover:underline"
              >
                {row.docket_id}
              </Link>
            </td>
            <td className="py-3 px-4">
              <Link 
                href={`/?agency=${row.agency_code}`}
                className="hover:text-[var(--accent-primary)] hover:underline"
              >
                {row.agency_code}
              </Link>
            </td>
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
  const { sortedItems, sortKey, sortDir, requestSort } = useSortableData(data, "docket_count");

  if (!sortedItems || sortedItems.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <SortableHeader<OrganizationResult> label="Organization" sortKey="title" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} />
          <SortableHeader<OrganizationResult> label="Comments" sortKey="comment_count" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <SortableHeader<OrganizationResult> label="Dockets" sortKey="docket_count" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
        </tr>
      </thead>
      <tbody>
        {sortedItems.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 max-w-md">
              <Tooltip text={row.title}>
                <span className="truncate block">{row.title}</span>
              </Tooltip>
            </td>
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
  const { sortedItems, sortKey, sortDir, requestSort } = useSortableData(data, "comment_count");
  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  if (!sortedItems || sortedItems.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  const maxComments = Math.max(...sortedItems.map(d => d.comment_count));

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <th className="w-8 py-3 px-2"></th>
          <SortableHeader<AgencyActivityResult> label="Agency" sortKey="agency_code" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} />
          <SortableHeader<AgencyActivityResult> label="Comments" sortKey="comment_count" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <SortableHeader<AgencyActivityResult> label="Dockets" sortKey="docket_count" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <th className="w-32 py-3 px-4"></th>
        </tr>
      </thead>
      <tbody>
        {sortedItems.map((row) => {
          const isExpanded = expandedRow === row.agency_code;
          return (
            <>
              <tr 
                key={row.agency_code} 
                className={`border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50 cursor-pointer ${isExpanded ? "bg-[var(--surface-elevated)]/30" : ""}`}
                onClick={() => setExpandedRow(isExpanded ? null : row.agency_code)}
              >
                <td className="py-3 px-2 text-center text-[var(--muted)]">
                  <span className={`inline-block transition-transform ${isExpanded ? "rotate-90" : ""}`}>▶</span>
                </td>
                <td className="py-3 px-4 font-medium">
                  <span className="text-[var(--accent-primary)]">{row.agency_code}</span>
                </td>
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
              {isExpanded && (
                <tr key={`${row.agency_code}-expanded`} className="bg-[var(--surface-elevated)]/20">
                  <td colSpan={5} className="py-4 px-6">
                    <div className="flex flex-wrap gap-3 items-center">
                      <span className="text-sm text-[var(--muted)]">Quick actions:</span>
                      <Link 
                        href={`/?agency=${row.agency_code}`}
                        className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[var(--accent-primary)]/10 text-[var(--accent-primary)] text-sm hover:bg-[var(--accent-primary)]/20 transition-colors"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                        </svg>
                        Explore Dockets
                      </Link>
                      <Link 
                        href={`/search?q=agency:${row.agency_code}`}
                        className="inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[var(--surface-elevated)] border border-[var(--border)] text-sm hover:border-[var(--accent-primary)] transition-colors"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                        </svg>
                        Search Comments
                      </Link>
                      <span className="text-xs text-[var(--muted)] ml-auto">
                        {((row.comment_count / sortedItems.reduce((sum, r) => sum + r.comment_count, 0)) * 100).toFixed(1)}% of all comments
                      </span>
                    </div>
                  </td>
                </tr>
              )}
            </>
          );
        })}
      </tbody>
    </table>
  );
}

function FrequentCommentersTable({ data }: { data: FrequentCommenterResult[] | null }) {
  const { sortedItems, sortKey, sortDir, requestSort } = useSortableData(data, "agencies_count");

  if (!sortedItems || sortedItems.length === 0) {
    return <p className="text-[var(--muted)]">No data available</p>;
  }

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-[var(--border)]">
          <SortableHeader<FrequentCommenterResult> label="Commenter" sortKey="commenter" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} />
          <SortableHeader<FrequentCommenterResult> label="Comments" sortKey="total_comments" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <SortableHeader<FrequentCommenterResult> label="Agencies" sortKey="agencies_count" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
          <SortableHeader<FrequentCommenterResult> label="Dockets" sortKey="dockets_count" currentKey={sortKey} currentDir={sortDir} onSort={requestSort} align="right" />
        </tr>
      </thead>
      <tbody>
        {sortedItems.map((row, i) => (
          <tr key={i} className="border-b border-[var(--border)]/50 hover:bg-[var(--surface-elevated)]/50">
            <td className="py-3 px-4 max-w-md">
              <Tooltip text={row.commenter}>
                <span className="truncate block">{row.commenter}</span>
              </Tooltip>
            </td>
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

  // Group by year
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
      <Link href={`/?agency=${stats.top_agency}`}>
        <StatCard
          label={`Top: ${stats.top_agency}`}
          value={Number(stats.top_agency_comments).toLocaleString()}
          sublabel="comments"
          clickable
        />
      </Link>
    </div>
  );
}

function StatCard({ label, value, sublabel, clickable }: { label: string; value: string; sublabel?: string; clickable?: boolean }) {
  return (
    <div className={`bg-[var(--surface-elevated)] rounded-xl p-5 border border-[var(--border)] ${clickable ? "hover:border-[var(--accent-primary)] cursor-pointer transition-colors" : ""}`}>
      <p className="text-sm text-[var(--muted)]">{label}</p>
      <p className="text-3xl font-bold gradient-text mt-1">{value}</p>
      {sublabel && <p className="text-xs text-[var(--muted)] mt-1">{sublabel}</p>}
    </div>
  );
}
