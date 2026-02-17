import Link from 'next/link';
import { Flame, Rss, Building2, BarChart3, Search, Bookmark, ExternalLink } from 'lucide-react';

export default function HomePage() {
  return (
    <div className="min-h-screen bg-[var(--background)]">
      {/* Hero */}
      <section className="relative overflow-hidden">
        <div className="max-w-4xl mx-auto px-6 pt-20 pb-16 text-center">
          <div className="inline-flex items-center gap-3 mb-6">
            <Flame size={40} className="text-[var(--accent-primary)]" />
            <h1 className="text-5xl font-bold">
              <span className="gradient-text">Spicy Regs</span>
            </h1>
          </div>

          <p className="text-xl text-[var(--foreground)] max-w-2xl mx-auto mb-4 font-serif leading-relaxed">
            An open-source civic tech platform that makes federal regulations
            accessible, searchable, and explorable for everyone.
          </p>

          <p className="text-[var(--muted)] max-w-xl mx-auto mb-10 leading-relaxed">
            Powered by{' '}
            <a
              href="https://www.regulations.gov"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[var(--accent-primary)] hover:underline"
            >
              regulations.gov
            </a>{' '}
            data, Spicy Regs transforms millions of regulatory records into a
            browsable, social-media-style feed — running entirely in your
            browser with no backend servers.
          </p>

          <Link
            href="/feed"
            className="btn-primary inline-flex items-center gap-2 text-base"
          >
            <Rss size={18} />
            Browse the Feed
          </Link>
        </div>
      </section>

      {/* What You Can Do */}
      <section className="max-w-4xl mx-auto px-6 pb-20">
        <h2 className="text-2xl font-bold text-center mb-10">
          What You Can Do
        </h2>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
          <FeatureCard
            icon={<Rss size={22} />}
            title="Docket Feed"
            description="Scroll through federal dockets as posts — filter by agency, sort by recency or popularity, and see comment counts at a glance."
            href="/feed"
          />
          <FeatureCard
            icon={<Building2 size={22} />}
            title="Agency Browser"
            description="Explore agencies like communities. View their dockets, documents, and public comments in one place."
            href="/agencies"
          />
          <FeatureCard
            icon={<BarChart3 size={22} />}
            title="Analysis Dashboard"
            description="Pre-computed analyses that surface trends across agencies, comment volumes, and regulatory activity."
            href="/analysis"
          />
          <FeatureCard
            icon={<Search size={22} />}
            title="Full-Text Search"
            description="Search across regulations, dockets, and documents using DuckDB-powered queries — all inside your browser."
            href="/search"
          />
          <FeatureCard
            icon={<Bookmark size={22} />}
            title="Bookmarks"
            description="Save dockets you care about for quick access later. Everything stays local in your browser — no account needed."
            href="/bookmarks"
          />
          <FeatureCard
            icon={<ExternalLink size={22} />}
            title="Open Source"
            description="Built with Next.js, DuckDB-WASM, and Cloudflare R2. Fork it, extend it, or contribute back to the project."
            href="https://github.com/civictechdc/spicy-regs"
            external
          />
        </div>
      </section>

      {/* How It Works */}
      <section className="border-t border-[var(--border)] bg-[var(--surface)]">
        <div className="max-w-3xl mx-auto px-6 py-16 text-center">
          <h2 className="text-2xl font-bold mb-4">How It Works</h2>
          <p className="text-[var(--muted)] leading-relaxed mb-8 max-w-2xl mx-auto">
            Spicy Regs runs a full analytical database in your browser using{' '}
            <strong className="text-[var(--foreground)]">DuckDB-WASM</strong>.
            It queries compressed Parquet files stored on Cloudflare R2 via HTTP
            range requests — no backend, no API keys, no cost to you.
          </p>
          <Link
            href="/feed"
            className="text-[var(--accent-primary)] font-medium hover:underline inline-flex items-center gap-1"
          >
            Start exploring →
          </Link>
        </div>
      </section>
    </div>
  );
}

function FeatureCard({
  icon,
  title,
  description,
  href,
  external,
}: {
  icon: React.ReactNode;
  title: string;
  description: string;
  href: string;
  external?: boolean;
}) {
  const linkProps = external
    ? { target: '_blank' as const, rel: 'noopener noreferrer' }
    : {};

  return (
    <Link href={href} {...linkProps}>
      <div className="card-gradient p-5 h-full hover:border-[var(--accent-primary)] transition-colors cursor-pointer">
        <div className="flex items-center gap-2 mb-3">
          <span className="text-[var(--accent-primary)]">{icon}</span>
          <h3 className="text-base font-semibold">{title}</h3>
        </div>
        <p className="text-sm text-[var(--muted)] leading-relaxed">
          {description}
        </p>
      </div>
    </Link>
  );
}
