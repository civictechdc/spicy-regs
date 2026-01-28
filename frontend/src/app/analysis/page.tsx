import { Header } from "@/components/Header";
import { UseCasesShowcase } from "@/components/UseCasesShowcase";

export default function AnalysisPage() {
  return (
    <div className="min-h-screen bg-[var(--background)]">
      <Header />
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Hero */}
        <div className="text-center py-8 mb-8">
          <h1 className="text-4xl font-bold mb-4">
            <span className="gradient-text">Analysis Dashboard</span>
          </h1>
          <p className="text-[var(--muted)] text-lg max-w-2xl mx-auto mb-4">
            Discover patterns in campaign activity, organizational influence, and comment trends
          </p>
          <a
            href="https://mybinder.org/v2/gh/civictechdc/spicy-regs/HEAD?urlpath=%2Fdoc%2Ftree%2Fnotebooks%2FREADME.md"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-[var(--surface-elevated)] border border-[var(--border)] text-sm text-[var(--muted)] hover:text-[var(--foreground)] hover:border-[var(--accent-primary)] transition-colors"
          >
            <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
              <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
            </svg>
            Run in Binder
          </a>
        </div>
        
        <UseCasesShowcase />
      </main>
    </div>
  );
}
