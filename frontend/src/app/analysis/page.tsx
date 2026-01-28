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
          <p className="text-[var(--muted)] text-lg max-w-2xl mx-auto">
            Discover patterns in campaign activity, organizational influence, and comment trends
          </p>
        </div>
        
        <UseCasesShowcase />
      </main>
    </div>
  );
}
