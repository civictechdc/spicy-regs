import { Header } from "@/components/Header";
import { UseCasesShowcase } from "@/components/UseCasesShowcase";

export default function AnalysisPage() {
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <Header />
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
            Analysis
          </h1>
          <p className="text-gray-600 dark:text-gray-400 mt-1">
            Explore patterns in federal regulations data
          </p>
        </div>
        <UseCasesShowcase />
      </main>
    </div>
  );
}
