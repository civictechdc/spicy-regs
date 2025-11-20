"use client";

import { useState } from "react";
import { AgencySelector } from "@/components/AgencySelector";
import { DocketSelector } from "@/components/DocketSelector";
import { DataTypeSelector } from "@/components/DataTypeSelector";
import { DataViewer } from "@/components/DataViewer";
import { DataType } from "@/lib/api";
import { CopilotSidebar } from "@copilotkit/react-ui";

export default function HomePage() {
  const [selectedAgency, setSelectedAgency] = useState<string | null>(null);
  const [selectedDocket, setSelectedDocket] = useState<string | null>(null);
  const [dataType, setDataType] = useState<DataType>("dockets");

  return (
    <main className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="container mx-auto px-4 py-8 max-w-7xl">
        <header className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100 mb-2">
            Spicy Regs
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Explore federal regulation data from regulations.gov
          </p>
        </header>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
          <div className="lg:col-span-1 space-y-4">
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <AgencySelector
                selectedAgency={selectedAgency}
                onSelectAgency={(agency) => {
                  setSelectedAgency(agency);
                  setSelectedDocket(null);
                }}
              />
            </div>

            {selectedAgency && (
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <DocketSelector
                  agencyCode={selectedAgency}
                  selectedDocket={selectedDocket}
                  onSelectDocket={setSelectedDocket}
                />
              </div>
            )}

            {selectedAgency && (
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
                <DataTypeSelector
                  selectedType={dataType}
                  onSelectType={setDataType}
                />
              </div>
            )}
          </div>

          <div className="lg:col-span-2">
            <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700">
              <DataViewer
                agencyCode={selectedAgency}
                dataType={dataType}
                docketId={selectedDocket}
              />
            </div>
          </div>
        </div>
      </div>

      <CopilotSidebar
        defaultOpen={false}
        labels={{
          title: "Spicy Regs Assistant",
          initial: "Hi! I can help you explore federal regulations. Ask me about agencies, dockets, or specific regulations."
        }}
      />
    </main>
  );
}
