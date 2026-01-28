"use client";

import { useState } from "react";
import { AgencySelector } from "@/components/AgencySelector";
import { DocketSelector } from "@/components/DocketSelector";
import { DataTypeSelector } from "@/components/DataTypeSelector";
import { DataViewer } from "@/components/DataViewer";
import { Header } from "@/components/Header";
import { DataType } from "@/lib/api";

export default function DashboardPage() {
  const [selectedAgency, setSelectedAgency] = useState<string | null>(null);
  const [selectedDocket, setSelectedDocket] = useState<string | null>(null);
  const [dataType, setDataType] = useState<DataType>("dockets");

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 flex flex-col">
      <Header />
      <main className="flex-1 p-6 overflow-auto">
        <div className="max-w-7xl mx-auto">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
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
              <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 h-[calc(100vh-12rem)]">
                <DataViewer
                  agencyCode={selectedAgency}
                  dataType={dataType}
                  docketId={selectedDocket}
                />
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
