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
    <div className="min-h-screen bg-[var(--background)] flex flex-col">
      <Header />
      <main className="flex-1 p-6 overflow-auto">
        <div className="max-w-7xl mx-auto">
          {/* Hero section */}
          {!selectedAgency && (
            <div className="text-center py-12 mb-8">
              <h1 className="text-4xl md:text-5xl font-bold mb-4">
                <span className="gradient-text">Explore Federal Regulations</span>
              </h1>
              <p className="text-[var(--muted)] text-lg max-w-2xl mx-auto">
                Access 27M+ public comments, 2M+ documents, and 346K+ dockets from regulations.gov
              </p>
            </div>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div className="lg:col-span-1 space-y-4">
              <div className="card-gradient p-6">
                <AgencySelector
                  selectedAgency={selectedAgency}
                  onSelectAgency={(agency) => {
                    setSelectedAgency(agency);
                    setSelectedDocket(null);
                  }}
                />
              </div>

              {selectedAgency && (
                <div className="card-gradient p-6">
                  <DocketSelector
                    agencyCode={selectedAgency}
                    selectedDocket={selectedDocket}
                    onSelectDocket={setSelectedDocket}
                  />
                </div>
              )}

              {selectedAgency && (
                <div className="card-gradient p-6">
                  <DataTypeSelector
                    selectedType={dataType}
                    onSelectType={setDataType}
                  />
                </div>
              )}
            </div>

            <div className="lg:col-span-2">
              <div className="card-gradient p-6 h-[calc(100vh-12rem)]">
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
