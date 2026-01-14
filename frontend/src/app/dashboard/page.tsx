"use client";

import { useState } from "react";
import { AgencySelector } from "@/components/AgencySelector";
import { DocketSelector } from "@/components/DocketSelector";
import { DataTypeSelector } from "@/components/DataTypeSelector";
import { DataViewer } from "@/components/DataViewer";
import { DataType } from "@/lib/api";
import Link from "next/link";

export default function HomePage() {
  const [selectedAgency, setSelectedAgency] = useState<string | null>(null);
  const [selectedDocket, setSelectedDocket] = useState<string | null>(null);
  const [dataType, setDataType] = useState<DataType>("dockets");

  return (
    <main className="h-screen w-screen overflow-hidden flex flex-col">
      <div className="h-full w-full p-8 flex flex-col overflow-y-auto">
        <div className="flex justify-between items-start mb-8">
          <div>
            <h1 className="text-3xl font-bold">
              Spicy Regs
            </h1>
            <p className="text-gray-600 dark:text-gray-400">
              Explore federal regulation data from regulations.gov
            </p>
            <Link href="/bookmarks" className="text-blue-600 dark:text-blue-400 hover:underline font-medium">
              Saved Dockets
            </Link>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8 flex-1 min-h-0">
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
    </main>
  );
}
