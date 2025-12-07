"use client";

import { useFrontendTool } from "@copilotkit/react-core";
import { useState } from "react";
import { AgencySelector } from "@/components/AgencySelector";
import { DocketSelector } from "@/components/DocketSelector";
import { DataTypeSelector } from "@/components/DataTypeSelector";
import { DataViewer } from "@/components/DataViewer";
import { DataType } from "@/lib/api";
import { CopilotKitCSSProperties, CopilotSidebar } from "@copilotkit/react-ui";
import { SignOutButton } from "@/components/SignOutButton";
import { SearchBar } from "@/components/SearchBar";
import Link from "next/link";

export default function HomePage() {
  const [selectedAgency, setSelectedAgency] = useState<string | null>(null);
  const [selectedDocket, setSelectedDocket] = useState<string | null>(null);
  const [dataType, setDataType] = useState<DataType>("dockets");
  const [themeColor, setThemeColor] = useState("black");


  useFrontendTool({
    name: "setThemeColor",
    description: "Change the theme color of the application. Use this to customize the visual appearance.",
    parameters: [{
      name: "themeColor",
      type: "string",
      description: "The theme color to set. Should be a valid CSS color (hex, rgb, or named color). Make sure to pick nice colors.",
      required: true, 
    }],
    handler({ themeColor }) {
      setThemeColor(themeColor);
    },
  });

  return (
    <main className="h-screen w-screen overflow-hidden flex flex-col" style={{ "--copilot-kit-primary-color": themeColor } as CopilotKitCSSProperties}>
      <div className="h-full w-full p-8 flex flex-col overflow-y-auto" style={{ backgroundColor: themeColor }}>
        <div className="flex justify-between items-start mb-8">
          <div>
            <h1 className="text-3xl font-bold">
              Spicy Regs
            </h1>
            <p className="text-gray-600 dark:text-gray-400">
              Explore federal regulation data from regulations.gov
            </p>
            <div className="mt-4 flex gap-4 items-center">
                <SearchBar />
                <Link href="/bookmarks" className="text-blue-600 dark:text-blue-400 hover:underline font-medium">
                    My Bookmarks
                </Link>
            </div>
          </div>
          <SignOutButton />
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
