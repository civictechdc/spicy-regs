"use client";

import { useCoAgent, useFrontendTool } from "@copilotkit/react-core";
import { CopilotChat, CopilotKitCSSProperties, CopilotSidebar } from "@copilotkit/react-ui";
import { useState } from "react";

type AgentState = {
  messages: string[];
}

export default function CopilotKitPage() {
  const [themeColor, setThemeColor] = useState("#000000");

  const { state, setState } = useCoAgent<AgentState>({
    name: "regulations_agent",
  })

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
    <main className="h-screen w-screen m-0 p-0 overflow-hidden" style={{ "--copilot-kit-primary-color": themeColor }  as CopilotKitCSSProperties}>
      <div className="h-full w-full flex flex-col items-center justify-center" style={{ backgroundColor: themeColor }}>
        <h1 className="text-2xl font-bold">spicy regs</h1>
        <CopilotSidebar
          defaultOpen
          labels={{
            title: "spicy regs",
            initial: "👋 Hi, there! You're chatting with an agent."
          }}
        />
      </div>
    </main>
  );
}
