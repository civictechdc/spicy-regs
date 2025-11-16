"use client";

import { useCoAgent, useCopilotAction } from "@copilotkit/react-core";
import { CopilotChat, CopilotKitCSSProperties, CopilotSidebar } from "@copilotkit/react-ui";
import { useState } from "react";

type AgentState = {
  messages: string[];
}

export default function CopilotKitPage() {
  const { state, setState } = useCoAgent<AgentState>({
    name: "regulations_agent",
  })
  return (
    <main>
      <CopilotChat
        labels={{
          title: "spicy regs",
          initial: "👋 Hi, there! You're chatting with an agent."
        }}
      />
    </main>
  );
}
