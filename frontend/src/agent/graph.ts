import { ChatOpenAI } from "@langchain/openai";
import { SystemMessage, ToolMessage, BaseMessage } from "@langchain/core/messages";
import { StateGraph, END, START, Annotation } from "@langchain/langgraph";
import { tools, toolsByName } from "./tools";
import { SYSTEM_PROMPT } from "./systemPrompt";

export const AgentState = Annotation.Root({
  messages: Annotation<BaseMessage[]>({
    reducer: (x, y) => x.concat(y),
    default: () => [],
  }),
  llm_calls: Annotation<number>({
    reducer: (x, y) => y, // replace
    default: () => 0,
  }),
  tools: Annotation<any[]>({ // CopilotKit tools are usually passed as objects/tools
     reducer: (x, y) => y,
     default: () => [],
  }),
});

// Define the agent state interface for easier usage in functions
// type AgentStateType = typeof AgentState.State; 
// Using `typeof AgentState.State` often doesn't work as expected in some TS versions/setups, 
// using generic inferred type if needed, but functions below infer correctly usually.

async function llmCall(state: typeof AgentState.State) {
  const model = new ChatOpenAI({
    modelName: "gpt-4o-mini", // Replacement for "gpt-5-mini"
    temperature: 0,
    apiKey: process.env.OPENAI_API_KEY,
  });

  // Combine tools
  // CopilotKit tools are in state.tools
  const copilotTools = state.tools || [];
  
  // Bind tools
  const modelWithTools = model.bindTools([
    ...tools,
    ...copilotTools
  ]);

  const messages = [
    new SystemMessage(SYSTEM_PROMPT),
    ...state.messages
  ];

  const result = await modelWithTools.invoke(messages);

  return {
    messages: [result],
    llm_calls: (state.llm_calls || 0) + 1,
    tools: state.tools,
  };
}

async function toolNode(state: typeof AgentState.State) {
  const lastMessage = state.messages[state.messages.length - 1];
  // Check if last message has tool calls
  if (!("tool_calls" in lastMessage) || !Array.isArray(lastMessage.tool_calls)) {
      return { messages: [] };
  }

  const results: BaseMessage[] = [];
  
  for (const toolCall of lastMessage.tool_calls) {
    if (!(toolCall.name in toolsByName)) {
      results.push(new ToolMessage({
        content: `Tool ${toolCall.name} not found`,
        tool_call_id: toolCall.id!,
      }));
      continue;
    }
    
    // Execute backend tool
    const tool = toolsByName[toolCall.name as keyof typeof toolsByName] as any;
    try {
        const observation = await tool.invoke(toolCall.args);
        // Stringify if object
        const content = typeof observation === 'string' ? observation : JSON.stringify(observation);
        results.push(new ToolMessage({
            content,
            tool_call_id: toolCall.id!,
        }));
    } catch (e: any) {
        results.push(new ToolMessage({
            content: `Error executing tool: ${e.message}`,
            tool_call_id: toolCall.id!,
        }));
    }
  }

  return { messages: results };
}

function shouldContinue(state: typeof AgentState.State) {
  const messages = state.messages;
  const lastMessage = messages[messages.length - 1];

  if (lastMessage && "tool_calls" in lastMessage && Array.isArray(lastMessage.tool_calls) && lastMessage.tool_calls.length > 0) {
      const copilotKitActions = state.tools || [];
      const actionNames = new Set(copilotKitActions.map((a: any) => a.name || a.function?.name));
      
      const hasBackendTools = lastMessage.tool_calls.some(tc => !actionNames.has(tc.name));
      
      if (hasBackendTools) {
          return "tool_node";
      }
  }
  
  return END;
}

const agentBuilder = new StateGraph(AgentState)
  .addNode("llm_call", llmCall)
  .addNode("tool_node", toolNode)
  .addEdge(START, "llm_call")
  .addConditionalEdges("llm_call", shouldContinue, {
      tool_node: "tool_node",
      [END]: END
  })
  .addEdge("tool_node", "llm_call");

export const graph = agentBuilder.compile();
