"""Graph for the agent."""

import json
from typing import Literal

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain.messages import SystemMessage, ToolMessage
from langgraph.graph import END, START, StateGraph

from src.agent.utils.state import AgentState
from src.agent.utils.serializer import DateTimeEncoder
from src.agent.utils.system_prompt import get_system_prompt
from src.agent.utils.tools import tools as spicy_regs_tools, tools_by_name

load_dotenv()


def llm_call(state: AgentState):
    """LLM decides whether to call a tool or not."""
    model = init_chat_model("gpt-5-mini", temperature=0)

    model_with_tools = model.bind_tools(
        [
            *spicy_regs_tools,  # spicy regs tools
            {"type": "web_search"},  # server-side web search tool
            *state.get("tools", []),  # CopilotKit tools
        ]
    )
    return {
        "messages": [
            model_with_tools.invoke(
                [SystemMessage(content=get_system_prompt())] + state["messages"]
            )
        ],
        "llm_calls": state.get("llm_calls", 0) + 1,
        "tools": state.get("tools", []),
    }


def tool_node(state: AgentState):
    """Perform the tool call."""
    result = []
    for tool_call in state["messages"][-1].tool_calls:
        if tool_call["name"] not in tools_by_name:
            result.append(
                ToolMessage(
                    content=f"Tool {tool_call['name']} not found",
                    tool_call_id=tool_call["id"],
                )
            )
            continue
        tool = tools_by_name[tool_call["name"]]
        observation = tool.invoke(tool_call["args"])
        # Convert non-string observations to JSON strings
        if not isinstance(observation, str):
            observation = json.dumps(observation, cls=DateTimeEncoder)  # noqa: F821
        result.append(ToolMessage(content=observation, tool_call_id=tool_call["id"]))
    return {"messages": result}


def should_continue(state: AgentState) -> Literal["tool_node", END]:
    """Decide if we should continue the loop or stop based upon whether the LLM made a tool call."""
    messages = state["messages"]
    last_message = messages[-1]

    if last_message.tool_calls:
        copilotkit_actions = state.get("tools", [])
        action_names = {
            action.get("name")
            if isinstance(action, dict)
            else getattr(action, "name", None)
            for action in copilotkit_actions
        }

        has_backend_tools = any(
            tool_call["name"] not in action_names
            for tool_call in last_message.tool_calls
        )

        if has_backend_tools:
            return "tool_node"

    return END


agent_builder = StateGraph(AgentState)

agent_builder.add_node("llm_call", llm_call)
agent_builder.add_node("tool_node", tool_node)

agent_builder.add_edge(START, "llm_call")
agent_builder.add_conditional_edges("llm_call", should_continue, ["tool_node", END])
agent_builder.add_edge("tool_node", "llm_call")

graph = agent_builder.compile()
