"""Graph for the agent."""
import json
from datetime import datetime, date
from typing import Literal

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model
from langchain.messages import SystemMessage, ToolMessage
from langgraph.graph import END, START, MessagesState, StateGraph

from src.agent.utils.system_prompt import get_system_prompt
from src.agent.utils.tools import tools, tools_by_name

load_dotenv()

model = init_chat_model(
    "gpt-5-mini",
    temperature=0
)

model_with_tools = model.bind_tools([*tools, { "type": "web_search" }])


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime and date objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def llm_call(state: dict):
    """LLM decides whether to call a tool or not."""
    return {
        "messages": [
            model_with_tools.invoke(
                [
                    SystemMessage(
                        content=get_system_prompt()
                    )
                ]
                + state["messages"]
            )
        ],
        "llm_calls": state.get('llm_calls', 0) + 1
    }



def tool_node(state: dict):
    """Perform the tool call."""
    result = []
    for tool_call in state["messages"][-1].tool_calls:
        tool = tools_by_name[tool_call["name"]]
        observation = tool.invoke(tool_call["args"])
        # Convert non-string observations to JSON strings
        if not isinstance(observation, str):
            observation = json.dumps(observation, cls=DateTimeEncoder)
        result.append(ToolMessage(content=observation, tool_call_id=tool_call["id"]))
    return {"messages": result}


def should_continue(state: MessagesState) -> Literal["tool_node", END]:
    """Decide if we should continue the loop or stop based upon whether the LLM made a tool call."""
    messages = state["messages"]
    last_message = messages[-1]

    if last_message.tool_calls:
        return "tool_node"

    return END

agent_builder = StateGraph(MessagesState)

agent_builder.add_node("llm_call", llm_call)
agent_builder.add_node("tool_node", tool_node)

agent_builder.add_edge(START, "llm_call")
agent_builder.add_conditional_edges(
    "llm_call",
    should_continue,
    ["tool_node", END]
)
agent_builder.add_edge("tool_node", "llm_call")

graph = agent_builder.compile()
