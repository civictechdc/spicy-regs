"""State for the agent."""

from typing import List

from langgraph.graph import MessagesState
from langgraph.graph.message import Any


class AgentState(MessagesState):
    """State for the agent."""

    tools: List[Any]
