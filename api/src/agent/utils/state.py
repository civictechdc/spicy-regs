"""State for the agent."""
import operator

from langchain.messages import AnyMessage
from typing_extensions import Annotated, TypedDict


class MessagesState(TypedDict):
    """State for the agent."""
    messages: Annotated[list[AnyMessage], operator.add]
    llm_calls: int
