"""Tools for the agent."""

from typing import Any
from langchain.tools import tool

from src.db.models import RegulationsDataTypes
from src.mirrulations import service as mirrulations
from src.db import service as db


@tool
def get_agencies() -> list[str]:
    """Get a list of all agencies from the Mirrulations S3 bucket"""
    return mirrulations.get_agencies()


@tool
def get_data(
    data_type: RegulationsDataTypes, agency_code: str, docket_id: str = None
) -> list[dict[str, Any]]:
    """Get a list of all data for a given data type for an agency from the database. If docket_id is provided, return data for that docket id."""
    return db.get_data_df(data_type, agency_code, docket_id).to_dicts()


# Augment the LLM with tools
tools = [get_agencies, get_data]
tools_by_name = {tool.name: tool for tool in tools}
