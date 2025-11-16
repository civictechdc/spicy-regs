from fastapi import FastAPI
from starlette.responses import RedirectResponse
from .mirrulations import service as mirrulations
from .db import service as db
from .db.config import initialize_database
from .db.models import RegulationsDataTypes

app = FastAPI(title="Spicy Regs API", description="API for the Spicy Regs project")
initialize_database()

@app.get("/")
def redirect_to_docs():
    return RedirectResponse(url="/docs")

@app.get("/agencies")
def get_agencies() -> list[str]:
    """
    Get a list of all agencies from the Mirrulations S3 bucket
    """
    return mirrulations.get_agencies()

@app.get("/dockets")
def get_dockets(agency_code: str) -> list[str]:
    """
    Get a list of all dockets for an agency from the Mirrulations S3 bucket
    """
    return mirrulations.get_dockets(agency_code)

@app.get("/{agency_code}/{data_type}")
def get_regulations_data(agency_code: str, data_type: RegulationsDataTypes, docket_id: str = None) -> list[dict]:
    """
    Get a list of all data for a given data type for an agency from the Mirrulations S3 bucket. If docket_id is provided, return data for that docket id.
    """
    return db.get_data_df(data_type, agency_code, docket_id).to_dicts()