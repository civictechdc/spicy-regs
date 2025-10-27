import boto3
from botocore import UNSIGNED
from botocore.client import Config

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

def get_agencies() -> list[str]:
    """
    Get a list of all agencies from the Mirrulations S3 bucket

    Returns:
        list[str]: A list of agency codes
    """
    response = s3.list_objects_v2(
        Bucket="mirrulations", Prefix="raw-data/", Delimiter="/"
    )
    common_prefixes = response.get("CommonPrefixes")
    org_list = [p.get("Prefix").split("/")[1] for p in common_prefixes]
    return org_list

def get_dockets(agency_code: str) -> list[str]:
    """
    Get a list of all dockets for an agency from the Mirrulations S3 bucket

    Returns:
        list[str]: A list of docket IDs
    """
    response = s3.list_objects_v2(
        Bucket="mirrulations", Prefix=f"raw-data/{agency_code}/", Delimiter="/"
    )
    common_prefixes = response.get("CommonPrefixes")
    docket_list = [p.get("Prefix").split("/")[2] for p in common_prefixes]
    return docket_list
