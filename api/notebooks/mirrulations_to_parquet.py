import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium")


@app.cell
def _():
    # Imports
    import marimo as mo
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config
    import duckdb
    return Config, UNSIGNED, boto3, duckdb


@app.cell
def _(Config, UNSIGNED, boto3):
    from pprint import pprint

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    response = s3.list_objects_v2(
        Bucket="mirrulations", Prefix="raw-data/", Delimiter="/"
    )
    common_prefixes = response.get("CommonPrefixes")

    org_list = [p.get("Prefix").split("/")[1] for p in common_prefixes]
    print(org_list)
    return pprint, s3


@app.cell
def _(pprint, s3):
    file_response = s3.list_objects_v2(
        Bucket="mirrulations", Prefix="raw-data/"
    )
    pprint(file_response.get('Contents'))
    return


@app.cell
def _(duckdb):
    # Using DuckDB to query S3 objects
    # 43s to query 618 ACF dockets
    duckdb.sql("SELECT count(*) FROM read_json('s3://mirrulations/raw-data/ACF/*/*/docket/*.json')").show()

    return


@app.cell
def _():
    # Dockets
    # query = """\
    # CREATE OR REPLACE VIEW src_docket_files AS
    # SELECT
    #   filename,
    #   content,
    #   split_part(filename, '/', 4) as agency_code,
    #   split_part(filename, '/', 5) as docket_id,
    #   split_part(split_part(filename, '/', 5), '-', 2) as year,
    # FROM read_text('s3://mirrulations/raw-data/*/*/*/docket/*.json');

    # CREATE OR REPLACE VIEW docket_parsed AS
    # SELECT
    #   f.agency_code,
    #   f.docket_id,
    #   f.year,

    #   json_extract_string(f.content, '$.data.attributes.docketType') as docket_type,
    #   json_extract_string(f.content, '$.data.attributes.modifyDate')::TIMESTAMP as modify_date,
    #   json_extract_string(f.content, '$.data.attributes.title') as title,

    #   f.content AS raw_json
    # FROM src_docket_files f;

    # COPY (
    #   SELECT *
    #   FROM docket_parsed
    # ) TO 'parquet/dockets'
    #   (FORMAT PARQUET,
    #    PARTITION_BY (agency_code, year),
    #    COMPRESSION SNAPPY);
    # """

    # conn.query(query)
    return


if __name__ == "__main__":
    app.run()
