import marimo

__generated_with = "0.16.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config

    # Create an S3 client
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    # List objects in the bucket
    response = s3.list_objects_v2(Bucket='mirrulations', Prefix='raw-data')

    # Extract and print the object keys
    if "Contents" in response:
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("No objects found in the bucket.")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
