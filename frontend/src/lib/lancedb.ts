import lancedb from "@lancedb/lancedb";

let db: Awaited<ReturnType<typeof lancedb.connect>> | null = null;

export async function getLanceDB() {
  if (db) return db;

  // Support explicit LANCE_DB_URI override, otherwise build from R2_BUCKET_NAME
  const uri =
    process.env.LANCE_DB_URI ||
    (process.env.R2_BUCKET_NAME
      ? `s3://${process.env.R2_BUCKET_NAME}/lance-data`
      : null);

  if (!uri) {
    throw new Error(
      "Set LANCE_DB_URI (local path or s3:// URI) or R2_BUCKET_NAME"
    );
  }

  const storageOptions: Record<string, string> = {};

  if (uri.startsWith("s3://")) {
    const accessKeyId = process.env.R2_ACCESS_KEY_ID;
    const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
    const endpoint = process.env.R2_ENDPOINT;

    if (!accessKeyId || !secretAccessKey || !endpoint) {
      throw new Error(
        "S3 credentials required: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT"
      );
    }

    storageOptions.aws_access_key_id = accessKeyId;
    storageOptions.aws_secret_access_key = secretAccessKey;
    storageOptions.aws_endpoint = endpoint;
    storageOptions.aws_region = "auto";
  }

  db = await lancedb.connect(uri, { storageOptions });
  return db;
}
