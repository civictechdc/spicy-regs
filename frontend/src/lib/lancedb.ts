import lancedb from "@lancedb/lancedb";

let db: Awaited<ReturnType<typeof lancedb.connect>> | null = null;

export async function getLanceDB() {
  if (db) return db;

  const uri = process.env.LANCE_S3_BUCKET;
  if (!uri) {
    throw new Error("LANCE_S3_BUCKET environment variable is not set");
  }

  const storageOptions: Record<string, string> = {};

  if (uri.startsWith("s3://")) {
    const accessKeyId = process.env.LANCE_S3_ACCESS_KEY_ID;
    const secretAccessKey = process.env.LANCE_S3_SECRET_ACCESS_KEY;
    const endpoint = process.env.LANCE_S3_ENDPOINT;

    if (!accessKeyId || !secretAccessKey || !endpoint) {
      throw new Error(
        "S3 credentials required: LANCE_S3_ACCESS_KEY_ID, LANCE_S3_SECRET_ACCESS_KEY, LANCE_S3_ENDPOINT"
      );
    }

    storageOptions.aws_access_key_id = accessKeyId;
    storageOptions.aws_secret_access_key = secretAccessKey;
    storageOptions.aws_endpoint = endpoint;
    storageOptions.aws_region = process.env.LANCE_S3_REGION || "auto";
  }

  db = await lancedb.connect(uri, { storageOptions });
  return db;
}
