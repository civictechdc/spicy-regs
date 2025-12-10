import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";

// Configure S3 client for unsigned requests (public bucket)
// Note: Mirrulations bucket is public, so no credentials needed
const s3 = new S3Client({
  region: "us-east-1", // Default region usually works for public buckets or specify correct one if known
  signer: { sign: async (request) => request }, // No signing for public access
});

const BUCKET_NAME = "mirrulations";

/**
 * Get a list of all agencies from the Mirrulations S3 bucket
 *
 * @returns A list of agency codes
 */
export async function getAgencies(): Promise<string[]> {
  const command = new ListObjectsV2Command({
    Bucket: BUCKET_NAME,
    Prefix: "raw-data/",
    Delimiter: "/",
  });

  try {
    const response = await s3.send(command);
    if (!response.CommonPrefixes) {
      return [];
    }
    
    // Prefix format: raw-data/{AGENCY}/
    const orgList = response.CommonPrefixes.map((prefix) => { // prefix is CommonPrefix type
      const p = prefix.Prefix || "";
      const parts = p.split("/");
      return parts[1]; // "raw-data" is index 0, agency is index 1
    }).filter((agency) => agency !== undefined && agency !== "");
    
    return orgList;
  } catch (error) {
    console.error("Error fetching agencies:", error);
    return [];
  }
}

/**
 * Get a list of all dockets for an agency from the Mirrulations S3 bucket
 *
 * @param agencyCode The agency code to fetch dockets for
 * @returns A list of docket IDs
 */
export async function getDockets(agencyCode: string): Promise<string[]> {
  const command = new ListObjectsV2Command({
    Bucket: BUCKET_NAME,
    Prefix: `raw-data/${agencyCode}/`,
    Delimiter: "/",
  });

  try {
    const response = await s3.send(command);
    if (!response.CommonPrefixes) {
      return [];
    }

    // Prefix format: raw-data/{AGENCY}/{DOCKET_ID}/
    const docketList = response.CommonPrefixes.map((prefix) => {
      const p = prefix.Prefix || "";
      const parts = p.split("/");
      return parts[2]; // "raw-data" is 0, agency is 1, docket is 2
    }).filter((docket) => docket !== undefined && docket !== "");

    return docketList.sort().reverse();
  } catch (error) {
    console.error(`Error fetching dockets for ${agencyCode}:`, error);
    return [];
  }
}
