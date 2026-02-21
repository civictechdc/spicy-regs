const CLOUDFLARE_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CLOUDFLARE_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;

interface CloudflareAIResponse {
  result: {
    shape: number[];
    data: number[][];
  };
  success: boolean;
  errors: string[];
}

export async function getEmbedding(text: string): Promise<number[]> {
  if (!CLOUDFLARE_ACCOUNT_ID || !CLOUDFLARE_API_TOKEN) {
    throw new Error(
      "Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN for embedding generation"
    );
  }

  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/ai/run/@cf/baai/bge-base-en-v1.5`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ text: [text] }),
    }
  );

  if (!response.ok) {
    throw new Error(`Cloudflare AI API error: ${response.status} ${response.statusText}`);
  }

  const data = (await response.json()) as CloudflareAIResponse;

  if (!data.success || !data.result?.data?.[0]) {
    throw new Error(`Cloudflare AI returned no embeddings: ${JSON.stringify(data.errors)}`);
  }

  return data.result.data[0];
}
