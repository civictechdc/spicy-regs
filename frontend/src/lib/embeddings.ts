import { pipeline, env, type FeatureExtractionPipeline } from "@huggingface/transformers";

// Disable local model check — always fetch from HuggingFace Hub
env.allowLocalModels = false;

// Cache the pipeline singleton across warm Vercel invocations
let embedder: FeatureExtractionPipeline | null = null;

async function getEmbedder(): Promise<FeatureExtractionPipeline> {
  if (!embedder) {
    // @ts-ignore - pipeline return type is too complex for TypeScript to represent
    embedder = await pipeline("feature-extraction", "Xenova/bge-base-en-v1.5");
  }
  return embedder;
}

export async function getEmbedding(text: string): Promise<number[]> {
  const model = await getEmbedder();
  const output = await model(text, { pooling: "mean", normalize: true });
  return Array.from(output.data as Float32Array);
}
