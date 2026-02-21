import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: [
    "pino",
    "apache-arrow",
    "@duckdb/duckdb-wasm",
    "@lancedb/lancedb",
  ],
  // Exclude non-Linux LanceDB native binaries to stay under Vercel's 250 MB limit
  outputFileTracingExcludes: {
    "/api/search": [
      "./node_modules/@lancedb/lancedb-darwin-arm64",
      "./node_modules/@lancedb/lancedb-win32-*",
      "./node_modules/@lancedb/lancedb-linux-arm64-*",
      "./node_modules/@lancedb/lancedb-linux-x64-musl",
      "./node_modules/@img/**",
    ],
    "/api/search/similar": [
      "./node_modules/@lancedb/lancedb-darwin-arm64",
      "./node_modules/@lancedb/lancedb-win32-*",
      "./node_modules/@lancedb/lancedb-linux-arm64-*",
      "./node_modules/@lancedb/lancedb-linux-x64-musl",
      "./node_modules/@img/**",
    ],
  },
  async headers() {
    return [
      {
        source: "/:path*",
        headers: [
          {
            key: "Cross-Origin-Opener-Policy",
            value: "same-origin",
          },
          {
            key: "Cross-Origin-Embedder-Policy",
            value: "require-corp",
          },
        ],
      },
    ];
  },
};

export default nextConfig;
