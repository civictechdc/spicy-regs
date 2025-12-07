import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  serverExternalPackages: ["@duckdb/node-api", "@duckdb/node-bindings", "pino"],
};

export default nextConfig;
