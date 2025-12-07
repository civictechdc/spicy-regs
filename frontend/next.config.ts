import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  serverExternalPackages: ["@duckdb/duckdb-wasm", "apache-arrow", "pino"],
};

export default nextConfig;
