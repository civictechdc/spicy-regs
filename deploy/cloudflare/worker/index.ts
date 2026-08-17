// Worker entrypoint fronting the MCP container (Cloudflare Containers, beta).
//
// The container runs the canonical Python MCP server (see ../Dockerfile) on
// port 8080; this Worker forwards /mcp requests to it. The server is stateless
// (stateless_http), so requests can land on any instance — getContainer load-
// balances across up to max_instances.
//
// BETA: verify the @cloudflare/containers API against the installed version on
// first deploy — the envVars/secret-injection surface has been changing.
import { Container, getContainer } from "@cloudflare/containers";

interface Env {
  MCP_CONTAINER: DurableObjectNamespace;
  // Secret, set via `wrangler secret put R2_CATALOG_TOKEN`.
  R2_CATALOG_TOKEN: string;
  // Non-secret catalog config, set as plain vars (wrangler.jsonc "vars" or here).
  R2_CATALOG_URI: string;
  R2_CATALOG_WAREHOUSE: string;
  R2_CATALOG_NAMESPACE: string;
}

export class McpContainer extends Container<Env> {
  defaultPort = 8080;
  // Idle timeout before the instance is reclaimed (scale-to-zero economics).
  sleepAfter = "20m";

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // DuckDB tuning + R2 config passed into the container's environment. Memory
    // capped under standard-4's 12 GiB so a heavy query spills to the real disk
    // (/tmp) instead of being OOM-killed; the token comes from a wrangler secret.
    this.envVars = {
      SPICY_REGS_MEMORY_LIMIT: "9GB",
      SPICY_REGS_TEMP_DIR: "/tmp",
      SPICY_REGS_HOME_DIR: "/tmp",
      SPICY_REGS_STATEMENT_TIMEOUT: "600s",
      SPICY_REGS_R2_URL: "https://data.spicy-regs.dev",
      R2_CATALOG_URI: env.R2_CATALOG_URI,
      R2_CATALOG_WAREHOUSE: env.R2_CATALOG_WAREHOUSE,
      R2_CATALOG_NAMESPACE: env.R2_CATALOG_NAMESPACE,
      R2_CATALOG_TOKEN: env.R2_CATALOG_TOKEN,
    };
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);
    if (pathname === "/mcp" || pathname.startsWith("/mcp/")) {
      return getContainer(env.MCP_CONTAINER).fetch(request);
    }
    return new Response("Not found", { status: 404 });
  },
};
