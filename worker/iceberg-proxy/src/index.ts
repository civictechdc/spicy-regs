/**
 * Cloudflare Worker: Iceberg REST Catalog CORS Proxy
 *
 * Sits between browser DuckDB-WASM and the Cloudflare R2 Data Catalog.
 * - Adds CORS headers so the browser can call the catalog
 * - Injects the R2 API token server-side (never exposed to browsers)
 * - Forwards all other request details unchanged
 */

interface Env {
  R2_API_TOKEN: string;
  CATALOG_ORIGIN: string;
}

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS",
  "Access-Control-Allow-Headers":
    "Content-Type, Authorization, X-Iceberg-Access-Delegation",
  "Access-Control-Expose-Headers": "Content-Length, Content-Type, ETag",
  "Access-Control-Max-Age": "86400",
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // Build the upstream URL: proxy path → catalog origin
    const url = new URL(request.url);
    const upstream = `${env.CATALOG_ORIGIN}${url.pathname}${url.search}`;

    // Forward the request with auth injected
    const headers = new Headers(request.headers);
    headers.set("Authorization", `Bearer ${env.R2_API_TOKEN}`);
    // Remove the host header so it matches the upstream
    headers.delete("Host");

    const response = await fetch(upstream, {
      method: request.method,
      headers,
      body: request.method !== "GET" && request.method !== "HEAD"
        ? request.body
        : undefined,
    });

    // Clone response and add CORS headers
    const corsResponse = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });

    for (const [key, value] of Object.entries(CORS_HEADERS)) {
      corsResponse.headers.set(key, value);
    }

    return corsResponse;
  },
};
