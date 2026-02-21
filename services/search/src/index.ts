import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
import searchRoute from "./routes/search.js";
import similarRoute from "./routes/similar.js";
import { getLanceDB } from "./lib/lancedb.js";

const app = new Hono();

// CORS — allow the Vercel frontend
const allowedOrigins = process.env.ALLOWED_ORIGINS?.split(",") || [
  "https://app.spicy-regs.dev",
  "http://localhost:3000",
];

app.use(
  "*",
  cors({
    origin: allowedOrigins,
    allowMethods: ["GET"],
  })
);

// Required for frontends with Cross-Origin-Embedder-Policy: require-corp
app.use("*", async (c, next) => {
  await next();
  c.header("Cross-Origin-Resource-Policy", "cross-origin");
});

// Health check
app.get("/health", (c) => c.json({ status: "ok" }));

// Mount routes
app.route("/search", searchRoute);
app.route("/search/similar", similarRoute);

const port = Number(process.env.PORT) || 3001;

// Pre-warm LanceDB connection on startup
getLanceDB()
  .then(() => console.log("LanceDB connection ready"))
  .catch((err) => console.error("LanceDB warmup failed:", err));

serve({ fetch: app.fetch, port }, () => {
  console.log(`Search API running on port ${port}`);
});
