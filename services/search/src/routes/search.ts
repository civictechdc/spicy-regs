import { Hono } from "hono";
import * as lancedb from "@lancedb/lancedb";
import { getLanceDB } from "../lib/lancedb.js";
import { getEmbedding } from "../lib/embeddings.js";

const SELECT_COLUMNS = [
  "id",
  "title",
  "text",
  "comment",
  "docket_id",
  "agency_code",
  "posted_date",
];

const app = new Hono();

app.get("/", async (c) => {
  const query = c.req.query("q");
  const limit = Math.min(Number(c.req.query("limit") || 20), 50);
  const agency = c.req.query("agency");

  if (!query) {
    return c.json({ error: "Missing q parameter" }, 400);
  }

  try {
    const queryVector = await getEmbedding(query);
    const db = await getLanceDB();
    const table = await db.openTable("comments");

    const reranker = await lancedb.rerankers.RRFReranker.create();

    let search = table
      .query()
      .fullTextSearch(query)
      .nearestTo(queryVector)
      .rerank(reranker)
      .select(SELECT_COLUMNS)
      .limit(limit);

    if (agency) {
      // Sanitize agency code to prevent injection
      const safeAgency = agency.replace(/'/g, "");
      search = search.where(`agency_code = '${safeAgency}'`);
    }

    const results = await search.toArray();

    return c.json({
      query,
      results: results.map((r) => ({
        id: r.id,
        title: r.title,
        text: r.text,
        comment: r.comment,
        docket_id: r.docket_id,
        agency_code: r.agency_code,
        posted_date: r.posted_date,
        score: r._relevance_score,
      })),
    });
  } catch (error) {
    console.error("Hybrid search failed:", error);
    return c.json(
      { error: "Search failed", details: String(error) },
      500
    );
  }
});

export default app;
