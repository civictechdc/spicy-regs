import { Hono } from "hono";
import { getLanceDB } from "../lib/lancedb.js";

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
  const commentId = c.req.query("id");
  const limit = Math.min(Number(c.req.query("limit") || 10), 30);

  if (!commentId) {
    return c.json({ error: "Missing id parameter" }, 400);
  }

  try {
    const db = await getLanceDB();
    const table = await db.openTable("comments");

    // Sanitize ID to prevent injection
    const safeId = commentId.replace(/'/g, "");

    // Look up the source comment's vector
    const source = await table
      .query()
      .where(`id = '${safeId}'`)
      .select([...SELECT_COLUMNS, "vector"])
      .limit(1)
      .toArray();

    if (source.length === 0) {
      return c.json({ error: "Comment not found in vector index" }, 404);
    }

    const sourceVector = source[0].vector as number[];

    // Find nearest neighbors, excluding the source comment
    const results = await table
      .search(sourceVector)
      .select(SELECT_COLUMNS)
      .where(`id != '${safeId}'`)
      .limit(limit)
      .toArray();

    return c.json({
      source_id: commentId,
      results: results.map((r, rank) => ({
        id: r.id,
        title: r.title,
        text: r.text,
        comment: r.comment,
        docket_id: r.docket_id,
        agency_code: r.agency_code,
        posted_date: r.posted_date,
        score: r._distance,
        rank: rank + 1,
      })),
    });
  } catch (error) {
    console.error("Similar search failed:", error);
    return c.json(
      { error: "Similar search failed", details: String(error) },
      500
    );
  }
});

export default app;
