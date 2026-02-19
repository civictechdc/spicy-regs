import { NextRequest, NextResponse } from "next/server";
import * as lancedb from "@lancedb/lancedb";
import { getLanceDB } from "@/lib/lancedb";
import { getEmbedding } from "@/lib/embeddings";

const SELECT_COLUMNS = [
  "id",
  "title",
  "text",
  "comment",
  "docket_id",
  "agency_code",
  "posted_date",
];

export async function GET(request: NextRequest) {
  const query = request.nextUrl.searchParams.get("q");
  const limit = Math.min(
    Number(request.nextUrl.searchParams.get("limit") || 20),
    50
  );
  const agency = request.nextUrl.searchParams.get("agency");

  if (!query) {
    return NextResponse.json(
      { error: "Missing q parameter" },
      { status: 400 }
    );
  }

  try {
    const queryVector = await getEmbedding(query);
    const db = await getLanceDB();
    const table = await db.openTable("comments");

    // Hybrid search: combine vector ANN + BM25 FTS with RRF reranking
    const reranker = await lancedb.rerankers.RRFReranker.create();

    let search = table
      .query()
      .fullTextSearch(query)
      .nearestTo(queryVector)
      .rerank(reranker)
      .select(SELECT_COLUMNS)
      .limit(limit);

    if (agency) {
      search = search.where(`agency_code = '${agency}'`);
    }

    const results = await search.toArray();

    return NextResponse.json({
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
    return NextResponse.json(
      { error: "Search failed", details: String(error) },
      { status: 500 }
    );
  }
}
