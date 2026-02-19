import { NextRequest, NextResponse } from "next/server";
import { getLanceDB } from "@/lib/lancedb";

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
  const commentId = request.nextUrl.searchParams.get("id");
  const limit = Math.min(
    Number(request.nextUrl.searchParams.get("limit") || 10),
    30
  );

  if (!commentId) {
    return NextResponse.json(
      { error: "Missing id parameter" },
      { status: 400 }
    );
  }

  try {
    const db = await getLanceDB();
    const table = await db.openTable("comments");

    // Look up the source comment's vector
    const source = await table
      .query()
      .where(`id = '${commentId}'`)
      .select([...SELECT_COLUMNS, "vector"])
      .limit(1)
      .toArray();

    if (source.length === 0) {
      return NextResponse.json(
        { error: "Comment not found in vector index" },
        { status: 404 }
      );
    }

    const sourceVector = source[0].vector as number[];

    // Find nearest neighbors, excluding the source comment
    const results = await table
      .search(sourceVector)
      .select(SELECT_COLUMNS)
      .where(`id != '${commentId}'`)
      .limit(limit)
      .toArray();

    return NextResponse.json({
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
    return NextResponse.json(
      { error: "Similar search failed", details: String(error) },
      { status: 500 }
    );
  }
}
