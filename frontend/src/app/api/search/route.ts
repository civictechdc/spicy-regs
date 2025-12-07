import { NextRequest, NextResponse } from "next/server";
import { searchResources } from "@/lib/db/service";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const q = searchParams.get("q");
  const agency = searchParams.get("agency");

  if (!q) {
    return NextResponse.json({ error: "Query parameter 'q' is required" }, { status: 400 });
  }

  try {
    const results = await searchResources(q, agency || undefined);
    return NextResponse.json({ results });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
