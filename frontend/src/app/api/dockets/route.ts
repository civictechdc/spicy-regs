import { NextRequest, NextResponse } from "next/server";
import { getDockets } from "../../../lib/mirrulations/service";

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams;
  const agencyCode = searchParams.get("agency_code");

  if (!agencyCode) {
    return NextResponse.json({ error: "agency_code is required" }, { status: 400 });
  }

  try {
    const dockets = await getDockets(agencyCode);
    return NextResponse.json(dockets);
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
