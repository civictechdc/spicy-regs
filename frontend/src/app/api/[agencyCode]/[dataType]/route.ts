import { NextRequest, NextResponse } from "next/server";
import { getData } from "@/lib/db/service";
import { RegulationsDataTypes } from "@/lib/db/models";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ agencyCode: string; dataType: string }> }
) {
  const { agencyCode, dataType } = await params;
  const searchParams = request.nextUrl.searchParams;
  const docketId = searchParams.get("docket_id") || undefined;

  // Validate dataType
  const validDataTypes = Object.values(RegulationsDataTypes) as string[];
  if (!validDataTypes.includes(dataType)) {
    return NextResponse.json({ error: "Invalid data type" }, { status: 400 });
  }

  try {
    const data = await getData(
      dataType as RegulationsDataTypes,
      agencyCode,
      docketId
    );
    return NextResponse.json(data);
  } catch (error: any) {
    console.error("API Error:", error);
    return NextResponse.json({ error: error.message || "Internal Server Error" }, { status: 500 });
  }
}
