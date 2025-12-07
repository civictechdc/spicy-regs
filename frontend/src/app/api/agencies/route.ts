import { NextResponse } from "next/server";
import { getAgencies } from "../../../lib/mirrulations/service";

export async function GET() {
  try {
    const agencies = await getAgencies();
    return NextResponse.json(agencies);
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
