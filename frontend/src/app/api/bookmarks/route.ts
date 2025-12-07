import { createClient } from "@/utils/supabase/server";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { data, error } = await supabase
    .from("bookmarks")
    .select("*")
    .eq("user_id", user.id)
    .order("created_at", { ascending: false });

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({ bookmarks: data });
}

export async function POST(request: NextRequest) {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const { resource_id, resource_type, agency_code, title, metadata } = body;

  if (!resource_id || !resource_type || !agency_code) {
    return NextResponse.json({ error: "Missing required fields" }, { status: 400 });
  }

  const { data, error } = await supabase
    .from("bookmarks")
    .insert([
      {
        user_id: user.id,
        resource_id,
        resource_type,
        agency_code,
        title,
        metadata: metadata || {},
      },
    ])
    .select()
    .single();

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({ bookmark: data });
}

export async function DELETE(request: NextRequest) {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const searchParams = request.nextUrl.searchParams;
  const bookmarkId = searchParams.get("id");
  const resourceId = searchParams.get("resource_id");

  let query = supabase.from("bookmarks").delete().eq("user_id", user.id);

  if (bookmarkId) {
    query = query.eq("id", bookmarkId);
  } else if (resourceId) {
    query = query.eq("resource_id", resourceId);
  } else {
    return NextResponse.json({ error: "Missing id or resource_id" }, { status: 400 });
  }

  const { error } = await query;

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json({ success: true });
}
