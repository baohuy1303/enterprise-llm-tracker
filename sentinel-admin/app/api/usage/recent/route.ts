import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getRecentUsage } from "@/lib/sentinel-api";

export async function GET(request: NextRequest) {
  try {
    const limitParam = request.nextUrl.searchParams.get("limit");
    const limit = limitParam ? Number(limitParam) : undefined;
    const data = await getRecentUsage(limit);
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}
