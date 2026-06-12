import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getLeaderboard } from "@/lib/sentinel-api";
import type { EfficiencyWindow } from "@/lib/types";

export async function GET(request: NextRequest) {
  try {
    const window = (request.nextUrl.searchParams.get("window") ?? "7d") as EfficiencyWindow;
    const data = await getLeaderboard(window);
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}
