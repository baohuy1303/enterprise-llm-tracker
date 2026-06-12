import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getEfficiencySignals } from "@/lib/sentinel-api";
import type { EfficiencyWindow } from "@/lib/types";

export async function GET(request: NextRequest) {
  try {
    const window = (request.nextUrl.searchParams.get("window") ?? "30d") as EfficiencyWindow;
    const data = await getEfficiencySignals(window);
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}
