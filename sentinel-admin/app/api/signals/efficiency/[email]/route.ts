import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getEfficiencySignalForEngineer } from "@/lib/sentinel-api";
import type { EfficiencyWindow } from "@/lib/types";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ email: string }> }
) {
  try {
    const { email } = await params;
    const window = (request.nextUrl.searchParams.get("window") ?? "30d") as EfficiencyWindow;
    const data = await getEfficiencySignalForEngineer(email, window);
    if (data === null) {
      return NextResponse.json(
        { error: "no rollup for this engineer/window — has the rollup job run yet?" },
        { status: 404 }
      );
    }
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}
