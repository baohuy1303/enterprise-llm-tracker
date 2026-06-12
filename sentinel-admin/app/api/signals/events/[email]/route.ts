import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getSignalEventsForEngineer } from "@/lib/sentinel-api";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ email: string }> }
) {
  try {
    const { email } = await params;
    const sp = request.nextUrl.searchParams;
    const data = await getSignalEventsForEngineer(email, {
      type: sp.get("type") ?? undefined,
      severity: sp.get("severity") ?? undefined,
      since: sp.get("since") ?? undefined,
      limit: sp.get("limit") ? Number(sp.get("limit")) : undefined,
    });
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}
