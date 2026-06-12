import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getSignalEvents } from "@/lib/sentinel-api";

export async function GET(request: NextRequest) {
  try {
    const sp = request.nextUrl.searchParams;
    const data = await getSignalEvents({
      engineer: sp.get("engineer") ?? undefined,
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
