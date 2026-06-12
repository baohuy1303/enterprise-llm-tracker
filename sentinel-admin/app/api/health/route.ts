import { NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { getHealthz, getReadyz } from "@/lib/sentinel-api";

export async function GET() {
  try {
    const [healthy, ready] = await Promise.all([getHealthz(), getReadyz()]);
    return NextResponse.json({ healthy, ...ready });
  } catch (err) {
    return apiError(err);
  }
}
