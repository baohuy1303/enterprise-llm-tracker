import { NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { refreshEfficiency } from "@/lib/sentinel-api";

export async function POST() {
  try {
    const data = await refreshEfficiency();
    return NextResponse.json(data, { status: 202 });
  } catch (err) {
    return apiError(err);
  }
}
