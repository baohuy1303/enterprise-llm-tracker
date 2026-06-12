import { NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { refreshRegistry } from "@/lib/sentinel-api";

export async function POST() {
  try {
    const data = await refreshRegistry();
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}
