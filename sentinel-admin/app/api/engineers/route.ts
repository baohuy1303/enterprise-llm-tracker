import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { createEngineer, listEngineers } from "@/lib/sentinel-api";
import type { EngineerCreateInput } from "@/lib/types";

export async function GET() {
  try {
    const data = await listEngineers();
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as EngineerCreateInput;
    const data = await createEngineer(body);
    return NextResponse.json(data, { status: 201 });
  } catch (err) {
    return apiError(err);
  }
}
