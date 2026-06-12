import { NextRequest, NextResponse } from "next/server";
import { apiError } from "@/lib/api-route";
import { deactivateEngineer, getEngineer, updateEngineer } from "@/lib/sentinel-api";
import type { EngineerUpdateInput } from "@/lib/types";

type RouteParams = { params: Promise<{ email: string }> };

export async function GET(_request: NextRequest, { params }: RouteParams) {
  try {
    const { email } = await params;
    const data = await getEngineer(decodeURIComponent(email));
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}

export async function PUT(request: NextRequest, { params }: RouteParams) {
  try {
    const { email } = await params;
    const body = (await request.json()) as EngineerUpdateInput;
    const data = await updateEngineer(decodeURIComponent(email), body);
    return NextResponse.json(data);
  } catch (err) {
    return apiError(err);
  }
}

export async function DELETE(_request: NextRequest, { params }: RouteParams) {
  try {
    const { email } = await params;
    await deactivateEngineer(decodeURIComponent(email));
    return new NextResponse(null, { status: 204 });
  } catch (err) {
    return apiError(err);
  }
}
