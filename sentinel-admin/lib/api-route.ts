import { NextResponse } from "next/server";
import { SentinelApiError } from "./sentinel-api";

// Shared error mapper for app/api/* route handlers. Forwards the upstream
// sentinel-api status/message when available, otherwise returns a generic 500.
export function apiError(err: unknown): NextResponse {
  if (err instanceof SentinelApiError) {
    return NextResponse.json({ error: err.message }, { status: err.status });
  }
  console.error(err);
  return NextResponse.json({ error: "internal error" }, { status: 500 });
}
