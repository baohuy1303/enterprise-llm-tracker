// Server-only typed client for the sentinel-api Go service.
//
// This module reads SENTINEL_API_URL / SENTINEL_ADMIN_TOKEN from the server
// environment and must never be imported from a "use client" component —
// doing so would bundle the admin bearer token into client JS. Only import
// this from Server Components, Route Handlers, or Server Actions.

import type {
  ApiErrorBody,
  Engineer,
  EngineerCreateInput,
  EngineerDetail,
  EngineerListResponse,
  EngineerSignal,
  EngineerUpdateInput,
  EfficiencyListResponse,
  EfficiencyWindow,
  LeaderboardResponse,
  ReadyzResponse,
  RecentUsageResponse,
  SignalEventsForEngineerResponse,
  SignalEventsResponse,
  StatusResponse,
} from "./types";

const API_URL = (process.env.SENTINEL_API_URL ?? "").replace(/\/+$/, "");
const ADMIN_TOKEN = process.env.SENTINEL_ADMIN_TOKEN ?? "";

export class SentinelApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "SentinelApiError";
    this.status = status;
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
  opts?: { admin?: boolean }
): Promise<T> {
  if (!API_URL) {
    throw new Error("SENTINEL_API_URL is not configured");
  }

  const headers = new Headers(init?.headers);
  if (opts?.admin !== false) {
    if (!ADMIN_TOKEN) {
      throw new Error("SENTINEL_ADMIN_TOKEN is not configured");
    }
    headers.set("Authorization", `Bearer ${ADMIN_TOKEN}`);
  }
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const res = await fetch(`${API_URL}${path}`, {
    ...init,
    headers,
    cache: "no-store",
  });

  if (!res.ok) {
    let message = res.statusText;
    try {
      const body = (await res.json()) as ApiErrorBody;
      if (body?.error) message = body.error;
    } catch {
      // non-JSON error body — fall back to statusText
    }
    throw new SentinelApiError(res.status, message);
  }

  if (res.status === 204) {
    return undefined as T;
  }
  return (await res.json()) as T;
}

function buildQuery(params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      search.set(key, String(value));
    }
  }
  const qs = search.toString();
  return qs ? `?${qs}` : "";
}

// --- Engineers ---------------------------------------------------------

export function listEngineers(): Promise<EngineerListResponse> {
  return request<EngineerListResponse>("/admin/engineers");
}

export function createEngineer(input: EngineerCreateInput): Promise<Engineer> {
  return request<Engineer>("/admin/engineers", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function getEngineer(email: string): Promise<EngineerDetail> {
  return request<EngineerDetail>(`/admin/engineers/${encodeURIComponent(email)}`);
}

export function updateEngineer(
  email: string,
  input: EngineerUpdateInput
): Promise<Engineer> {
  return request<Engineer>(`/admin/engineers/${encodeURIComponent(email)}`, {
    method: "PUT",
    body: JSON.stringify(input),
  });
}

export function deactivateEngineer(email: string): Promise<void> {
  return request<void>(`/admin/engineers/${encodeURIComponent(email)}`, {
    method: "DELETE",
  });
}

// --- Leaderboard & usage -------------------------------------------------

export function getLeaderboard(
  window: EfficiencyWindow = "7d"
): Promise<LeaderboardResponse> {
  return request<LeaderboardResponse>(`/admin/leaderboard${buildQuery({ window })}`);
}

export function getRecentUsage(limit = 100): Promise<RecentUsageResponse> {
  return request<RecentUsageResponse>(`/admin/usage/recent${buildQuery({ limit })}`);
}

// --- Ops triggers ----------------------------------------------------------

export function refreshEfficiency(): Promise<StatusResponse> {
  return request<StatusResponse>("/admin/refresh-efficiency", { method: "POST" });
}

export function refreshRegistry(): Promise<StatusResponse> {
  return request<StatusResponse>("/admin/registry/refresh", { method: "POST" });
}

// --- Efficiency signals ----------------------------------------------------

export function getEfficiencySignals(
  window: EfficiencyWindow = "30d"
): Promise<EfficiencyListResponse> {
  return request<EfficiencyListResponse>(`/admin/signals/efficiency${buildQuery({ window })}`);
}

export async function getEfficiencySignalForEngineer(
  email: string,
  window: EfficiencyWindow = "30d"
): Promise<EngineerSignal | null> {
  try {
    return await request<EngineerSignal>(
      `/admin/signals/efficiency/${encodeURIComponent(email)}${buildQuery({ window })}`
    );
  } catch (err) {
    if (err instanceof SentinelApiError && err.status === 404) {
      return null;
    }
    throw err;
  }
}

// --- Signal events -----------------------------------------------------------

export interface SignalEventsQuery {
  engineer?: string;
  type?: string;
  severity?: string;
  since?: string;
  limit?: number;
}

export function getSignalEvents(
  query: SignalEventsQuery = {}
): Promise<SignalEventsResponse> {
  return request<SignalEventsResponse>(`/admin/signals/events${buildQuery(query)}`);
}

export function getSignalEventsForEngineer(
  email: string,
  query: Omit<SignalEventsQuery, "engineer"> = {}
): Promise<SignalEventsForEngineerResponse> {
  return request<SignalEventsForEngineerResponse>(
    `/admin/signals/events/${encodeURIComponent(email)}${buildQuery(query)}`
  );
}

// --- Health (no admin token required) ---------------------------------------

export async function getHealthz(): Promise<boolean> {
  if (!API_URL) return false;
  try {
    const res = await fetch(`${API_URL}/healthz`, { cache: "no-store" });
    return res.ok;
  } catch {
    return false;
  }
}

export function getReadyz(): Promise<ReadyzResponse> {
  return request<ReadyzResponse>("/readyz", undefined, { admin: false });
}
