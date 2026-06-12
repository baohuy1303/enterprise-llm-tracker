"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import type { EngineerListResponse, EngineerWithUsage, ReadyzResponse, RecentEvent, RecentUsageResponse } from "@/lib/types";
import { formatDateTime, formatRelativeTime, formatUSD } from "@/lib/format";
import { Badge } from "../components/Badge";
import { Card, StatCard } from "../components/Card";

const POLL_INTERVAL_MS = 10_000;
const STALE_OTEL_MS = 24 * 60 * 60 * 1000; // 24h

export type HealthSummary = Partial<ReadyzResponse> & { healthy: boolean };

interface Props {
  initialHealth: HealthSummary;
  initialEngineers: EngineerWithUsage[];
  initialEvents: RecentEvent[];
}

export function HealthDashboard({ initialHealth, initialEngineers, initialEvents }: Props) {
  const [health, setHealth] = useState(initialHealth);
  const [engineers, setEngineers] = useState(initialEngineers);
  const [events, setEvents] = useState(initialEvents);
  const [error, setError] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionPending, setActionPending] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const [healthRes, engineersRes, eventsRes] = await Promise.all([
          fetch("/api/health"),
          fetch("/api/engineers"),
          fetch("/api/usage/recent?limit=50"),
        ]);

        if (cancelled) return;

        setHealth(healthRes.ok ? await healthRes.json() : { healthy: false });

        if (engineersRes.ok) {
          const data: EngineerListResponse = await engineersRes.json();
          setEngineers(data.engineers ?? []);
        }

        if (eventsRes.ok) {
          const data: RecentUsageResponse = await eventsRes.json();
          setEvents(data.events ?? []);
        }

        setError(null);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "failed to refresh");
      }
    }

    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  async function runAction(path: string, label: string) {
    setActionPending(label);
    setActionMessage(null);
    try {
      const res = await fetch(path, { method: "POST" });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.error || `${label} failed`);
      setActionMessage(`${label}: ${body.status ?? "ok"}`);
    } catch (err) {
      setActionMessage(err instanceof Error ? `${label} failed: ${err.message}` : `${label} failed`);
    } finally {
      setActionPending(null);
    }
  }

  const engineerById = new Map(engineers.map((eng) => [eng.id, eng]));

  const fiveMinAgo = Date.now() - 5 * 60 * 1000;
  const recentCount = events.filter((e) => new Date(e.occurred_at).getTime() >= fiveMinAgo).length;
  const ingestRate = (recentCount / 5).toFixed(1);

  const staleEngineers = [...engineers].sort((a, b) => {
    const at = a.last_otel_at ? new Date(a.last_otel_at).getTime() : Number.MIN_SAFE_INTEGER;
    const bt = b.last_otel_at ? new Date(b.last_otel_at).getTime() : Number.MIN_SAFE_INTEGER;
    return at - bt;
  });

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Pipeline health</h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">refreshes every 10s</p>
      </div>

      {error ? <p className="text-sm text-red-600 dark:text-red-400">Refresh failed: {error}</p> : null}

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="API"
          value={health.healthy ? <Badge variant="ok">healthy</Badge> : <Badge variant="critical">unreachable</Badge>}
        />
        <StatCard label="Postgres" value={<StatusBadge value={health.postgres} />} />
        <StatCard label="Redis" value={<StatusBadge value={health.redis} />} />
        <StatCard label="Tracked engineers" value={health.engineer_count ?? engineers.length} />
      </div>

      <Card>
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400">Registry refresh</h2>
            <p className="mt-1 text-sm">
              Last refresh:{" "}
              {health.last_refresh_at
                ? `${formatDateTime(health.last_refresh_at)} (${formatRelativeTime(health.last_refresh_at)})`
                : "never"}
            </p>
            {health.last_refresh_error ? (
              <p className="mt-1 text-sm text-red-600 dark:text-red-400">Last error: {health.last_refresh_error}</p>
            ) : null}
          </div>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => runAction("/api/registry/refresh", "Registry refresh")}
              disabled={actionPending !== null}
              className="rounded-md border border-zinc-300 px-3 py-2 text-sm font-medium hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-700 dark:hover:bg-zinc-800"
            >
              {actionPending === "Registry refresh" ? "Refreshing…" : "Refresh registry"}
            </button>
            <button
              type="button"
              onClick={() => runAction("/api/refresh-efficiency", "Efficiency recompute")}
              disabled={actionPending !== null}
              className="rounded-md border border-zinc-300 px-3 py-2 text-sm font-medium hover:bg-zinc-100 disabled:opacity-50 dark:border-zinc-700 dark:hover:bg-zinc-800"
            >
              {actionPending === "Efficiency recompute" ? "Requesting…" : "Recompute efficiency"}
            </button>
          </div>
        </div>
        {actionMessage ? <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-300">{actionMessage}</p> : null}
      </Card>

      <Card>
        <h2 className="text-sm font-medium text-zinc-500 dark:text-zinc-400">Ingest activity</h2>
        <p className="mt-1 text-2xl font-semibold">{ingestRate} events/min</p>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          {recentCount} events in the last 5 minutes (of {events.length} loaded)
        </p>
      </Card>

      <Card className="overflow-x-auto p-0">
        <h2 className="px-4 pt-4 pb-2 text-sm font-medium text-zinc-500 dark:text-zinc-400">Last OTel signal per engineer</h2>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-200 text-left text-xs uppercase tracking-wide text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
              <th className="px-4 py-2 font-medium">Engineer</th>
              <th className="px-4 py-2 font-medium">Last OTel seen</th>
              <th className="px-4 py-2 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {staleEngineers.map((eng) => {
              const lastMs = eng.last_otel_at ? new Date(eng.last_otel_at).getTime() : null;
              const stale = lastMs === null || Date.now() - lastMs > STALE_OTEL_MS;
              return (
                <tr key={eng.id} className="border-b border-zinc-100 last:border-0 dark:border-zinc-900">
                  <td className="px-4 py-2">
                    <Link href={`/engineers/${encodeURIComponent(eng.email)}`} className="font-medium hover:underline">
                      {eng.name}
                    </Link>
                    <div className="text-xs text-zinc-500 dark:text-zinc-400">{eng.email}</div>
                  </td>
                  <td className="px-4 py-2">
                    {eng.last_otel_at ? `${formatDateTime(eng.last_otel_at)} (${formatRelativeTime(eng.last_otel_at)})` : "never"}
                  </td>
                  <td className="px-4 py-2">
                    {stale ? <Badge variant="warn">stale</Badge> : <Badge variant="ok">recent</Badge>}
                  </td>
                </tr>
              );
            })}
            {staleEngineers.length === 0 ? (
              <tr>
                <td colSpan={3} className="px-4 py-8 text-center text-zinc-500 dark:text-zinc-400">
                  No engineers tracked.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </Card>

      <Card className="overflow-x-auto p-0">
        <h2 className="px-4 pt-4 pb-2 text-sm font-medium text-zinc-500 dark:text-zinc-400">Recent ingest events</h2>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-200 text-left text-xs uppercase tracking-wide text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
              <th className="px-4 py-2 font-medium">Time</th>
              <th className="px-4 py-2 font-medium">Engineer</th>
              <th className="px-4 py-2 font-medium">Source</th>
              <th className="px-4 py-2 font-medium">Metric</th>
              <th className="px-4 py-2 font-medium">Model</th>
              <th className="px-4 py-2 font-medium">Cost</th>
            </tr>
          </thead>
          <tbody>
            {events.slice(0, 15).map((e) => {
              const eng = engineerById.get(e.engineer_id);
              return (
                <tr key={e.id} className="border-b border-zinc-100 last:border-0 dark:border-zinc-900">
                  <td className="px-4 py-2 whitespace-nowrap">{formatRelativeTime(e.occurred_at)}</td>
                  <td className="px-4 py-2">
                    {eng ? eng.name : <span className="font-mono text-xs">{e.engineer_id}</span>}
                  </td>
                  <td className="px-4 py-2">{e.source}</td>
                  <td className="px-4 py-2">{e.metric_name}</td>
                  <td className="px-4 py-2">{e.model || "—"}</td>
                  <td className="px-4 py-2">{e.cost_usd != null ? formatUSD(e.cost_usd) : "—"}</td>
                </tr>
              );
            })}
            {events.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-zinc-500 dark:text-zinc-400">
                  No usage events recorded yet.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </Card>
    </div>
  );
}

function StatusBadge({ value }: { value?: string }) {
  if (!value) return <Badge variant="neutral">unknown</Badge>;
  const normalized = value.toLowerCase();
  if (normalized === "ok" || normalized === "up" || normalized === "healthy") {
    return <Badge variant="ok">{value}</Badge>;
  }
  if (normalized === "down" || normalized === "error" || normalized === "unhealthy") {
    return <Badge variant="critical">{value}</Badge>;
  }
  return <Badge variant="warn">{value}</Badge>;
}
