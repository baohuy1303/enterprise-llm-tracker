"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import type {
  EngineerListResponse,
  EngineerWithUsage,
  LeaderboardEntry,
  LeaderboardResponse,
} from "@/lib/types";
import { formatUSD } from "@/lib/format";
import { Badge } from "../components/Badge";
import { Card } from "../components/Card";
import { ProgressBar } from "../components/ProgressBar";

const POLL_INTERVAL_MS = 10_000;

interface Props {
  initialEngineers: EngineerWithUsage[];
  initialLeaderboard: LeaderboardEntry[];
}

export function EngineerTable({ initialEngineers, initialLeaderboard }: Props) {
  const [engineers, setEngineers] = useState(initialEngineers);
  const [leaderboard, setLeaderboard] = useState(initialLeaderboard);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const [engineersRes, leaderboardRes] = await Promise.all([
          fetch("/api/engineers"),
          fetch("/api/leaderboard?window=7d"),
        ]);
        if (!engineersRes.ok) throw new Error("failed to load engineers");

        const engineersData: EngineerListResponse = await engineersRes.json();
        const leaderboardData: LeaderboardResponse | { entries: [] } = leaderboardRes.ok
          ? await leaderboardRes.json()
          : { entries: [] };

        if (!cancelled) {
          setEngineers(engineersData.engineers ?? []);
          setLeaderboard(leaderboardData.entries ?? []);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "failed to refresh");
        }
      }
    }

    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []);

  const dollarsPerPr = new Map(leaderboard.map((entry) => [entry.engineer_id, entry.dollars_per_merged_pr]));

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Engineers</h1>
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            {engineers.length} engineer{engineers.length === 1 ? "" : "s"} · refreshes every 10s
          </p>
        </div>
        <Link
          href="/engineers/new"
          className="rounded-md bg-zinc-900 px-3 py-2 text-sm font-medium text-white hover:bg-zinc-700 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
        >
          Add engineer
        </Link>
      </div>

      {error ? <p className="text-sm text-red-600 dark:text-red-400">Refresh failed: {error}</p> : null}

      <Card className="overflow-x-auto p-0">
        <table className="w-full min-w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-200 text-left text-xs uppercase tracking-wide text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
              <th className="px-4 py-3 font-medium">Engineer</th>
              <th className="px-4 py-3 font-medium">GitHub</th>
              <th className="px-4 py-3 font-medium">Today</th>
              <th className="px-4 py-3 font-medium">This month</th>
              <th className="px-4 py-3 font-medium">$ / merged PR (7d)</th>
              <th className="px-4 py-3 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {engineers.map((eng) => {
              const dpp = dollarsPerPr.get(eng.id);
              return (
                <tr key={eng.id} className="border-b border-zinc-100 last:border-0 dark:border-zinc-900">
                  <td className="px-4 py-3">
                    <Link href={`/engineers/${encodeURIComponent(eng.email)}`} className="font-medium hover:underline">
                      {eng.name}
                    </Link>
                    <div className="text-xs text-zinc-500 dark:text-zinc-400">{eng.email}</div>
                  </td>
                  <td className="px-4 py-3 text-zinc-600 dark:text-zinc-300">
                    {eng.github_username ? `@${eng.github_username}` : "—"}
                  </td>
                  <td className="px-4 py-3">
                    <div className="mb-1 whitespace-nowrap">
                      {formatUSD(eng.cost_today_usd)}
                      {eng.daily_budget_usd ? ` / ${formatUSD(eng.daily_budget_usd)}` : ""}
                    </div>
                    {eng.daily_budget_usd ? <ProgressBar value={eng.cost_today_usd} max={eng.daily_budget_usd} /> : null}
                  </td>
                  <td className="px-4 py-3">
                    <div className="mb-1 whitespace-nowrap">
                      {formatUSD(eng.cost_month_usd)}
                      {eng.monthly_budget_usd ? ` / ${formatUSD(eng.monthly_budget_usd)}` : ""}
                    </div>
                    {eng.monthly_budget_usd ? (
                      <ProgressBar value={eng.cost_month_usd} max={eng.monthly_budget_usd} />
                    ) : null}
                  </td>
                  <td className="px-4 py-3">{dpp !== undefined ? formatUSD(dpp) : "—"}</td>
                  <td className="px-4 py-3">
                    {eng.active ? <Badge variant="ok">active</Badge> : <Badge variant="neutral">inactive</Badge>}
                  </td>
                </tr>
              );
            })}
            {engineers.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-zinc-500 dark:text-zinc-400">
                  No engineers yet.{" "}
                  <Link href="/engineers/new" className="underline">
                    Add one
                  </Link>{" "}
                  to get started.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
