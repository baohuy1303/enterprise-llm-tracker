"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import {
  EFFICIENCY_WINDOWS,
  type EfficiencyWindow,
  type EngineerWithUsage,
  type LeaderboardEntry,
  type LeaderboardResponse,
} from "@/lib/types";
import { formatPercent, formatUSD } from "@/lib/format";
import { Card } from "../components/Card";

const POLL_INTERVAL_MS = 10_000;

const WINDOW_LABELS: Record<EfficiencyWindow, string> = {
  "1d": "1 day",
  "7d": "7 days",
  "30d": "30 days",
  "180d": "180 days",
};

interface Props {
  engineers: EngineerWithUsage[];
  initialEntries: LeaderboardEntry[];
  initialWindow: EfficiencyWindow;
}

export function LeaderboardTable({ engineers, initialEntries, initialWindow }: Props) {
  const [activeWindow, setActiveWindow] = useState<EfficiencyWindow>(initialWindow);
  const [entries, setEntries] = useState(initialEntries);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const isFirstRender = useRef(true);

  const engineerById = new Map(engineers.map((eng) => [eng.id, eng]));

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      try {
        const res = await fetch(`/api/leaderboard?window=${activeWindow}`);
        if (!res.ok) throw new Error("failed to load leaderboard");
        const data: LeaderboardResponse = await res.json();
        if (!cancelled) {
          setEntries(data.entries ?? []);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "failed to load leaderboard");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    if (isFirstRender.current) {
      isFirstRender.current = false;
    } else {
      load();
    }

    const interval = setInterval(load, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [activeWindow]);

  const sorted = [...entries].sort((a, b) => a.dollars_per_merged_pr - b.dollars_per_merged_pr);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold">Efficiency leaderboard</h1>
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            Sorted by $ / merged PR (lower is better) · refreshes every 10s
          </p>
        </div>
        <div className="flex gap-1 rounded-md border border-zinc-300 p-1 dark:border-zinc-700">
          {EFFICIENCY_WINDOWS.map((w) => (
            <button
              key={w}
              type="button"
              onClick={() => setActiveWindow(w)}
              className={
                w === activeWindow
                  ? "rounded px-3 py-1 text-sm font-medium bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900"
                  : "rounded px-3 py-1 text-sm font-medium text-zinc-600 hover:bg-zinc-100 dark:text-zinc-300 dark:hover:bg-zinc-800"
              }
            >
              {WINDOW_LABELS[w]}
            </button>
          ))}
        </div>
      </div>

      {error ? <p className="text-sm text-red-600 dark:text-red-400">{error}</p> : null}

      <Card className="overflow-x-auto p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-200 text-left text-xs uppercase tracking-wide text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
              <th className="px-4 py-3 font-medium">Rank</th>
              <th className="px-4 py-3 font-medium">Engineer</th>
              <th className="px-4 py-3 font-medium">$ / merged PR</th>
              <th className="px-4 py-3 font-medium">Merged PRs</th>
              <th className="px-4 py-3 font-medium">Revert rate</th>
              <th className="px-4 py-3 font-medium">Spend</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((entry, idx) => {
              const eng = engineerById.get(entry.engineer_id);
              return (
                <tr key={entry.engineer_id} className="border-b border-zinc-100 last:border-0 dark:border-zinc-900">
                  <td className="px-4 py-3 text-zinc-500 dark:text-zinc-400">{idx + 1}</td>
                  <td className="px-4 py-3">
                    {eng ? (
                      <>
                        <Link href={`/engineers/${encodeURIComponent(eng.email)}`} className="font-medium hover:underline">
                          {eng.name}
                        </Link>
                        <div className="text-xs text-zinc-500 dark:text-zinc-400">{eng.email}</div>
                      </>
                    ) : (
                      <span className="font-mono text-xs">{entry.engineer_id}</span>
                    )}
                  </td>
                  <td className="px-4 py-3 font-medium">{formatUSD(entry.dollars_per_merged_pr)}</td>
                  <td className="px-4 py-3">{entry.merged_pr_count}</td>
                  <td className="px-4 py-3">{formatPercent(entry.revert_rate)}</td>
                  <td className="px-4 py-3">{formatUSD(entry.cost_usd)}</td>
                </tr>
              );
            })}
            {sorted.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-zinc-500 dark:text-zinc-400">
                  {loading ? "Loading…" : "No efficiency data for this window yet."}
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
