"use client";

import { useEffect, useState, type FormEvent } from "react";
import { useRouter } from "next/navigation";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type {
  ApiErrorBody,
  Engineer,
  EngineerDetail as EngineerDetailData,
  EngineerUpdateInput,
} from "@/lib/types";
import { formatDate, formatDateTime, formatPercent, formatUSD } from "@/lib/format";
import { Badge } from "../components/Badge";
import { Card, StatCard } from "../components/Card";
import { ProgressBar } from "../components/ProgressBar";

const POLL_INTERVAL_MS = 10_000;

const inputClass =
  "w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-zinc-500 focus:outline-none dark:border-zinc-700 dark:bg-zinc-900";

interface Props {
  email: string;
  initialDetail: EngineerDetailData;
}

export function EngineerDetail({ email, initialDetail }: Props) {
  const router = useRouter();
  const [detail, setDetail] = useState(initialDetail);
  const [pollError, setPollError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function poll() {
      try {
        const res = await fetch(`/api/engineers/${encodeURIComponent(email)}`);
        if (!res.ok) throw new Error("failed to refresh");
        const data: EngineerDetailData = await res.json();
        if (!cancelled) {
          setDetail(data);
          setPollError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setPollError(err instanceof Error ? err.message : "failed to refresh");
        }
      }
    }

    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [email]);

  const chartData = detail.usage_history.map((d) => ({
    label: formatDate(d.date),
    cost: d.cost_usd,
  }));

  const snap = detail.efficiency_snapshot;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold">{detail.name}</h1>
          <p className="text-sm text-zinc-500 dark:text-zinc-400">{detail.email}</p>
          <div className="mt-2 flex flex-wrap items-center gap-2 text-sm">
            {detail.active ? <Badge variant="ok">active</Badge> : <Badge variant="neutral">inactive</Badge>}
            {detail.team ? <Badge variant="neutral">{detail.team}</Badge> : null}
            {detail.github_username ? (
              <a
                href={`https://github.com/${detail.github_username}`}
                target="_blank"
                rel="noreferrer"
                className="text-zinc-500 hover:underline dark:text-zinc-400"
              >
                @{detail.github_username}
              </a>
            ) : null}
          </div>
        </div>
        <DeactivateButton email={email} active={detail.active} onDeactivated={() => router.push("/")} />
      </div>

      {pollError ? <p className="text-sm text-red-600 dark:text-red-400">Refresh failed: {pollError}</p> : null}

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard
          label="Spend today"
          value={formatUSD(detail.cost_today_usd)}
          sub={
            detail.daily_budget_usd ? (
              <>
                <span>of {formatUSD(detail.daily_budget_usd)} budget</span>
                <div className="mt-2">
                  <ProgressBar value={detail.cost_today_usd} max={detail.daily_budget_usd} />
                </div>
              </>
            ) : (
              "no daily budget set"
            )
          }
        />
        <StatCard
          label="Spend this month"
          value={formatUSD(detail.cost_month_usd)}
          sub={
            detail.monthly_budget_usd ? (
              <>
                <span>of {formatUSD(detail.monthly_budget_usd)} budget</span>
                <div className="mt-2">
                  <ProgressBar value={detail.cost_month_usd} max={detail.monthly_budget_usd} />
                </div>
              </>
            ) : (
              "no monthly budget set"
            )
          }
        />
        <StatCard
          label="$ / merged PR (30d)"
          value={snap ? formatUSD(snap.DollarsPerMergedPR) : "—"}
          sub={snap ? `${snap.MergedPRCount} merged PRs` : "no efficiency rollup yet"}
        />
        <StatCard
          label="Revert rate (30d)"
          value={snap ? formatPercent(snap.RevertRate) : "—"}
          sub={detail.last_otel_at ? `last OTel ${formatDateTime(detail.last_otel_at)}` : "no OTel data yet"}
        />
      </div>

      <Card>
        <h2 className="mb-4 text-sm font-medium text-zinc-500 dark:text-zinc-400">Daily spend (last 30 days)</h2>
        {chartData.length > 0 ? (
          <div className="h-64 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-zinc-200 dark:stroke-zinc-800" />
                <XAxis dataKey="label" tick={{ fontSize: 12 }} minTickGap={24} />
                <YAxis tick={{ fontSize: 12 }} width={64} tickFormatter={(value) => formatUSD(Number(value))} />
                <Tooltip formatter={(value) => formatUSD(Number(value))} />
                <Area type="monotone" dataKey="cost" name="Cost" stroke="#52525b" fill="#a1a1aa" fillOpacity={0.3} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">No usage recorded yet.</p>
        )}
      </Card>

      <Card className="overflow-x-auto p-0">
        <h2 className="px-4 pt-4 pb-2 text-sm font-medium text-zinc-500 dark:text-zinc-400">
          Recent threshold triggers
        </h2>
        {detail.recent_triggers && detail.recent_triggers.length > 0 ? (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-200 text-left text-xs uppercase tracking-wide text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
                <th className="px-4 py-2 font-medium">Period</th>
                <th className="px-4 py-2 font-medium">Threshold</th>
                <th className="px-4 py-2 font-medium">Spend</th>
                <th className="px-4 py-2 font-medium">Budget</th>
                <th className="px-4 py-2 font-medium">Triggered</th>
                <th className="px-4 py-2 font-medium">Manager notified</th>
              </tr>
            </thead>
            <tbody>
              {detail.recent_triggers.map((t, idx) => (
                <tr
                  key={`${t.Period}-${t.TriggeredAt}-${idx}`}
                  className="border-b border-zinc-100 last:border-0 dark:border-zinc-900"
                >
                  <td className="px-4 py-2 capitalize">{t.Period}</td>
                  <td className="px-4 py-2">{t.ThresholdPct}%</td>
                  <td className="px-4 py-2">{formatUSD(t.SpendAtTriggerUSD)}</td>
                  <td className="px-4 py-2">{formatUSD(t.BudgetUSD)}</td>
                  <td className="px-4 py-2">{formatDateTime(t.TriggeredAt)}</td>
                  <td className="px-4 py-2">{t.NotifiedManager ? "yes" : "no"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="px-4 pb-4 text-sm text-zinc-500 dark:text-zinc-400">No threshold triggers recorded.</p>
        )}
      </Card>

      <BudgetEditor
        email={email}
        engineer={detail}
        onUpdated={(updated) => setDetail((prev) => ({ ...prev, ...updated }))}
      />
    </div>
  );
}

function DeactivateButton({
  email,
  active,
  onDeactivated,
}: {
  email: string;
  active: boolean;
  onDeactivated: () => void;
}) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleClick() {
    if (!active) return;
    if (!window.confirm(`Deactivate ${email}? This cannot be undone from this dashboard.`)) return;

    setSubmitting(true);
    setError(null);
    try {
      const res = await fetch(`/api/engineers/${encodeURIComponent(email)}`, { method: "DELETE" });
      if (!res.ok) {
        const body: ApiErrorBody = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(body.error || "failed to deactivate engineer");
      }
      onDeactivated();
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to deactivate engineer");
      setSubmitting(false);
    }
  }

  return (
    <div className="text-right">
      <button
        type="button"
        onClick={handleClick}
        disabled={!active || submitting}
        className="rounded-md border border-red-300 px-3 py-2 text-sm font-medium text-red-700 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-red-800 dark:text-red-400 dark:hover:bg-red-950"
      >
        {active ? (submitting ? "Deactivating…" : "Deactivate") : "Inactive"}
      </button>
      {error ? <p className="mt-1 text-sm text-red-600 dark:text-red-400">{error}</p> : null}
    </div>
  );
}

function BudgetEditor({
  email,
  engineer,
  onUpdated,
}: {
  email: string;
  engineer: Engineer;
  onUpdated: (updated: Engineer) => void;
}) {
  const [daily, setDaily] = useState(String(engineer.daily_budget_usd ?? ""));
  const [monthly, setMonthly] = useState(String(engineer.monthly_budget_usd ?? ""));
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    setSaved(false);

    const body: EngineerUpdateInput = {
      daily_budget_usd: daily.trim() ? Number(daily) : 0,
      monthly_budget_usd: monthly.trim() ? Number(monthly) : 0,
    };

    try {
      const res = await fetch(`/api/engineers/${encodeURIComponent(email)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const errBody: ApiErrorBody = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(errBody.error || "failed to update budgets");
      }
      const updated: Engineer = await res.json();
      onUpdated(updated);
      setSaved(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to update budgets");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Card>
      <h2 className="mb-4 text-sm font-medium text-zinc-500 dark:text-zinc-400">Budgets</h2>
      <form onSubmit={handleSubmit} className="flex flex-wrap items-end gap-4">
        <label className="block">
          <span className="mb-1 block text-sm font-medium">Daily budget (USD)</span>
          <input
            type="number"
            min="0"
            step="0.01"
            value={daily}
            onChange={(e) => {
              setDaily(e.target.value);
              setSaved(false);
            }}
            className={inputClass}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-sm font-medium">Monthly budget (USD)</span>
          <input
            type="number"
            min="0"
            step="0.01"
            value={monthly}
            onChange={(e) => {
              setMonthly(e.target.value);
              setSaved(false);
            }}
            className={inputClass}
          />
        </label>
        <button
          type="submit"
          disabled={submitting}
          className="rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
        >
          {submitting ? "Saving…" : "Save budgets"}
        </button>
        {saved ? <span className="text-sm text-emerald-600 dark:text-emerald-400">Saved</span> : null}
      </form>
      {error ? <p className="mt-2 text-sm text-red-600 dark:text-red-400">{error}</p> : null}
    </Card>
  );
}
