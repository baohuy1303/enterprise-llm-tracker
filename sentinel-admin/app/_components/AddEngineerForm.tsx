"use client";

import { useState, type FormEvent, type ReactNode } from "react";
import { useRouter } from "next/navigation";
import type { ApiErrorBody, Engineer, EngineerCreateInput } from "@/lib/types";

const INITIAL_STATE = {
  email: "",
  name: "",
  github_username: "",
  team: "",
  daily_budget_usd: "",
  monthly_budget_usd: "",
  slack_user_id: "",
  manager_slack_id: "",
};

const inputClass =
  "w-full rounded-md border border-zinc-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-zinc-500 focus:outline-none dark:border-zinc-700 dark:bg-zinc-900";

function Field({ label, required, children }: { label: string; required?: boolean; children: ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1 block text-sm font-medium">
        {label}
        {required ? <span className="text-red-500"> *</span> : null}
      </span>
      {children}
    </label>
  );
}

export function AddEngineerForm() {
  const router = useRouter();
  const [form, setForm] = useState(INITIAL_STATE);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  function update<K extends keyof typeof INITIAL_STATE>(key: K, value: string) {
    setForm((prev) => ({ ...prev, [key]: value }));
  }

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    setSubmitting(true);

    const body: EngineerCreateInput = {
      email: form.email.trim(),
      name: form.name.trim(),
      github_username: form.github_username.trim(),
    };
    if (form.team.trim()) body.team = form.team.trim();
    if (form.slack_user_id.trim()) body.slack_user_id = form.slack_user_id.trim();
    if (form.manager_slack_id.trim()) body.manager_slack_id = form.manager_slack_id.trim();
    if (form.daily_budget_usd.trim()) body.daily_budget_usd = Number(form.daily_budget_usd);
    if (form.monthly_budget_usd.trim()) body.monthly_budget_usd = Number(form.monthly_budget_usd);

    try {
      const res = await fetch("/api/engineers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const errBody: ApiErrorBody = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(errBody.error || "failed to create engineer");
      }

      const created: Engineer = await res.json();
      router.push(`/engineers/${encodeURIComponent(created.email)}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to create engineer");
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {error ? (
        <p className="rounded-md bg-red-50 px-3 py-2 text-sm text-red-700 dark:bg-red-900/30 dark:text-red-300">
          {error}
        </p>
      ) : null}

      <Field label="Email" required>
        <input
          type="email"
          required
          value={form.email}
          onChange={(e) => update("email", e.target.value)}
          className={inputClass}
          placeholder="jane@company.com"
        />
      </Field>

      <Field label="Name" required>
        <input
          type="text"
          required
          value={form.name}
          onChange={(e) => update("name", e.target.value)}
          className={inputClass}
          placeholder="Jane Doe"
        />
      </Field>

      <Field label="GitHub username" required>
        <input
          type="text"
          required
          value={form.github_username}
          onChange={(e) => update("github_username", e.target.value)}
          className={inputClass}
          placeholder="janedoe"
        />
      </Field>

      <Field label="Team">
        <input
          type="text"
          value={form.team}
          onChange={(e) => update("team", e.target.value)}
          className={inputClass}
          placeholder="platform"
        />
      </Field>

      <div className="grid grid-cols-2 gap-4">
        <Field label="Daily budget (USD)">
          <input
            type="number"
            min="0"
            step="0.01"
            value={form.daily_budget_usd}
            onChange={(e) => update("daily_budget_usd", e.target.value)}
            className={inputClass}
            placeholder="default if blank"
          />
        </Field>
        <Field label="Monthly budget (USD)">
          <input
            type="number"
            min="0"
            step="0.01"
            value={form.monthly_budget_usd}
            onChange={(e) => update("monthly_budget_usd", e.target.value)}
            className={inputClass}
            placeholder="default if blank"
          />
        </Field>
      </div>

      <Field label="Slack user ID">
        <input
          type="text"
          value={form.slack_user_id}
          onChange={(e) => update("slack_user_id", e.target.value)}
          className={inputClass}
          placeholder="U01234567"
        />
      </Field>

      <Field label="Manager Slack ID">
        <input
          type="text"
          value={form.manager_slack_id}
          onChange={(e) => update("manager_slack_id", e.target.value)}
          className={inputClass}
          placeholder="U07654321"
        />
      </Field>

      <button
        type="submit"
        disabled={submitting}
        className="rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white hover:bg-zinc-700 disabled:opacity-50 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300"
      >
        {submitting ? "Adding…" : "Add engineer"}
      </button>
    </form>
  );
}
