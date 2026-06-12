import type { ReactNode } from "react";

const VARIANTS: Record<string, string> = {
  ok: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300",
  info: "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300",
  warn: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300",
  critical: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300",
  degraded: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300",
  error: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300",
  neutral: "bg-zinc-100 text-zinc-800 dark:bg-zinc-800 dark:text-zinc-300",
};

export function Badge({ variant = "neutral", children }: { variant?: string; children: ReactNode }) {
  const cls = VARIANTS[variant] ?? VARIANTS.neutral;
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${cls}`}>
      {children}
    </span>
  );
}
