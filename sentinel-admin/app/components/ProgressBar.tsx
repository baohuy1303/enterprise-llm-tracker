interface ProgressBarProps {
  value: number;
  max: number;
}

// Budget usage bar: green under 80%, amber 80-100%, red at/over 100%.
// Mirrors the 80/100 threshold percentages used by ThresholdTrigger.
export function ProgressBar({ value, max }: ProgressBarProps) {
  const ratio = max > 0 ? value / max : 0;
  const pct = Math.min(Math.max(ratio, 0), 1) * 100;
  const color =
    ratio >= 1 ? "bg-red-500" : ratio >= 0.8 ? "bg-amber-500" : "bg-emerald-500";

  return (
    <div
      className="h-2 w-full min-w-24 overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800"
      role="progressbar"
      aria-valuenow={Math.round(ratio * 100)}
      aria-valuemin={0}
      aria-valuemax={100}
    >
      <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
    </div>
  );
}
