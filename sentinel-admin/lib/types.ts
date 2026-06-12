// Mirrors internal/store/engineers.go Engineer (full snake_case json tags).
export interface Engineer {
  id: string;
  email: string;
  name: string;
  github_username: string;
  slack_user_id: string;
  manager_slack_id: string;
  daily_budget_usd: number;
  monthly_budget_usd: number;
  team: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

// Mirrors internal/service/engineer.go EngineerWithUsage. store.Engineer is
// embedded anonymously in Go, so its fields are flattened into this object.
export interface EngineerWithUsage extends Engineer {
  cost_today_usd: number;
  cost_month_usd: number;
  last_otel_at?: string | null;
}

// Mirrors internal/store/engineers.go DailyUsage.
export interface DailyUsage {
  date: string;
  cost_usd: number;
  tokens: number;
  event_count: number;
}

// Mirrors internal/store/models.go ThresholdTrigger. This struct has NO json
// tags, so Go's default marshaling uses the PascalCase field names verbatim.
export interface ThresholdTrigger {
  EngineerID: string;
  Period: string;
  ThresholdPct: number;
  TriggeredAt: string;
  SpendAtTriggerUSD: number;
  BudgetUSD: number;
  SlackMessageTS: string;
  NotifiedManager: boolean;
}

// Mirrors internal/store/models.go EfficiencySnapshot. Also has NO json tags
// — PascalCase keys, same as ThresholdTrigger above.
export interface EfficiencySnapshot {
  EngineerID: string;
  WindowStart: string;
  WindowEnd: string;
  CostUSD: number;
  MergedPRCount: number;
  RevertRate: number;
  DollarsPerMergedPR: number;
  ComputedAt: string;
}

// Mirrors internal/service/engineer.go EngineerDetail.
export interface EngineerDetail extends EngineerWithUsage {
  usage_history: DailyUsage[];
  recent_triggers: ThresholdTrigger[] | null;
  efficiency_snapshot?: EfficiencySnapshot | null;
}

// Mirrors internal/store/github.go LeaderboardEntry.
export interface LeaderboardEntry {
  engineer_id: string;
  window_start: string;
  window_end: string;
  cost_usd: number;
  merged_pr_count: number;
  revert_rate: number;
  dollars_per_merged_pr: number;
  computed_at: string;
}

// Mirrors internal/store/models.go EngineerSignal.
export interface EngineerSignal {
  engineer_id: string;
  window_name: string;
  window_start: string;
  window_end: string;
  prs_opened: number;
  prs_merged: number;
  prs_closed_unmerged: number;
  prs_reverted: number;
  cost_usd: number;
  lines_shipped: number;
  cache_hit_ratio?: number | null;
  dollars_per_kloc?: number | null;
  model_mix?: Record<string, number> | null;
  team_dollars_per_pr_median?: number | null;
  peer_percentile?: number | null;
  computed_at: string;
}

// Mirrors internal/store/models.go SignalEvent.
export interface SignalEvent {
  id: number;
  engineer_id: string;
  signal_type: string;
  severity: string;
  occurred_at: string;
  observed_value?: number | null;
  baseline_value?: number | null;
  z_score?: number | null;
  context?: Record<string, unknown> | null;
  notified: boolean;
  notified_at?: string | null;
}

// Mirrors internal/store/engineers.go RecentEvent.
export interface RecentEvent {
  id: number;
  engineer_id: string;
  occurred_at: string;
  source: string;
  metric_name: string;
  cost_usd?: number | null;
  model?: string;
}

// --- Admin API response envelopes -----------------------------------------

export interface EngineerListResponse {
  engineers: EngineerWithUsage[];
  count: number;
}

export interface LeaderboardResponse {
  window: string;
  start: string;
  end: string;
  entries: LeaderboardEntry[];
}

export interface RecentUsageResponse {
  events: RecentEvent[];
  count: number;
}

export interface EfficiencyListResponse {
  window: string;
  window_end: string;
  rows: EngineerSignal[];
  count: number;
}

export interface SignalEventsResponse {
  events: SignalEvent[];
  count: number;
}

export interface SignalEventsForEngineerResponse extends SignalEventsResponse {
  engineer: string;
}

export interface StatusResponse {
  status: string;
}

// Mirrors the /readyz response built in internal/http/router.go.
export interface ReadyzResponse {
  status: string;
  engineer_count: number;
  last_refresh_at: string;
  last_refresh_error: string;
  postgres: string;
  redis: string;
}

// --- Admin API request bodies ----------------------------------------------
// Mirrors createEngineerRequest / updateEngineerRequest in
// internal/http/admin_engineers.go. The Go decoder uses
// DisallowUnknownFields, so these shapes must match exactly.

export interface EngineerCreateInput {
  email: string;
  name: string;
  github_username: string;
  slack_user_id?: string;
  manager_slack_id?: string;
  daily_budget_usd?: number;
  monthly_budget_usd?: number;
  team?: string;
}

export interface EngineerUpdateInput {
  name?: string;
  github_username?: string;
  slack_user_id?: string;
  manager_slack_id?: string;
  daily_budget_usd?: number;
  monthly_budget_usd?: number;
  team?: string;
}

// Mirrors service.WindowForName valid window names.
export type EfficiencyWindow = "1d" | "7d" | "30d" | "180d";

export const EFFICIENCY_WINDOWS: EfficiencyWindow[] = ["1d", "7d", "30d", "180d"];

export interface ApiErrorBody {
  error: string;
}
