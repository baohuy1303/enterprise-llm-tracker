package store

import "time"

type Event struct {
	EventID             string            `json:"event_id"`
	EngineerID          string            `json:"engineer_id"`
	OccurredAt          time.Time         `json:"occurred_at"`
	Source              string            `json:"source"`
	MetricName          string            `json:"metric_name"`
	CostUSD             *float64          `json:"cost_usd,omitempty"`
	TokensInput         *int              `json:"tokens_input,omitempty"`
	TokensOutput        *int              `json:"tokens_output,omitempty"`
	TokensCacheRead     *int              `json:"tokens_cache_read,omitempty"`
	TokensCacheCreation *int              `json:"tokens_cache_creation,omitempty"`
	Model               string            `json:"model,omitempty"`
	Raw                 map[string]string `json:"raw,omitempty"`
}

// GitHubPR mirrors the github_prs row. Files holds the list of file paths
// touched by the PR — used by the revert detector for file-overlap scoring.
type GitHubPR struct {
	EngineerGitHub string
	Repo           string
	PRNumber       int
	Title          string
	State          string // "OPEN" | "CLOSED" | "MERGED"
	CreatedAt      time.Time
	MergedAt       *time.Time
	ReviewCount    int
	FilesChanged   int
	Files          []string
	Reverted       bool
	RevertedAt     *time.Time
	LastSyncedAt   time.Time
}

// EfficiencySnapshot is a per-engineer rollup over a window.
type EfficiencySnapshot struct {
	EngineerID         string
	WindowStart        time.Time
	WindowEnd          time.Time
	CostUSD            float64
	MergedPRCount      int
	RevertRate         float64 // 0..1
	DollarsPerMergedPR float64 // 0 when MergedPRCount == 0
	ComputedAt         time.Time
}

type ThresholdTrigger struct {
	EngineerID        string
	Period            string // "daily" or "monthly"
	ThresholdPct      int    // 80, 100
	TriggeredAt       time.Time
	SpendAtTriggerUSD float64
	BudgetUSD         float64
	SlackMessageTS    string // empty if Slack not sent
	NotifiedManager   bool
}

// EngineerSignal is one per-engineer × per-window efficiency rollup row.
// Replaces EfficiencySnapshot for new reads; same data plus PR breakdown,
// cache ratio, model mix, and peer percentile.
type EngineerSignal struct {
	EngineerID  string    `json:"engineer_id"`
	WindowName  string    `json:"window_name"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`

	PRsOpened         int `json:"prs_opened"`
	PRsMerged         int `json:"prs_merged"`
	PRsClosedUnmerged int `json:"prs_closed_unmerged"`
	PRsReverted       int `json:"prs_reverted"`

	CostUSD        float64            `json:"cost_usd"`
	LinesShipped   int64              `json:"lines_shipped"`
	CacheHitRatio  *float64           `json:"cache_hit_ratio,omitempty"`
	DollarsPerKLOC *float64           `json:"dollars_per_kloc,omitempty"`
	ModelMix       map[string]float64 `json:"model_mix,omitempty"`

	TeamDollarsPerPRMedian *float64 `json:"team_dollars_per_pr_median,omitempty"`
	PeerPercentile         *int     `json:"peer_percentile,omitempty"`

	ComputedAt time.Time `json:"computed_at"`
}

// SignalEvent is a single detected anomaly written by the signal-detector.
// Used for the dashboard "what fired recently?" view.
type SignalEvent struct {
	ID            int64          `json:"id"`
	EngineerID    string         `json:"engineer_id"`
	SignalType    string         `json:"signal_type"` // burst | spend_zscore_high | rhythm_break
	Severity      string         `json:"severity"`    // info | warn | critical
	OccurredAt    time.Time      `json:"occurred_at"`
	ObservedValue *float64       `json:"observed_value,omitempty"`
	BaselineValue *float64       `json:"baseline_value,omitempty"`
	ZScore        *float64       `json:"z_score,omitempty"`
	Context       map[string]any `json:"context,omitempty"`
	Notified      bool           `json:"notified"`
	NotifiedAt    *time.Time     `json:"notified_at,omitempty"`
}

// EngineerBaseline is the cached per-engineer baseline used by the detector.
// Persisted to Redis as individual keys; loaded into this struct in-memory.
type EngineerBaseline struct {
	DailyMean        float64
	DailyStddev      float64
	HourlyP95        float64
	ActiveDays       int
	HourDistribution [24]float64 // fractions of activity by hour-of-day (UTC)
	RebuiltAt        time.Time
}
