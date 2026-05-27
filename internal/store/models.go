package store

import "time"

type Event struct {
	EventID             string    `json:"event_id"`
	EngineerID          string    `json:"engineer_id"`
	OccurredAt          time.Time `json:"occurred_at"`
	Source              string    `json:"source"`
	MetricName          string    `json:"metric_name"`
	CostUSD             *float64  `json:"cost_usd,omitempty"`
	TokensInput         *int      `json:"tokens_input,omitempty"`
	TokensOutput        *int      `json:"tokens_output,omitempty"`
	TokensCacheRead     *int      `json:"tokens_cache_read,omitempty"`
	TokensCacheCreation *int      `json:"tokens_cache_creation,omitempty"`
	Model               string    `json:"model,omitempty"`
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
	EngineerID         string
	Period             string  // "daily" or "monthly"
	ThresholdPct       int     // 80, 100
	TriggeredAt        time.Time
	SpendAtTriggerUSD  float64
	BudgetUSD          float64
	SlackMessageTS     string  // empty if Slack not sent
	NotifiedManager    bool
}
