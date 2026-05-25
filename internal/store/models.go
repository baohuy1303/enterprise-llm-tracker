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
