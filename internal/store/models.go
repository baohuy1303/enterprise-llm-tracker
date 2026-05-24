package store

import "time"

type Event struct {
	EngineerID          string
	OccurredAt          time.Time
	Source              string
	MetricName          string
	CostUSD             *float64
	TokensInput         *int
	TokensOutput        *int
	TokensCacheRead     *int
	TokensCacheCreation *int
	Model               string
	Raw                 map[string]string
}
