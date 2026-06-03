package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen     string           `yaml:"listen"`
	Postgres   PostgresConfig   `yaml:"postgres"`
	Redis      RedisConfig      `yaml:"redis"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	GitHub     GitHubConfig     `yaml:"github"`
	Slack      SlackConfig      `yaml:"slack"`
	Thresholds ThresholdsConfig `yaml:"thresholds"`
	Registry   RegistryConfig   `yaml:"registry"`
	Admin      AdminConfig      `yaml:"admin"`
	Signals    SignalsConfig    `yaml:"signals"`
}

type PostgresConfig struct {
	URL string `yaml:"url"`
}

type RedisConfig struct {
	Addr string `yaml:"addr"`
}

type KafkaConfig struct {
	Brokers             []string `yaml:"brokers"`
	Topic               string   `yaml:"topic"`
	ConsumerGroupPrefix string   `yaml:"consumer_group_prefix"`
}

type GitHubConfig struct {
	Org       string          `yaml:"org"`
	Repos     []string        `yaml:"repos"`
	TokenEnv  string          `yaml:"token_env"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	// LookbackDays bounds how far back the PR/commit fetcher pages.
	LookbackDays int `yaml:"lookback_days"`
	// IsUser controls whether the search uses `user:ORG` (personal account)
	// or `org:ORG` (GitHub organization). Set to true when Org is a personal
	// GitHub username rather than an organization name.
	IsUser bool `yaml:"is_user"`
}

type SchedulerConfig struct {
	// IntervalSeconds is the cron tick interval for the GitHub collector.
	IntervalSeconds int `yaml:"interval_seconds"`
	// MinTriggerIntervalSeconds debounces Kafka-driven refreshes — back-to-back
	// triggers within this window coalesce into one run.
	MinTriggerIntervalSeconds int `yaml:"min_trigger_interval_seconds"`
}

type AdminConfig struct {
	TokenEnv string `yaml:"token_env"`
}

type SlackConfig struct {
	BotTokenEnv           string `yaml:"bot_token_env"`
	DefaultManagerChannel string `yaml:"default_manager_channel"`
}

type ThresholdsConfig struct {
	NotifyAtPercent        []int `yaml:"notify_at_percent"`
	NotifyManagerAtPercent []int `yaml:"notify_manager_at_percent"`
}

type RegistryConfig struct {
	RefreshIntervalSeconds int `yaml:"refresh_interval_seconds"`
}

// SignalsConfig drives the signal-analytics subsystem in sentinel-workers.
// Defaults applied in Validate(); leaving the section empty in YAML is fine.
type SignalsConfig struct {
	// BaselineRebuildIntervalSeconds is the cron tick for the nightly job that
	// rebuilds per-engineer baselines (mean, stddev, hourly p95, hour-of-day
	// distribution). Defaults to 24h.
	BaselineRebuildIntervalSeconds int `yaml:"baseline_rebuild_interval_seconds"`
	// EfficiencyRollupIntervalSeconds is the cron tick for the nightly job
	// that aggregates usage_events + github_prs into engineer_signals.
	// Defaults to 24h.
	EfficiencyRollupIntervalSeconds int `yaml:"efficiency_rollup_interval_seconds"`
	// BaselineWindowDays bounds how far back we look when rebuilding the
	// daily-spend mean/stddev (defaults to 30).
	BaselineWindowDays int `yaml:"baseline_window_days"`
	// HourlyBaselineWindowDays bounds how far back we look for the hourly p95
	// used by burst detection (defaults to 14).
	HourlyBaselineWindowDays int `yaml:"hourly_baseline_window_days"`
	// MinBaselineDays is the minimum number of distinct active days an
	// engineer must have before we emit Z-score or rhythm-break signals for
	// them. Below this floor the detector skips to avoid noisy alerts on new
	// hires. Defaults to 14.
	MinBaselineDays int `yaml:"min_baseline_days"`
	// BurstWindowMinutes is the rolling window for burst detection
	// (defaults to 30 min).
	BurstWindowMinutes int `yaml:"burst_window_minutes"`
	// BurstMultiplier is the multiple of an engineer's hourly p95 that
	// triggers a `warn` burst (defaults to 2.0). `critical` fires at
	// BurstMultiplier * 1.5.
	BurstMultiplier float64 `yaml:"burst_multiplier"`
	// ZScoreWarn / ZScoreCritical are the stddev cutoffs for spend_zscore_high
	// (defaults 2.0 / 3.0).
	ZScoreWarn     float64 `yaml:"zscore_warn"`
	ZScoreCritical float64 `yaml:"zscore_critical"`
	// RhythmBreakPct is the maximum hour-of-day-distribution fraction that
	// counts as a rhythm break (defaults to 0.05 — i.e. <5% of historical
	// activity in that hour).
	RhythmBreakPct float64 `yaml:"rhythm_break_pct"`
	// NotifyBurstCritical sends an immediate Slack DM to engineer + manager
	// on burst-critical signals (defaults to true).
	NotifyBurstCritical bool `yaml:"notify_burst_critical"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Listen == "" {
		return fmt.Errorf("listen must be set")
	}
	if c.Postgres.URL == "" {
		return fmt.Errorf("postgres.url must be set")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr must be set")
	}
	if c.Slack.BotTokenEnv != "" && os.Getenv(c.Slack.BotTokenEnv) == "" {
		return fmt.Errorf("env var %q (slack.bot_token_env) is not set", c.Slack.BotTokenEnv)
	}
	if c.Registry.RefreshIntervalSeconds <= 0 {
		c.Registry.RefreshIntervalSeconds = 30
	}
	if c.GitHub.Scheduler.IntervalSeconds <= 0 {
		c.GitHub.Scheduler.IntervalSeconds = 3600
	}
	if c.GitHub.Scheduler.MinTriggerIntervalSeconds <= 0 {
		c.GitHub.Scheduler.MinTriggerIntervalSeconds = 300
	}
	if c.GitHub.LookbackDays <= 0 {
		c.GitHub.LookbackDays = 30
	}
	if c.Signals.BaselineRebuildIntervalSeconds <= 0 {
		c.Signals.BaselineRebuildIntervalSeconds = 86400
	}
	if c.Signals.EfficiencyRollupIntervalSeconds <= 0 {
		c.Signals.EfficiencyRollupIntervalSeconds = 86400
	}
	if c.Signals.BaselineWindowDays <= 0 {
		c.Signals.BaselineWindowDays = 30
	}
	if c.Signals.HourlyBaselineWindowDays <= 0 {
		c.Signals.HourlyBaselineWindowDays = 14
	}
	if c.Signals.MinBaselineDays <= 0 {
		c.Signals.MinBaselineDays = 14
	}
	if c.Signals.BurstWindowMinutes <= 0 {
		c.Signals.BurstWindowMinutes = 30
	}
	if c.Signals.BurstMultiplier <= 0 {
		c.Signals.BurstMultiplier = 2.0
	}
	if c.Signals.ZScoreWarn <= 0 {
		c.Signals.ZScoreWarn = 2.0
	}
	if c.Signals.ZScoreCritical <= 0 {
		c.Signals.ZScoreCritical = 3.0
	}
	if c.Signals.RhythmBreakPct <= 0 {
		c.Signals.RhythmBreakPct = 0.05
	}
	return nil
}
