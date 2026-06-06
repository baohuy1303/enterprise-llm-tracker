package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "sentinel-*.yaml")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	f.Close()
	return f.Name()
}

const minimalYAML = `
listen: ":8081"
postgres:
  url: "postgres://sentinel:sentinel@localhost:5432/sentinel"
redis:
  addr: "localhost:6379"
kafka:
  brokers: ["localhost:9092"]
  topic: "claude.usage.events"
`

func TestLoad_Valid(t *testing.T) {
	path := writeTemp(t, minimalYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if cfg.Listen != ":8081" {
		t.Errorf("Listen = %q, want %q", cfg.Listen, ":8081")
	}
	if cfg.Postgres.URL != "postgres://sentinel:sentinel@localhost:5432/sentinel" {
		t.Errorf("Postgres.URL = %q", cfg.Postgres.URL)
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("Redis.Addr = %q, want localhost:6379", cfg.Redis.Addr)
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := writeTemp(t, "listen: [not a string")
	_, err := Load(path)
	if err == nil {
		t.Error("expected error for invalid YAML, got nil")
	}
}

func TestValidate_MissingListen(t *testing.T) {
	cfg := &Config{
		Postgres: PostgresConfig{URL: "postgres://x"},
		Redis:    RedisConfig{Addr: "localhost:6379"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing listen, got nil")
	}
}

func TestValidate_MissingPostgres(t *testing.T) {
	cfg := &Config{
		Listen: ":8081",
		Redis:  RedisConfig{Addr: "localhost:6379"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing postgres.url, got nil")
	}
}

func TestValidate_MissingRedis(t *testing.T) {
	cfg := &Config{
		Listen:   ":8081",
		Postgres: PostgresConfig{URL: "postgres://x"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing redis.addr, got nil")
	}
}

func TestValidate_SignalDefaults(t *testing.T) {
	path := writeTemp(t, minimalYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Signals section omitted → defaults applied
	if cfg.Signals.BurstWindowMinutes != 30 {
		t.Errorf("BurstWindowMinutes = %d, want 30", cfg.Signals.BurstWindowMinutes)
	}
	if cfg.Signals.BurstMultiplier != 2.0 {
		t.Errorf("BurstMultiplier = %v, want 2.0", cfg.Signals.BurstMultiplier)
	}
	if cfg.Signals.ZScoreWarn != 2.0 {
		t.Errorf("ZScoreWarn = %v, want 2.0", cfg.Signals.ZScoreWarn)
	}
	if cfg.Signals.ZScoreCritical != 3.0 {
		t.Errorf("ZScoreCritical = %v, want 3.0", cfg.Signals.ZScoreCritical)
	}
	if cfg.Signals.RhythmBreakPct != 0.05 {
		t.Errorf("RhythmBreakPct = %v, want 0.05", cfg.Signals.RhythmBreakPct)
	}
	if cfg.Signals.MinBaselineDays != 14 {
		t.Errorf("MinBaselineDays = %d, want 14", cfg.Signals.MinBaselineDays)
	}
	if cfg.Registry.RefreshIntervalSeconds != 30 {
		t.Errorf("Registry.RefreshIntervalSeconds = %d, want 30", cfg.Registry.RefreshIntervalSeconds)
	}
}

func TestValidate_SignalOverride(t *testing.T) {
	yaml := minimalYAML + `
signals:
  burst_window_minutes: 15
  burst_multiplier: 3.0
  zscore_warn: 1.5
  min_baseline_days: 5
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Signals.BurstWindowMinutes != 15 {
		t.Errorf("BurstWindowMinutes = %d, want 15", cfg.Signals.BurstWindowMinutes)
	}
	if cfg.Signals.BurstMultiplier != 3.0 {
		t.Errorf("BurstMultiplier = %v, want 3.0", cfg.Signals.BurstMultiplier)
	}
	if cfg.Signals.ZScoreWarn != 1.5 {
		t.Errorf("ZScoreWarn = %v, want 1.5", cfg.Signals.ZScoreWarn)
	}
	if cfg.Signals.MinBaselineDays != 5 {
		t.Errorf("MinBaselineDays = %d, want 5", cfg.Signals.MinBaselineDays)
	}
}

func TestValidate_SlackTokenEnvMissing(t *testing.T) {
	yaml := minimalYAML + `
slack:
  bot_token_env: "SENTINEL_TEST_SLACK_TOKEN_NONEXISTENT_XYZ"
`
	path := writeTemp(t, yaml)
	// Env var not set → Validate returns error
	os.Unsetenv("SENTINEL_TEST_SLACK_TOKEN_NONEXISTENT_XYZ")
	_, err := Load(path)
	if err == nil {
		t.Error("expected error when bot_token_env is set but env var missing, got nil")
	}
}

func TestValidate_SlackTokenEnvPresent(t *testing.T) {
	yaml := minimalYAML + `
slack:
  bot_token_env: "SENTINEL_TEST_SLACK_TOKEN_NONEXISTENT_XYZ"
`
	path := writeTemp(t, yaml)
	t.Setenv("SENTINEL_TEST_SLACK_TOKEN_NONEXISTENT_XYZ", "xoxb-test")
	_, err := Load(path)
	if err != nil {
		t.Errorf("expected no error when env var is set, got %v", err)
	}
}
