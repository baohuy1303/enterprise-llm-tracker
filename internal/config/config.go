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
	Org   string   `yaml:"org"`
	Repos []string `yaml:"repos"`
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
	return nil
}
