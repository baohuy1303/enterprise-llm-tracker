// Package github wraps the GitHub GraphQL v4 API for the efficiency collector.
//
// Two responsibilities:
//  1. Authenticated client construction (PAT bearer via oauth2)
//  2. Rate-limit awareness — GraphQL queries return a `rateLimit { remaining,
//     resetAt }` block that we surface so the collector can back off before
//     burning through the 5000 points/hour budget.
package github

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/shurcooL/githubv4"
	"golang.org/x/oauth2"
)

// Client wraps githubv4.Client with rate-limit tracking and structured logging.
type Client struct {
	gql    *githubv4.Client
	logger *slog.Logger

	mu        sync.Mutex
	remaining int
	resetAt   time.Time
}

// New returns a client bound to a personal access token. The token must have
// `repo` scope (or `public_repo` for public-only repos).
func New(token string, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	src := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	httpClient := oauth2.NewClient(context.Background(), src)
	return &Client{
		gql:       githubv4.NewClient(httpClient),
		logger:    logger,
		remaining: 5000, // assume fresh budget until first query updates it
	}
}

// RateLimit returns a snapshot of the last-observed rate-limit state.
func (c *Client) RateLimit() (remaining int, resetAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.remaining, c.resetAt
}

// updateRateLimit is called by query wrappers after each successful response.
func (c *Client) updateRateLimit(remaining int, resetAt time.Time) {
	c.mu.Lock()
	c.remaining = remaining
	c.resetAt = resetAt
	c.mu.Unlock()
}

// waitIfThrottled blocks until the rate limit resets if we're below a safety
// floor. The floor (200 points) leaves headroom for parallel callers.
func (c *Client) waitIfThrottled(ctx context.Context) error {
	const floor = 200
	c.mu.Lock()
	remaining := c.remaining
	resetAt := c.resetAt
	c.mu.Unlock()

	if remaining > floor || resetAt.IsZero() {
		return nil
	}
	wait := time.Until(resetAt)
	if wait <= 0 {
		return nil
	}
	c.logger.Warn("github rate limit floor hit — sleeping until reset",
		slog.Int("remaining", remaining),
		slog.Duration("wait", wait))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait + time.Second):
		return nil
	}
}

// Query runs a GraphQL query and updates rate-limit state from the response.
// All queries should embed a `rateLimit` field so we can keep our counter
// fresh — see fetcher.go for examples.
func (c *Client) Query(ctx context.Context, q any, vars map[string]any) error {
	if err := c.waitIfThrottled(ctx); err != nil {
		return err
	}
	if err := c.gql.Query(ctx, q, vars); err != nil {
		return fmt.Errorf("graphql query: %w", err)
	}
	if rl, ok := extractRateLimit(q); ok {
		c.updateRateLimit(rl.Remaining, rl.ResetAt)
	}
	return nil
}
