package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const postMessageURL = "https://slack.com/api/chat.postMessage"

type Client struct {
	token  string
	http   *http.Client
	logger *slog.Logger
}

func New(token string, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		token:  token,
		http:   &http.Client{Timeout: 5 * time.Second},
		logger: logger,
	}
}

// Configured reports whether a bot token was supplied. If not, callers should
// log instead of attempting to post (useful for local dev without a Slack app).
func (c *Client) Configured() bool { return c.token != "" }

type postMessageReq struct {
	Channel string `json:"channel"`
	Text    string `json:"text"`
}

type postMessageResp struct {
	OK    bool   `json:"ok"`
	TS    string `json:"ts"`
	Error string `json:"error,omitempty"`
}

// PostDM sends a message to a Slack user ID. Returns the Slack message
// timestamp ("ts") so callers can write it to the audit log for traceability.
// On API error (non-ok response), returns an error and the empty ts.
func (c *Client) PostDM(ctx context.Context, slackUserID, text string) (string, error) {
	if c.token == "" {
		return "", errors.New("slack: bot token not configured")
	}
	if slackUserID == "" {
		return "", errors.New("slack: empty user id")
	}

	body, err := json.Marshal(postMessageReq{Channel: slackUserID, Text: text})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, postMessageURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("slack request: %w", err)
	}
	defer resp.Body.Close()

	var parsed postMessageResp
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("slack decode response: %w", err)
	}
	if !parsed.OK {
		return "", fmt.Errorf("slack api error: %s", parsed.Error)
	}
	return parsed.TS, nil
}
