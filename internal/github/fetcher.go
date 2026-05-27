package github

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/shurcooL/githubv4"

	"enterprise-llm-tracker/internal/store"
)

// rateLimit is embedded in every query so we can update the client's
// remaining/resetAt after each call.
type rateLimit struct {
	Remaining int
	ResetAt   time.Time
}

// extractRateLimit pulls the RateLimit field out of an arbitrary query struct
// via reflection. Each query type names the field "RateLimit" with this same
// shape; if it's missing we just skip the update (no harm).
func extractRateLimit(q any) (rateLimit, bool) {
	v := reflect.ValueOf(q)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return rateLimit{}, false
	}
	f := v.FieldByName("RateLimit")
	if !f.IsValid() {
		return rateLimit{}, false
	}
	rl, ok := f.Interface().(rateLimit)
	return rl, ok
}

// prSearchQuery walks PRs authored by an engineer across the configured org.
// We use search() because it supports cross-repo author + date filters in a
// single paged stream — cheaper than scanning every repo individually.
type prSearchQuery struct {
	Search struct {
		PageInfo struct {
			HasNextPage bool
			EndCursor   githubv4.String
		}
		Nodes []struct {
			PullRequest struct {
				Number    int
				Title     string
				State     githubv4.PullRequestState
				CreatedAt time.Time
				MergedAt  *time.Time
				Repository struct {
					NameWithOwner string
				}
				Author struct {
					Login string
				}
				Reviews struct {
					TotalCount int
				} `graphql:"reviews(first: 0)"`
				ChangedFiles int
				Files        struct {
					Nodes []struct {
						Path string
					}
				} `graphql:"files(first: 100)"`
			} `graphql:"... on PullRequest"`
		}
	} `graphql:"search(query: $q, type: ISSUE, first: 50, after: $cursor)"`
	RateLimit rateLimit
}

// FetchAuthoredPRs returns PRs authored by `login` in the given org or user
// account, created in the lookback window. Walks pagination until exhausted
// or context cancels. Set isUser=true when `org` is a personal GitHub account
// rather than an organization (uses `user:` instead of `org:` in the search).
func (c *Client) FetchAuthoredPRs(ctx context.Context, org, login string, since time.Time, isUser bool) ([]store.GitHubPR, error) {
	// search syntax: "type:pr author:LOGIN org:ORG created:>=YYYY-MM-DD"
	// or:            "type:pr author:LOGIN user:USER created:>=YYYY-MM-DD"
	scopeKey := "org:"
	if isUser {
		scopeKey = "user:"
	}
	queryStr := strings.Join([]string{
		"type:pr",
		"author:" + login,
		scopeKey + org,
		"created:>=" + since.Format("2006-01-02"),
	}, " ")

	var cursor *githubv4.String
	out := make([]store.GitHubPR, 0, 32)
	now := time.Now().UTC()

	for {
		var q prSearchQuery
		vars := map[string]any{
			"q":      githubv4.String(queryStr),
			"cursor": cursor,
		}
		if err := c.Query(ctx, &q, vars); err != nil {
			return nil, err
		}

		for _, node := range q.Search.Nodes {
			pr := node.PullRequest
			if pr.Number == 0 {
				// non-PR result (search returns ISSUE too); skip
				continue
			}
			files := make([]string, 0, len(pr.Files.Nodes))
			for _, f := range pr.Files.Nodes {
				files = append(files, f.Path)
			}
			out = append(out, store.GitHubPR{
				EngineerGitHub: pr.Author.Login,
				Repo:           pr.Repository.NameWithOwner,
				PRNumber:       pr.Number,
				Title:          pr.Title,
				State:          string(pr.State),
				CreatedAt:      pr.CreatedAt,
				MergedAt:       pr.MergedAt,
				ReviewCount:    pr.Reviews.TotalCount,
				FilesChanged:   pr.ChangedFiles,
				Files:          files,
				LastSyncedAt:   now,
			})
		}

		if !q.Search.PageInfo.HasNextPage {
			break
		}
		next := q.Search.PageInfo.EndCursor
		cursor = &next
	}
	return out, nil
}
