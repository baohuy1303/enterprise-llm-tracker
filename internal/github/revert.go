package github

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"enterprise-llm-tracker/internal/store"
)

// revertTitleRe matches GitHub's default revert PR title format and common
// hand-written variants: `Revert "Original title"` or `Revert: ...`.
var revertTitleRe = regexp.MustCompile(`(?i)^revert\b[ :"']`)

// fileOverlapThreshold is the minimum Jaccard similarity between two PRs'
// file lists for the newer PR to be considered "touching the same code" as
// the older one. 0.6 keeps false positives down — small refactors and pure
// renames won't trigger, but a genuine "undo this PR" usually will.
const fileOverlapThreshold = 0.6

// fileOverlapWindow bounds how far back we look for an older PR that the
// suspected revert is undoing. Reverts later than 14 days post-merge are rare
// and not worth the false-positive risk.
const fileOverlapWindow = 14 * 24 * time.Hour

// RevertFinding pairs the original PR (to be marked reverted) with the PR
// that revealed the revert and a timestamp for when the revert merged.
type RevertFinding struct {
	OriginalRepo   string
	OriginalPR     int
	RevertedAt     time.Time
	RevertingPR    int
	Heuristic      string // "title" | "file_overlap"
}

// DetectReverts scans `prs` (a recent window for one repo) and returns the
// originals that should be marked reverted. Two heuristics:
//
//   - Title-based: PR title starts with "Revert " → look for the most recent
//     prior merged PR whose title is mentioned in the revert title.
//   - File-overlap: each merged PR is compared against PRs merged in the
//     preceding 14 days; if Jaccard(files) ≥ 0.6 AND the newer PR has fewer
//     than half the original's files changed (suggesting an undo, not an
//     extension), the original is flagged reverted.
//
// The two heuristics may overlap. We dedupe by (repo, original_pr).
func DetectReverts(prs []store.GitHubPR) []RevertFinding {
	seen := map[string]RevertFinding{}
	key := func(r string, n int) string { return r + "#" + strconv.Itoa(n) }

	for _, candidate := range prs {
		if f, ok := detectFromTitle(candidate, prs); ok {
			k := key(f.OriginalRepo, f.OriginalPR)
			if _, exists := seen[k]; !exists {
				seen[k] = f
			}
		}
	}

	for _, candidate := range prs {
		if f, ok := detectFromFileOverlap(candidate, prs); ok {
			k := key(f.OriginalRepo, f.OriginalPR)
			if _, exists := seen[k]; !exists {
				seen[k] = f
			}
		}
	}

	out := make([]RevertFinding, 0, len(seen))
	for _, f := range seen {
		out = append(out, f)
	}
	return out
}

// detectFromTitle looks for "Revert ..." PRs and matches them to a recent
// prior PR whose title appears quoted/inline in the revert title.
func detectFromTitle(revert store.GitHubPR, all []store.GitHubPR) (RevertFinding, bool) {
	if revert.MergedAt == nil {
		return RevertFinding{}, false
	}
	if !revertTitleRe.MatchString(revert.Title) {
		return RevertFinding{}, false
	}
	// Pull the quoted-or-trailing portion: `Revert "Foo bar"` → `Foo bar`
	stripped := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(revert.Title, "Revert"), ":"))
	stripped = strings.Trim(stripped, `"' `)
	if stripped == "" {
		return RevertFinding{}, false
	}

	var best store.GitHubPR
	var bestTime time.Time
	for _, prior := range all {
		if prior.PRNumber == revert.PRNumber || prior.Repo != revert.Repo {
			continue
		}
		if prior.MergedAt == nil || !prior.MergedAt.Before(*revert.MergedAt) {
			continue
		}
		// Title containment in either direction — handles "Revert \"X\"" → X
		// as well as the rare "Revert: short summary" → "short summary..." case.
		if strings.Contains(prior.Title, stripped) || strings.Contains(stripped, prior.Title) {
			if prior.MergedAt.After(bestTime) {
				best = prior
				bestTime = *prior.MergedAt
			}
		}
	}
	if best.PRNumber == 0 {
		return RevertFinding{}, false
	}
	return RevertFinding{
		OriginalRepo: best.Repo,
		OriginalPR:   best.PRNumber,
		RevertedAt:   *revert.MergedAt,
		RevertingPR:  revert.PRNumber,
		Heuristic:    "title",
	}, true
}

// detectFromFileOverlap flags an older PR as reverted when a newer merged PR
// in the same repo touches a near-identical set of files but with materially
// fewer changes (a typical undo signature).
func detectFromFileOverlap(suspect store.GitHubPR, all []store.GitHubPR) (RevertFinding, bool) {
	if suspect.MergedAt == nil || len(suspect.Files) == 0 {
		return RevertFinding{}, false
	}
	cutoff := suspect.MergedAt.Add(-fileOverlapWindow)
	suspectSet := toSet(suspect.Files)

	var best store.GitHubPR
	var bestScore float64
	for _, prior := range all {
		if prior.PRNumber == suspect.PRNumber || prior.Repo != suspect.Repo {
			continue
		}
		if prior.MergedAt == nil || prior.MergedAt.Before(cutoff) || !prior.MergedAt.Before(*suspect.MergedAt) {
			continue
		}
		if len(prior.Files) == 0 {
			continue
		}
		// Undo signature: suspect changes substantially fewer files than prior.
		if suspect.FilesChanged*2 > prior.FilesChanged*3 {
			continue
		}
		score := jaccard(suspectSet, toSet(prior.Files))
		if score >= fileOverlapThreshold && score > bestScore {
			best = prior
			bestScore = score
		}
	}
	if best.PRNumber == 0 {
		return RevertFinding{}, false
	}
	return RevertFinding{
		OriginalRepo: best.Repo,
		OriginalPR:   best.PRNumber,
		RevertedAt:   *suspect.MergedAt,
		RevertingPR:  suspect.PRNumber,
		Heuristic:    "file_overlap",
	}, true
}

func toSet(s []string) map[string]struct{} {
	m := make(map[string]struct{}, len(s))
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}

func jaccard(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for k := range a {
		if _, ok := b[k]; ok {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

