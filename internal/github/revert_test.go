package github

import (
	"testing"
	"time"

	"enterprise-llm-tracker/internal/store"
)

func ptr[T any](v T) *T { return &v }

var (
	t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(24 * time.Hour)
	t2 = t1.Add(24 * time.Hour)
)

func TestDetectReverts_Title(t *testing.T) {
	prs := []store.GitHubPR{
		{PRNumber: 1, Repo: "org/repo", Title: "Add login feature", State: "MERGED", MergedAt: ptr(t0)},
		{PRNumber: 2, Repo: "org/repo", Title: `Revert "Add login feature"`, State: "MERGED", MergedAt: ptr(t1)},
	}
	findings := DetectReverts(prs)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	f := findings[0]
	if f.OriginalPR != 1 {
		t.Errorf("OriginalPR = %d, want 1", f.OriginalPR)
	}
	if f.RevertingPR != 2 {
		t.Errorf("RevertingPR = %d, want 2", f.RevertingPR)
	}
	if f.Heuristic != "title" {
		t.Errorf("Heuristic = %q, want %q", f.Heuristic, "title")
	}
}

func TestDetectReverts_TitleColon(t *testing.T) {
	// "Revert: ..." variant
	prs := []store.GitHubPR{
		{PRNumber: 1, Repo: "org/repo", Title: "Add feature", State: "MERGED", MergedAt: ptr(t0)},
		{PRNumber: 2, Repo: "org/repo", Title: "Revert: Add feature", State: "MERGED", MergedAt: ptr(t1)},
	}
	findings := DetectReverts(prs)
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].OriginalPR != 1 {
		t.Errorf("OriginalPR = %d, want 1", findings[0].OriginalPR)
	}
}

func TestDetectReverts_NoReverts(t *testing.T) {
	prs := []store.GitHubPR{
		{PRNumber: 1, Repo: "org/repo", Title: "feat: add auth", State: "MERGED", MergedAt: ptr(t0)},
		{PRNumber: 2, Repo: "org/repo", Title: "fix: null pointer", State: "MERGED", MergedAt: ptr(t1)},
		{PRNumber: 3, Repo: "org/repo", Title: "chore: bump deps", State: "MERGED", MergedAt: ptr(t2)},
	}
	findings := DetectReverts(prs)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings, got %d: %+v", len(findings), findings)
	}
}

func TestDetectReverts_FileOverlap(t *testing.T) {
	// suspect touches 3/4 of the same files with fewer total changes → undo signature
	files := []string{"a.go", "b.go", "c.go", "d.go"}
	prs := []store.GitHubPR{
		{
			PRNumber:     1,
			Repo:         "org/repo",
			Title:        "refactor: big change",
			State:        "MERGED",
			MergedAt:     ptr(t0),
			FilesChanged: 4,
			Files:        files,
		},
		{
			// Touches 3 of the 4 same files, and has fewer changes → undo
			PRNumber:     2,
			Repo:         "org/repo",
			Title:        "fix: partial rollback",
			State:        "MERGED",
			MergedAt:     ptr(t1),
			FilesChanged: 2,
			Files:        []string{"a.go", "b.go", "c.go"},
		},
	}
	findings := DetectReverts(prs)
	// Jaccard({a,b,c,d},{a,b,c}) = 3/4 = 0.75 ≥ 0.6, and 2*2 <= 3*4 (4 <= 12) passes undo check
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].OriginalPR != 1 {
		t.Errorf("OriginalPR = %d, want 1", findings[0].OriginalPR)
	}
	if findings[0].Heuristic != "file_overlap" {
		t.Errorf("Heuristic = %q, want file_overlap", findings[0].Heuristic)
	}
}

func TestDetectReverts_FileOverlapBelowThreshold(t *testing.T) {
	// Low overlap (2/6 files) — should not fire
	prs := []store.GitHubPR{
		{
			PRNumber:     1,
			Repo:         "org/repo",
			Title:        "refactor: big change",
			State:        "MERGED",
			MergedAt:     ptr(t0),
			FilesChanged: 6,
			Files:        []string{"a.go", "b.go", "c.go", "d.go", "e.go", "f.go"},
		},
		{
			PRNumber:     2,
			Repo:         "org/repo",
			Title:        "fix: unrelated",
			State:        "MERGED",
			MergedAt:     ptr(t1),
			FilesChanged: 2,
			Files:        []string{"a.go", "g.go"},
		},
	}
	findings := DetectReverts(prs)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings, got %d", len(findings))
	}
}

func TestDetectReverts_CrossRepoBoundary(t *testing.T) {
	// Revert title matches but different repo — should not fire
	prs := []store.GitHubPR{
		{PRNumber: 1, Repo: "org/repo-a", Title: "Add feature", State: "MERGED", MergedAt: ptr(t0)},
		{PRNumber: 2, Repo: "org/repo-b", Title: `Revert "Add feature"`, State: "MERGED", MergedAt: ptr(t1)},
	}
	findings := DetectReverts(prs)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings across repos, got %d", len(findings))
	}
}

func TestDetectReverts_Dedupe(t *testing.T) {
	// Both title and file-overlap would flag the same original PR — should count as one finding
	files := []string{"a.go", "b.go", "c.go", "d.go"}
	prs := []store.GitHubPR{
		{
			PRNumber:     1,
			Repo:         "org/repo",
			Title:        "Add feature",
			State:        "MERGED",
			MergedAt:     ptr(t0),
			FilesChanged: 4,
			Files:        files,
		},
		{
			PRNumber:     2,
			Repo:         "org/repo",
			Title:        `Revert "Add feature"`,
			State:        "MERGED",
			MergedAt:     ptr(t1),
			FilesChanged: 2,
			Files:        []string{"a.go", "b.go", "c.go"},
		},
	}
	findings := DetectReverts(prs)
	if len(findings) != 1 {
		t.Errorf("expected 1 deduplicated finding, got %d: %+v", len(findings), findings)
	}
}

func TestDetectReverts_FileOverlapWindowExceeded(t *testing.T) {
	// Suspect merged >14 days after original — outside the revert window
	old := t0
	recent := t0.Add(20 * 24 * time.Hour)
	prs := []store.GitHubPR{
		{
			PRNumber:     1,
			Repo:         "org/repo",
			Title:        "big change",
			State:        "MERGED",
			MergedAt:     ptr(old),
			FilesChanged: 4,
			Files:        []string{"a.go", "b.go", "c.go", "d.go"},
		},
		{
			PRNumber:     2,
			Repo:         "org/repo",
			Title:        "old rollback",
			State:        "MERGED",
			MergedAt:     ptr(recent),
			FilesChanged: 2,
			Files:        []string{"a.go", "b.go", "c.go"},
		},
	}
	findings := DetectReverts(prs)
	if len(findings) != 0 {
		t.Errorf("expected 0 findings (too old), got %d", len(findings))
	}
}

func TestJaccard(t *testing.T) {
	cases := []struct {
		a, b []string
		want float64
	}{
		{[]string{"a", "b"}, []string{"a", "b"}, 1.0},
		{[]string{"a", "b"}, []string{"c", "d"}, 0.0},
		{[]string{"a", "b", "c"}, []string{"a", "b"}, 2.0 / 3.0},
		{nil, []string{"a"}, 0.0},
		{[]string{"a"}, nil, 0.0},
	}
	for _, c := range cases {
		got := jaccard(toSet(c.a), toSet(c.b))
		if abs(got-c.want) > 1e-9 {
			t.Errorf("jaccard(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
