package git

import (
	"strings"
	"testing"
)

func TestCommitMessageAndLog(t *testing.T) {
	tr := Track{Dir: t.TempDir(), File: "doc.json"}

	first, changed, err := tr.CommitMessage("x", "{}\n", "Jane Doe <jane@example.com>", "first\n\n- one")
	if err != nil || !changed || first == "" {
		t.Fatalf("first commit: %q %v %v", first, changed, err)
	}
	// The same content records nothing and answers the last commit.
	same, changed, err := tr.CommitMessage("x", "{}\n", "Jane Doe <jane@example.com>", "again")
	if err != nil || changed || same != first {
		t.Fatalf("unchanged commit: %q %v %v, want %q false", same, changed, err, first)
	}
	second, changed, err := tr.CommitMessage("x", "{\"a\": 1}\n", "", "second")
	if err != nil || !changed || second == first {
		t.Fatalf("second commit: %q %v %v", second, changed, err)
	}

	log, err := tr.Log("x", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(log) != 2 {
		t.Fatalf("got %d commits, want 2: %+v", len(log), log)
	}
	if log[0].ID != second || log[0].Subject != "second" {
		t.Errorf("newest: %+v", log[0])
	}
	if log[1].Subject != "first" || log[1].Body != "- one" || log[1].Author != "Jane Doe <jane@example.com>" {
		t.Errorf("oldest: %+v", log[1])
	}
	if empty, err := (Track{Dir: t.TempDir(), File: "doc"}).Log("none", 10); err != nil || len(empty) != 0 {
		t.Errorf("no repository: %v %v", empty, err)
	}
}

func TestFileAtParentDiff(t *testing.T) {
	tr := Track{Dir: t.TempDir(), File: "doc"}
	first, _, err := tr.CommitMessage("x", "a\\n", "", "one")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := tr.CommitMessage("x", "b\\n", "", "two")
	if err != nil {
		t.Fatal(err)
	}
	if full, ok := tr.ResolveCommit("x", second[:7]); !ok || full != second {
		t.Errorf("resolve: %q %v", full, ok)
	}
	if _, ok := tr.ResolveCommit("x", "--all"); ok {
		t.Error("an option resolved as a commit")
	}
	if parent, ok := tr.Parent("x", second); !ok || parent != first {
		t.Errorf("parent: %q %v", parent, ok)
	}
	if _, ok := tr.Parent("x", first); ok {
		t.Error("the first commit has a parent")
	}
	if content, err := tr.FileAt("x", first); err != nil || content != "a\\n" {
		t.Errorf("file at first: %q %v", content, err)
	}
	if diff, err := tr.DiffFile("x", first, second); err != nil || !strings.Contains(diff, "-a") || !strings.Contains(diff, "+b") {
		t.Errorf("diff: %q %v", diff, err)
	}
	if diff, err := tr.DiffFile("x", "", first); err != nil || !strings.Contains(diff, "+a") {
		t.Errorf("diff of the first commit: %q %v", diff, err)
	}
	if log, err := tr.Log("x", 1, first); err != nil || len(log) != 1 || log[0].ID != first {
		t.Errorf("log from first: %+v %v", log, err)
	}
}
