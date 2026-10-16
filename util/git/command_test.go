package git

import (
	"slices"
	"testing"
)

// TestCommandKeepsGcInTheForeground checks that every git command oc3 runs keeps
// its automatic gc attached, before the command's own arguments.
func TestCommandKeepsGcInTheForeground(t *testing.T) {
	cmd := command("--git-dir=/x/.git", "commit", "-m", "m")
	want := []string{"git", "-c", "gc.autoDetach=false", "--git-dir=/x/.git", "commit", "-m", "m"}
	if !slices.Equal(cmd.Args, want) {
		t.Errorf("args: got %v, want %v", cmd.Args, want)
	}
}
