package git

import "os/exec"

// command returns the git command running args. Its automatic housekeeping, the
// "git gc --auto" a commit may start, runs in the foreground: detached, gc would
// outlive the git process oc3 waits for, and be left to PID 1 to reap; in a
// container where oc3 is PID 1, as a zombie.
func command(args ...string) *exec.Cmd {
	return exec.Command("git", append([]string{"-c", "gc.autoDetach=false"}, args...)...)
}
