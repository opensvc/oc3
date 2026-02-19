package git

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/viper"
)

func Commit(repoDir string) error {
	gitDir := filepath.Join(repoDir, ".git")

	if _, err := exec.LookPath("git"); err != nil {
		return fmt.Errorf("git not found: %w", err)
	}

	if _, err := os.Stat(repoDir); os.IsNotExist(err) {
		return fmt.Errorf("dir does not exist: %s", repoDir)
	}

	if _, err := os.Stat(gitDir); os.IsNotExist(err) {
		email := viper.GetString("git.user_email")

		cmds := [][]string{
			{"init"},
			{"config", "user.email", email},
			{"config", "user.name", "collector"},
		}
		for _, args := range cmds {
			cmd := exec.Command("git", args...)
			cmd.Dir = repoDir
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("git %v failed: %s: %w", args, out, err)
			}
		}
	}

	_ = os.Remove(filepath.Join(gitDir, "index.lock"))

	cmd := exec.Command("git", "add", ".")
	cmd.Dir = repoDir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git add failed: %s: %w", out, err)
	}

	cmd = exec.Command("git", "commit", "-m", "sysreport", "-a")
	cmd.Dir = repoDir
	_ = cmd.Run()

	return nil
}
