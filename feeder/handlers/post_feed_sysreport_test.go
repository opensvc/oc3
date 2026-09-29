package feederhandlers

import (
	"archive/tar"
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testTar(t *testing.T, entries map[string]string, symlinks map[string]string) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for name, content := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range symlinks {
		if err := tw.WriteHeader(&tar.Header{Name: name, Linkname: target, Typeflag: tar.TypeSymlink}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &buf
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %s", p, err)
	}
	return string(b)
}

// A path climbing out of the base with ".." stops at the base.
func TestConfine(t *testing.T) {
	for rel, want := range map[string]string{
		"file/etc/hosts":           "/base/file/etc/hosts",
		"/etc/hosts":               "/base/etc/hosts",
		"../../etc/passwd":         "/base/etc/passwd",
		"file/../../../etc/shadow": "/base/etc/shadow",
	} {
		if got := confine("/base", rel); got != want {
			t.Errorf("%s: got %s, want %s", rel, got, want)
		}
	}
}

// The members of the archive land in the node tree, the node name replaced
// by it, a member climbing with ".." included, and a symlink is not written.
func TestSysreportExtract(t *testing.T) {
	root := t.TempDir()
	nodeDir := filepath.Join(root, "sysreport", "node1")
	archive := testTar(t, map[string]string{
		"dev2n1/file/etc/hosts":         "hosts",
		"dev2n1/cmd/uname":              "Linux",
		"dev2n1/../../../escaped":       "escaped",
		"dev2n1/file/../../../../outer": "outer",
	}, map[string]string{
		"dev2n1/file/etc/link": "/etc/shadow",
	})
	written, err := sysreportExtractReader(archive, nodeDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, filepath.Join(nodeDir, "file/etc/hosts")); got != "hosts" {
		t.Errorf("hosts: %q", got)
	}
	if got := readFile(t, filepath.Join(nodeDir, "cmd/uname")); got != "Linux" {
		t.Errorf("uname: %q", got)
	}
	for _, p := range []string{filepath.Join(root, "escaped"), filepath.Join(root, "outer"), filepath.Join(filepath.Dir(root), "outer")} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("%s written out of the node tree", p)
		}
	}
	if _, err := os.Lstat(filepath.Join(nodeDir, "file/etc/link")); err == nil {
		t.Error("a symlink member is written")
	}
	for p := range written {
		if !strings.HasPrefix(p, nodeDir+"/") {
			t.Errorf("%s written out of the node tree", p)
		}
	}
}

// The deleted files are removed from the file tree of the node, and a path
// climbing with ".." does not reach out of it.
func TestSysreportDelete(t *testing.T) {
	root := t.TempDir()
	nodeDir := filepath.Join(root, "sysreport", "node1")
	victim := filepath.Join(root, "victim")
	if err := os.WriteFile(victim, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := sysreportExtractReader(testTar(t, map[string]string{"n/file/etc/motd": "hi"}, nil), nodeDir); err != nil {
		t.Fatal(err)
	}
	log := slog.Default()
	if !sysreportDelete(log, []string{"/etc/motd", "../../../victim"}, nodeDir) {
		t.Error("deletions not reported")
	}
	if _, err := os.Stat(filepath.Join(nodeDir, "file/etc/motd")); err == nil {
		t.Error("deleted file still there")
	}
	if _, err := os.Stat(victim); err != nil {
		t.Error("a file out of the node tree was deleted")
	}
	if sysreportDelete(log, nil, nodeDir) {
		t.Error("no deletion reported as one")
	}
}

// A full report removes the files the node no longer holds, and leaves the
// git history alone.
func TestSysreportRemoveOthers(t *testing.T) {
	nodeDir := filepath.Join(t.TempDir(), "sysreport", "node1")
	if _, err := sysreportExtractReader(testTar(t, map[string]string{
		"n/file/etc/old": "old",
		"n/cmd/oldcmd":   "old",
	}, nil), nodeDir); err != nil {
		t.Fatal(err)
	}
	gitFile := filepath.Join(nodeDir, ".git", "HEAD")
	if err := os.MkdirAll(filepath.Dir(gitFile), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gitFile, []byte("ref"), 0644); err != nil {
		t.Fatal(err)
	}
	written, err := sysreportExtractReader(testTar(t, map[string]string{
		"n/file/etc/new": "new",
		"n/cmd/newcmd":   "new",
	}, nil), nodeDir)
	if err != nil {
		t.Fatal(err)
	}
	if n := sysreportRemoveOthers(slog.Default(), nodeDir, written); n != 2 {
		t.Errorf("removed %d files, want 2", n)
	}
	for _, p := range []string{"file/etc/old", "cmd/oldcmd"} {
		if _, err := os.Stat(filepath.Join(nodeDir, p)); err == nil {
			t.Errorf("%s not removed", p)
		}
	}
	for _, p := range []string{"file/etc/new", "cmd/newcmd", ".git/HEAD"} {
		if _, err := os.Stat(filepath.Join(nodeDir, p)); err != nil {
			t.Errorf("%s removed", p)
		}
	}
}
