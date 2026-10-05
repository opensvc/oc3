package git

import "testing"

func TestSysreportDisplay(t *testing.T) {
	cases := []struct{ path, display, kind string }{
		{"file/etc/resolv.conf", "/etc/resolv.conf", "file"},
		{"file//etc/hosts", "/etc/hosts", "file"},
		{"cmd/ip(space)route(space)show(space)table(space)all", "ip route show table all", "command"},
		{"cmd/ls(space)(slash)etc(pipe)wc(space)-l", "ls /etc|wc -l", "command"},
	}
	for _, c := range cases {
		display, kind := SysreportDisplay(c.path)
		if display != c.display || kind != c.kind {
			t.Errorf("SysreportDisplay(%q) = %q, %q; want %q, %q", c.path, display, kind, c.display, c.kind)
		}
	}
}

func TestParsePatch(t *testing.T) {
	patch := `diff --git a/file/etc/resolv.conf b/file/etc/resolv.conf
index 1111111..2222222 100644
--- a/file/etc/resolv.conf
+++ b/file/etc/resolv.conf
@@ -1,2 +1,2 @@
 nameserver 10.0.0.1
-search old.example.com
+search new.example.com
diff --git a/cmd/ip(space)route b/cmd/ip(space)route
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/cmd/ip(space)route
@@ -0,0 +1 @@
+default via 10.0.0.254
diff --git a/file/etc/gone b/file/etc/gone
deleted file mode 100644
index 4444444..0000000
--- a/file/etc/gone
+++ /dev/null
@@ -1 +0,0 @@
-bye
`
	diffs := parsePatch(patch)
	if len(diffs) != 3 {
		t.Fatalf("got %d diffs, want 3", len(diffs))
	}
	want := []struct {
		path           string
		added, deleted int
	}{
		{"file/etc/resolv.conf", 1, 1},
		{"cmd/ip(space)route", 1, 0},
		{"file/etc/gone", 0, 1},
	}
	for i, w := range want {
		d := diffs[i]
		if d.Path != w.path || d.Added != w.added || d.Deleted != w.deleted {
			t.Errorf("diff %d = %q +%d -%d; want %q +%d -%d", i, d.Path, d.Added, d.Deleted, w.path, w.added, w.deleted)
		}
		if len(d.Diff) == 0 || d.Diff[:2] != "@@" {
			t.Errorf("diff %d does not start at its first hunk: %q", i, d.Diff)
		}
	}
}

func TestParseNumstat(t *testing.T) {
	if s, ok := parseNumstat("3\t1\tfile/etc/hosts"); !ok || s.Added != 3 || s.Deleted != 1 || s.Path != "file/etc/hosts" {
		t.Errorf("text numstat = %+v, %v", s, ok)
	}
	if s, ok := parseNumstat("-\t-\tfile/bin/blob"); !ok || !s.Binary {
		t.Errorf("binary numstat = %+v, %v", s, ok)
	}
	if _, ok := parseNumstat(""); ok {
		t.Errorf("empty line parsed as a stat")
	}
}
