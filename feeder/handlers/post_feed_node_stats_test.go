package feederhandlers

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/opensvc/oc3/feeder"
)

func TestNodeStatsSeries(t *testing.T) {
	series, skipped := nodeStatsSeries("cpu", feeder.NodeStatsGroup{
		Columns: []string{"date", "cpu", "usr", "idle", "nodename"},
		Rows: [][]string{
			{"2026-10-01T10:00:00+02:00", "all", "12.5", "80", "n1"},
			{"2026-10-01T10:10:00+02:00", "0", "1", "n/a", "n1"},
			{"bad", "all", "1", "2", "n1"},
			{"2026-10-01T10:20:00+02:00", "all"},
		},
	})
	if got := len(series[filepath.Join("cpu", "all", "usr.wsp")]); got != 1 {
		t.Errorf("cpu/all/usr: %d points", got)
	}
	if got := series[filepath.Join("cpu", "all", "usr.wsp")][0]; got.Value != 12.5 ||
		got.Time != int(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC).Unix()) {
		t.Errorf("cpu/all/usr point: %+v", got)
	}
	if _, ok := series[filepath.Join("cpu", "0", "idle.wsp")]; ok {
		t.Error("a value that is not a number is stored")
	}
	if _, ok := series[filepath.Join("cpu", "all", "nodename.wsp")]; ok {
		t.Error("nodename is stored as a metric")
	}
	if len(skipped) != 2 {
		t.Errorf("skipped: %v", skipped)
	}

	series, _ = nodeStatsSeries("fs_u", feeder.NodeStatsGroup{
		Columns: []string{"date", "nodename", "mntpt", "size", "used"},
		Rows: [][]string{
			{"2026-10-01 10:00:00", "n1", "/", "100", "10"},
			{"2026-10-01 10:00:00", "n1", "/var/log", "50", "5"},
			{"2026-10-01 10:00:00", "n1", "/../etc", "50", "5"},
		},
	})
	for _, path := range []string{"fs_u/size.wsp", "fs_u/var/log/used.wsp"} {
		if _, ok := series[filepath.FromSlash(path)]; !ok {
			t.Errorf("missing %s in %v", path, series)
		}
	}
	if len(series) != 4 {
		t.Errorf("a mount point leaving the group directory is stored: %v", series)
	}

	if _, skipped := nodeStatsSeries("cpu", feeder.NodeStatsGroup{Columns: []string{"date", "usr"}}); len(skipped) != 1 {
		t.Errorf("a cpu group without cpu column: %v", skipped)
	}
	if _, skipped := nodeStatsSeries("cpu", feeder.NodeStatsGroup{Columns: []string{"date", "cpu", "../x"}}); len(skipped) != 1 {
		t.Errorf("an invalid metric name: %v", skipped)
	}
}
