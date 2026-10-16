package serverhandlers

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/timeseries"
)

func TestNodeStatsSeries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "netdev", "eth0")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	now := int(time.Now().Unix()) / 600 * 600
	for _, metric := range []string{"txkBps", "rxkBps"} {
		for i := 0; i < 3; i++ {
			if err := timeseries.Update(filepath.Join(dir, metric+".wsp"), float64(i), now-(2-i)*600, timeseries.DefaultRetentions, whisper.Average, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	series := nodeStatsSeries(dir, "eth0", now-3600, now+60)
	if len(series) != 2 || series[0].Metric != "rxkBps" || series[1].Metric != "txkBps" {
		t.Fatalf("unexpected series: %+v", series)
	}
	if series[0].Device == nil || *series[0].Device != "eth0" || len(series[0].Points) != 3 {
		t.Errorf("unexpected series: %+v", series[0])
	}
	if got := nodeStatsSeries(filepath.Join(dir, "missing"), "", 0, now); len(got) != 0 {
		t.Errorf("a missing directory gives no series, got %+v", got)
	}
}

func TestNodeStatsMountSeries(t *testing.T) {
	base := filepath.Join(t.TempDir(), "fs_u")
	now := int(time.Now().Unix()) / 600 * 600
	for _, dir := range []string{base, filepath.Join(base, "var", "log")} {
		if err := timeseries.Update(filepath.Join(dir, "used.wsp"), 42, now, timeseries.DefaultRetentions, whisper.Average, 0); err != nil {
			t.Fatal(err)
		}
	}
	series := nodeStatsMountSeries(base, now-3600, now+60)
	if len(series) != 2 {
		t.Fatalf("unexpected series: %+v", series)
	}
	if *series[0].Device != "/" || *series[1].Device != "/var/log" || series[1].Metric != "used" {
		t.Errorf("unexpected devices: %s, %s", *series[0].Device, *series[1].Device)
	}
}

func TestNodeStatsRange(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	at := func(t time.Time) *time.Time { return &t }
	days := func(n int) *int { return &n }
	cases := []struct {
		name     string
		params   server.GetNodeStatsParams
		from, to time.Time
		fails    bool
	}{
		{name: "default", from: now.Add(-day), to: now},
		{name: "days", params: server.GetNodeStatsParams{Days: days(365)}, from: now.Add(-365 * day), to: now},
		{name: "days capped", params: server.GetNodeStatsParams{Days: days(5000)}, from: now.Add(-nodeStatsMaxDays * day), to: now},
		{name: "end and days", params: server.GetNodeStatsParams{Days: days(7), End: at(now.Add(-day))}, from: now.Add(-8 * day), to: now.Add(-day)},
		{name: "begin alone", params: server.GetNodeStatsParams{Begin: at(now.Add(-3 * day))}, from: now.Add(-3 * day), to: now},
		{name: "begin and end", params: server.GetNodeStatsParams{Begin: at(now.Add(-3 * day)), End: at(now.Add(-2 * day))}, from: now.Add(-3 * day), to: now.Add(-2 * day)},
		{name: "reversed", params: server.GetNodeStatsParams{Begin: at(now), End: at(now.Add(-day))}, fails: true},
		{name: "too long", params: server.GetNodeStatsParams{Begin: at(now.Add(-2000 * day))}, fails: true},
	}
	for _, tc := range cases {
		from, to, err := nodeStatsRange(tc.params, now)
		if tc.fails {
			if err == nil {
				t.Errorf("%s: expected an error", tc.name)
			}
			continue
		}
		if err != nil || from != int(tc.from.Unix()) || to != int(tc.to.Unix()) {
			t.Errorf("%s: got %d..%d (%v), want %d..%d", tc.name, from, to, err, tc.from.Unix(), tc.to.Unix())
		}
	}
}
