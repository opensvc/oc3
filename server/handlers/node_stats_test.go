package serverhandlers

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"

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
