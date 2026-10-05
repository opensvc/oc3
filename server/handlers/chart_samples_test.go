package serverhandlers

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"

	"github.com/opensvc/oc3/timeseries"
)

func TestReadSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "linux.wsp")
	day := 24 * 3600
	now := int(time.Now().Unix()) / day * day
	for i, value := range []float64{10, 12, 15} {
		if err := timeseries.Update(path, value, now-(2-i)*day, timeseries.DailyRetentions, whisper.Last, 0); err != nil {
			t.Fatal(err)
		}
	}
	points, err := readSeries(path, now-10*day, now+day)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 3 || points[0][1] != 10 || points[2][1] != 15 || points[0][0] >= points[2][0] {
		t.Errorf("unexpected points: %v", points)
	}
}
