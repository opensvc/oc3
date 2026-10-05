package availability

import (
	"math"
	"testing"
	"time"

	"github.com/opensvc/oc3/cdb"
)

func TestServiceAvailability(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.Local)
	f := func(d time.Duration) string { return now.Add(d).Format("2006-01-02 15:04:05") }
	periods := []cdb.StatusPeriod{
		{Avail: "up", Begin: f(-10 * time.Hour), End: f(-6 * time.Hour)},
		{Avail: "down", Begin: f(-6 * time.Hour), End: f(-4 * time.Hour)},
		// Two hours of no status, then the current period, refreshed a minute ago.
		{Avail: "up", Begin: f(-2 * time.Hour), End: f(-time.Minute)},
	}
	// The history starts 10 hours ago, within the 1 day asked: 8 of 10 hours up.
	r := Compute(periods, nil, 1, now)
	if r.From != now.Add(-10*time.Hour) || r.Available != 6*time.Hour || r.Counted != 10*time.Hour {
		t.Fatalf("no ack: %+v", r)
	}
	if math.Abs(r.Rate()-60) > 0.001 {
		t.Errorf("rate %f", r.Rate())
	}

	// The down period justified and not accounted: 6 of 8 counted hours up.
	acks := []cdb.StatusAck{{Begin: f(-6 * time.Hour), End: f(-4 * time.Hour), Account: false}}
	r = Compute(periods, acks, 1, now)
	if r.Excluded != 2*time.Hour || r.Counted != 8*time.Hour || math.Abs(r.Rate()-75) > 0.001 {
		t.Errorf("not accounted: %+v rate %f", r, r.Rate())
	}

	// Justified but still accounted: no change.
	acks[0].Account = true
	if r = Compute(periods, acks, 1, now); r.Excluded != 0 {
		t.Errorf("accounted: %+v", r)
	}

	// A justification overlapping an up period excludes only the downtime.
	acks = []cdb.StatusAck{{Begin: f(-7 * time.Hour), End: f(-5 * time.Hour), Account: false}}
	if r = Compute(periods, acks, 1, now); r.Excluded != time.Hour {
		t.Errorf("overlap: %+v", r)
	}

	// A stale last status leaves the time since as downtime.
	stale := []cdb.StatusPeriod{{Avail: "up", Begin: f(-4 * time.Hour), End: f(-2 * time.Hour)}}
	if r = Compute(stale, nil, 1, now); r.Available != 2*time.Hour || r.Counted != 4*time.Hour {
		t.Errorf("stale: %+v", r)
	}
}
