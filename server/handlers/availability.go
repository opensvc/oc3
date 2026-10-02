package serverhandlers

import (
	"sort"
	"time"

	"github.com/opensvc/oc3/cdb"
)

// availabilityFresh is how recent the last status of a service must be for its
// current period to run to now; an older one means the service stopped
// reporting, which counts as unavailable, as a hole of the historical
// availability did.
const availabilityFresh = 15 * time.Minute

// availableStatuses are the availability statuses during which a service is
// available, the "up" ranges of service_availability(); every other status, and
// the time no status covers, is downtime.
var availableStatuses = map[string]bool{"up": true, "stdby up": true}

type interval struct{ from, to time.Time }

// availabilityResult is the availability of a service over a window: the time it
// was available, the downtime left out of the count by a justification marked
// not to account, and the time counted.
type availabilityResult struct {
	From, To  time.Time
	Available time.Duration
	Excluded  time.Duration
	Counted   time.Duration
}

// Rate is the available share of the counted time, in percent.
func (r availabilityResult) Rate() float64 {
	if r.Counted <= 0 {
		return 100
	}
	return float64(r.Available) * 100 / float64(r.Counted)
}

func parseCollectorTime(s string) (time.Time, bool) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local)
	return t, err == nil
}

// mergeIntervals sorts and joins overlapping intervals.
func mergeIntervals(l []interval) []interval {
	sort.Slice(l, func(i, j int) bool { return l[i].from.Before(l[j].from) })
	var out []interval
	for _, iv := range l {
		if !iv.to.After(iv.from) {
			continue
		}
		if n := len(out); n > 0 && !iv.from.After(out[n-1].to) {
			if iv.to.After(out[n-1].to) {
				out[n-1].to = iv.to
			}
			continue
		}
		out = append(out, iv)
	}
	return out
}

func clip(iv interval, from, to time.Time) interval {
	if iv.from.Before(from) {
		iv.from = from
	}
	if iv.to.After(to) {
		iv.to = to
	}
	return iv
}

func total(l []interval) time.Duration {
	var d time.Duration
	for _, iv := range l {
		d += iv.to.Sub(iv.from)
	}
	return d
}

// subtract returns the parts of a not covered by b, both merged.
func subtract(a, b []interval) []interval {
	var out []interval
	for _, iv := range a {
		cur := []interval{iv}
		for _, cut := range b {
			var next []interval
			for _, c := range cur {
				if !cut.to.After(c.from) || !cut.from.Before(c.to) {
					next = append(next, c)
					continue
				}
				if cut.from.After(c.from) {
					next = append(next, interval{c.from, cut.from})
				}
				if cut.to.Before(c.to) {
					next = append(next, interval{cut.to, c.to})
				}
			}
			cur = next
		}
		out = append(out, cur...)
	}
	return out
}

// serviceAvailability computes the availability of a service from now-days to
// now, from the start of its history when that is later, as
// service_availability() of the historical collector: the downtime is every
// moment the service was neither up nor standby up, the time no status covers
// included, but the parts justified with account off, which are left out of the
// count.
func serviceAvailability(periods []cdb.StatusPeriod, acks []cdb.StatusAck, days int, now time.Time) availabilityResult {
	from := now.Add(-time.Duration(days) * 24 * time.Hour)
	var up []interval
	var first time.Time
	for i, p := range periods {
		b, ok1 := parseCollectorTime(p.Begin)
		e, ok2 := parseCollectorTime(p.End)
		if !ok1 || !ok2 {
			continue
		}
		if first.IsZero() || b.Before(first) {
			first = b
		}
		if i == len(periods)-1 && now.Sub(e) < availabilityFresh {
			e = now
		}
		if availableStatuses[p.Avail] {
			up = append(up, interval{b, e})
		}
	}
	if !first.IsZero() && first.After(from) {
		from = first
	}
	result := availabilityResult{From: from, To: now}
	if !now.After(from) {
		return result
	}
	for i := range up {
		up[i] = clip(up[i], from, now)
	}
	up = mergeIntervals(up)
	var excluded []interval
	for _, a := range acks {
		if a.Account {
			continue
		}
		b, ok1 := parseCollectorTime(a.Begin)
		e, ok2 := parseCollectorTime(a.End)
		if ok1 && ok2 {
			excluded = append(excluded, clip(interval{b, e}, from, now))
		}
	}
	excluded = subtract(mergeIntervals(excluded), up)
	result.Available = total(up)
	result.Excluded = total(excluded)
	result.Counted = now.Sub(from) - result.Excluded
	return result
}
