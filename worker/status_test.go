package worker

import "testing"

func TestStatusAdd(t *testing.T) {
	var (
		na     = statusNotApplicable
		up     = statusUp
		down   = statusDown
		warn   = statusWarn
		su     = statusStandbyUp
		sd     = statusStandbyDown
		suWUp  = statusStandbyUpWithUp
		suWDwn = statusStandbyUpWithDown
		undef  = statusUndef
	)
	// merge rules from opensvc v2 core/status MERGE_RULES
	cases := []struct {
		a, b, expected status
	}{
		{up, up, up},
		{up, down, warn},
		{up, warn, warn},
		{up, na, up},
		{up, su, suWUp},
		{up, sd, warn},
		{up, suWUp, suWUp},
		{up, suWDwn, warn},
		{down, down, down},
		{down, warn, warn},
		{down, na, down},
		{down, su, suWDwn},
		{down, sd, sd},
		{down, suWUp, warn},
		{down, suWDwn, suWDwn},
		{warn, warn, warn},
		{warn, na, warn},
		{warn, su, warn},
		{warn, sd, warn},
		{warn, suWUp, warn},
		{warn, suWDwn, warn},
		{na, na, na},
		{na, su, su},
		{na, sd, sd},
		{na, suWUp, suWUp},
		{na, suWDwn, suWDwn},
		{su, su, su},
		{su, sd, warn},
		{su, suWUp, suWUp},
		{su, suWDwn, suWDwn},
		{sd, sd, sd},
		{sd, suWUp, warn},
		{sd, suWDwn, warn},
		{suWUp, suWDwn, warn},
		{suWUp, suWUp, suWUp},
		{suWDwn, suWDwn, suWDwn},

		{undef, undef, undef},
		{undef, na, na},
		{undef, up, up},
		{undef, down, down},
		{undef, warn, warn},
		{undef, su, su},
		{undef, sd, sd},
	}
	for _, c := range cases {
		for _, pair := range [][2]status{{c.a, c.b}, {c.b, c.a}} {
			s := pair[0]
			s.Add(pair[1])
			if s != c.expected {
				t.Errorf("%s(%d) + %s(%d): expected %s(%d), got %s(%d)",
					pair[0], pair[0], pair[1], pair[1], c.expected, c.expected, s, s)
			}
		}
	}
}

func TestParseStatus(t *testing.T) {
	cases := map[string]status{
		"up":         statusUp,
		"down":       statusDown,
		"warn":       statusWarn,
		"n/a":        statusNotApplicable,
		"undef":      statusUndef,
		"stdby up":   statusStandbyUp,
		"stdby down": statusStandbyDown,
		" up ":       statusUp,
		"":           statusUndef,
		"foo":        statusUndef,
	}
	for s, expected := range cases {
		if got := parseStatus(s); got != expected {
			t.Errorf("parseStatus(%q): expected %s, got %s", s, expected, got)
		}
	}
}

func TestStatusString(t *testing.T) {
	cases := map[status]string{
		statusStandbyUpWithUp:   "up",
		statusStandbyUpWithDown: "stdby up",
		statusNotApplicable:     "n/a",
		statusUndef:             "undef",
	}
	for s, expected := range cases {
		if got := s.String(); got != expected {
			t.Errorf("%d.String(): expected %q, got %q", s, expected, got)
		}
	}
}
