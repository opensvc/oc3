package cdb

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseMaxAge parses a duration string accepting the time.ParseDuration
// units plus a "d" day unit, like "15m", "25h", "2d" or "1d12h".
func ParseMaxAge(s string) (time.Duration, error) {
	input := s
	s = strings.TrimSpace(s)
	var days time.Duration
	if i := strings.Index(s, "d"); i >= 0 {
		n, err := strconv.ParseUint(s[:i], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", input)
		}
		days = time.Duration(n) * 24 * time.Hour
		s = s[i+1:]
		if s == "" {
			return days, nil
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", input)
	}
	return days + d, nil
}

// FormatMaxAge formats a duration for human readable messages, using the
// largest of the d, h, m, s units that divides it.
func FormatMaxAge(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	default:
		return d.String()
	}
}

// maxAgeSeconds returns the maxAge duration as a SQL "INTERVAL ? SECOND" argument.
func maxAgeSeconds(maxAge time.Duration) int64 {
	return int64(maxAge / time.Second)
}
