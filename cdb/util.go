package cdb

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

func logDuration(s string, begin time.Time) {
	slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", s, time.Since(begin)))
}

func logDurationInfo(s string, begin time.Time) {
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", s, time.Since(begin)))
}

// Todo : to move elsewhere...
// Tz can be either a timezone name (ex: "Europe/Paris") or a UTC offset (ex: "+01:00")
func ParseTimeWithTimezone(dateStr, tzStr string) (time.Time, error) {
	const timeFormat = "2006-01-02 15:04:05"

	// Named Tz
	if loc, err := time.LoadLocation(tzStr); err == nil {
		return time.ParseInLocation(timeFormat, dateStr, loc)
	}

	// Offset Tz
	tzStr = strings.TrimSpace(tzStr)
	if tzStr != "" && (tzStr[0] == '+' || tzStr[0] == '-') {
		parts := strings.Split(tzStr[1:], ":")
		if len(parts) >= 2 {
			hours, errH := strconv.Atoi(parts[0])
			minutes, errM := strconv.Atoi(parts[1])
			if errH == nil && errM == nil {
				offset := hours*3600 + minutes*60
				if tzStr[0] == '-' {
					offset = -offset
				}
				loc := time.FixedZone(tzStr, offset)
				return time.ParseInLocation(timeFormat, dateStr, loc)
			}
		}
	}

	// Fallback
	return time.Parse(timeFormat, dateStr)
}
