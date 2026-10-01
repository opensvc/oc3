package feederhandlers

import (
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-whisper"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/timeseries"
	"github.com/opensvc/oc3/util/logkey"
)

// nodeStatsGroups are the groups of node statistics the agent pushes, with the
// column, for the groups having one, naming the directory a row goes in, as the
// stats_options of the historical feed controller (feed/controllers/default.py)
// declare them.
var nodeStatsGroups = map[string]string{
	"cpu":        "cpu",
	"mem_u":      "",
	"swap":       "",
	"proc":       "",
	"block":      "",
	"blockdev":   "dev",
	"netdev":     "dev",
	"netdev_err": "dev",
	"fs_u":       "mntpt",
	"svc":        "svcname",
}

// nodeStatsIgnored are the columns that are neither the date, the directory nor
// a metric.
var nodeStatsIgnored = map[string]bool{"nodename": true}

// nodeStatsMetricRe is what a metric name, a file name, may be made of.
var nodeStatsMetricRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// nodeStatsDateLayouts are the date formats a row may carry: RFC 3339, then the
// ones of the historical agent, read in the collector time zone as the
// historical collector did.
var nodeStatsDateLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
}

func parseNodeStatsDate(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	for _, layout := range nodeStatsDateLayouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// nodeStatsSubPath turns the value of the directory column into path elements,
// as the historical collector split it on "/": a mount point "/var/log" goes in
// var/log, the root file system in the group directory itself. A value that
// would leave the group directory is refused.
func nodeStatsSubPath(value string) ([]string, bool) {
	var parts []string
	for _, part := range strings.Split(value, "/") {
		switch part {
		case "":
			continue
		case ".", "..":
			return nil, false
		}
		if strings.ContainsAny(part, "\\\x00") {
			return nil, false
		}
		parts = append(parts, part)
	}
	return parts, true
}

// nodeStatsSeries converts the rows of a group into the points of each whisper
// file, by path relative to the node directory, and tells what it skipped.
func nodeStatsSeries(group string, data feeder.NodeStatsGroup) (map[string][]*whisper.TimeSeriesPoint, []string) {
	subCol, known := nodeStatsGroups[group]
	if !known {
		return nil, []string{fmt.Sprintf("%s: unknown group", group)}
	}
	dateIdx, subIdx := -1, -1
	metrics := map[int]string{}
	var skipped []string
	for i, col := range data.Columns {
		switch {
		case col == "date":
			dateIdx = i
		case subCol != "" && col == subCol:
			subIdx = i
		case nodeStatsIgnored[col]:
		case nodeStatsMetricRe.MatchString(col):
			metrics[i] = col
		default:
			skipped = append(skipped, fmt.Sprintf("%s: invalid column %q", group, col))
		}
	}
	if dateIdx < 0 {
		return nil, append(skipped, fmt.Sprintf("%s: no date column", group))
	}
	if subCol != "" && subIdx < 0 {
		return nil, append(skipped, fmt.Sprintf("%s: no %s column", group, subCol))
	}
	series := map[string][]*whisper.TimeSeriesPoint{}
	badDates, badSubs, short := 0, 0, 0
	for _, row := range data.Rows {
		if len(row) != len(data.Columns) {
			short++
			continue
		}
		date, ok := parseNodeStatsDate(row[dateIdx])
		if !ok {
			badDates++
			continue
		}
		dir := []string{group}
		if subIdx >= 0 {
			parts, ok := nodeStatsSubPath(row[subIdx])
			if !ok {
				badSubs++
				continue
			}
			dir = append(dir, parts...)
		}
		for i, metric := range metrics {
			value, err := strconv.ParseFloat(strings.TrimSpace(row[i]), 64)
			if err != nil {
				continue
			}
			path := filepath.Join(append(append([]string{}, dir...), metric+".wsp")...)
			series[path] = append(series[path], &whisper.TimeSeriesPoint{Time: int(date.Unix()), Value: value})
		}
	}
	if short > 0 {
		skipped = append(skipped, fmt.Sprintf("%s: %d rows without a value per column", group, short))
	}
	if badDates > 0 {
		skipped = append(skipped, fmt.Sprintf("%s: %d rows with an invalid date", group, badDates))
	}
	if badSubs > 0 {
		skipped = append(skipped, fmt.Sprintf("%s: %d rows with an invalid %s", group, badSubs, subCol))
	}
	return series, skipped
}

// PostNodeStats handles POST /node/stats: the performance statistics of the
// authenticated node, written at once in its whisper files, as the historical
// collector's insert_stats did, with the same layout and retentions.
func (a *Api) PostNodeStats(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "PostNodeStats")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}
	var payload feeder.PostNodeStatsJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	for group := range payload.Data {
		if _, ok := nodeStatsGroups[group]; !ok {
			return JSONProblemf(c, http.StatusBadRequest, "unknown stats group %q", group)
		}
	}
	uploadDir := viper.GetString("scheduler.directories.uploads")
	if uploadDir == "" {
		log.Error("no uploads directory configured")
		return JSONProblem(c, http.StatusInternalServerError, "the series directory is not configured")
	}
	// The node id comes from the authentication: a UUID, safe as a path element.
	nodeDir := filepath.Join(uploadDir, "stats", "nodes", nodeID)

	groups := make([]string, 0, len(payload.Data))
	for group := range payload.Data {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	result := feeder.NodeStatsStored{}
	var skipped []string
	for _, group := range groups {
		series, groupSkipped := nodeStatsSeries(group, payload.Data[group])
		skipped = append(skipped, groupSkipped...)
		for path, points := range series {
			file := filepath.Join(nodeDir, path)
			if err := timeseries.UpdateMany(file, points, timeseries.DefaultRetentions, whisper.Average, 0.0); err != nil {
				log.Warn("update whisper file", logkey.Error, err, "file", file)
				skipped = append(skipped, fmt.Sprintf("%s: %s", path, err))
				continue
			}
			result.Series++
			result.Points += len(points)
		}
	}
	if len(skipped) > 0 {
		result.Skipped = &skipped
	}
	log.Info(fmt.Sprintf("stored %d points in %d series", result.Points, result.Series))
	return c.JSON(http.StatusOK, result)
}
