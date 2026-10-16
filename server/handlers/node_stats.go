package serverhandlers

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

const (
	nodeStatsDefaultDays = 1
	nodeStatsMaxDays     = 3 * 365
)

// nodeStatsLayout tells how the whisper files of a group are laid out.
type nodeStatsLayout int

const (
	// nodeStatsFlat: the metrics are in the group directory.
	nodeStatsFlat nodeStatsLayout = iota
	// nodeStatsDevices: one directory per device (network interface, block
	// device, container) holds the metrics.
	nodeStatsDevices
	// nodeStatsMounts: the directories mirror the mount points, "/var/log" in
	// var/log and the root file system in the group directory itself.
	nodeStatsMounts
)

// nodeStatsGroups are the groups of node statistics, as the historical collector
// stored them: the directory of each group under stats/nodes/<node_id> and its
// layout.
var nodeStatsGroups = map[string]struct {
	dir    string
	layout nodeStatsLayout
}{
	"cpu":        {dir: "cpu/all"},
	"mem":        {dir: "mem_u"},
	"swap":       {dir: "swap"},
	"proc":       {dir: "proc"},
	"block":      {dir: "block"},
	"netdev":     {dir: "netdev", layout: nodeStatsDevices},
	"netdev_err": {dir: "netdev_err", layout: nodeStatsDevices},
	"blockdev":   {dir: "blockdev", layout: nodeStatsDevices},
	"fs":         {dir: "fs_u", layout: nodeStatsMounts},
	"svc":        {dir: "svc", layout: nodeStatsDevices},
}

// nodeStatsMountSeries reads the series of every mount point under dir, the
// device of each series being its mount point.
func nodeStatsMountSeries(dir string, from, until int) []server.NodeStatSeries {
	out := []server.NodeStatSeries{}
	_ = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || !entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return nil
		}
		mount := "/"
		if rel != "." {
			mount = "/" + filepath.ToSlash(rel)
		}
		out = append(out, nodeStatsSeries(path, mount, from, until)...)
		return nil
	})
	return out
}

// nodeStatsRange is the time span of a request: begin and end when given, an
// end alone going back the days, a begin alone running to now, else the last
// days. The span is capped to nodeStatsMaxDays.
func nodeStatsRange(params server.GetNodeStatsParams, now time.Time) (int, int, error) {
	days := nodeStatsDefaultDays
	if params.Days != nil {
		days = min(max(*params.Days, 1), nodeStatsMaxDays)
	}
	span := time.Duration(days) * 24 * time.Hour
	until := now
	if params.End != nil {
		until = *params.End
	}
	from := until.Add(-span)
	if params.Begin != nil {
		from = *params.Begin
	}
	if !from.Before(until) {
		return 0, 0, fmt.Errorf("begin must be before end")
	}
	if until.Sub(from) > nodeStatsMaxDays*24*time.Hour {
		return 0, 0, fmt.Errorf("the span may not exceed %d days", nodeStatsMaxDays)
	}
	return int(from.Unix()), int(until.Unix()), nil
}

// nodeStatsSeries reads the series of the whisper files of a directory: one per
// file, named by the file, in name order.
func nodeStatsSeries(dir, device string, from, until int) []server.NodeStatSeries {
	files, err := filepath.Glob(filepath.Join(dir, "*.wsp"))
	if err != nil {
		return nil
	}
	sort.Strings(files)
	out := []server.NodeStatSeries{}
	for _, file := range files {
		points, err := readSeries(file, from, until)
		if err != nil {
			continue
		}
		s := server.NodeStatSeries{Metric: strings.TrimSuffix(filepath.Base(file), ".wsp"), Points: points}
		if device != "" {
			d := device
			s.Device = &d
		}
		out = append(out, s)
	}
	return out
}

// GetNodeStats handles GET /nodes/{node_id}/stats: the performance statistics of
// a node for one group — cpu, memory, swap, processes and load, block I/O,
// network, block devices, file systems, containers — over the last days or
// between two dates, read from the whisper files the
// historical collector kept under stats/nodes/<node_id>.
func (a *Api) GetNodeStats(c echo.Context, nodeId string, params server.GetNodeStatsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeStats")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	group, ok := nodeStatsGroups[string(params.Group)]
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "unknown group %q", params.Group)
	}
	stats := statsDirectory()
	if stats == "" {
		log.Error("no uploads directory configured")
		return JSONProblemf(c, http.StatusInternalServerError, "the series directory is not configured")
	}
	from, until, err := nodeStatsRange(params, time.Now())
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	// The node id comes from the database: a UUID, safe as a path element.
	base := filepath.Join(stats, "nodes", node.NodeID, filepath.FromSlash(group.dir))

	series := []server.NodeStatSeries{}
	switch group.layout {
	case nodeStatsMounts:
		series = nodeStatsMountSeries(base, from, until)
	case nodeStatsDevices:
		entries, err := os.ReadDir(base)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					series = append(series, nodeStatsSeries(filepath.Join(base, entry.Name()), entry.Name(), from, until)...)
				}
			}
		}
	default:
		series = nodeStatsSeries(base, "", from, until)
	}
	return c.JSON(http.StatusOK, server.NodeStatsResponse{Data: series})
}
