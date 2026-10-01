package serverhandlers

import (
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

// nodeStatsGroups are the groups of node statistics, as the historical collector
// stored them: the directory of each group under stats/nodes/<node_id>, and
// whether the group has one directory per device (network interface, block
// device) holding the metrics, rather than the metrics themselves.
var nodeStatsGroups = map[string]struct {
	dir     string
	devices bool
}{
	"cpu":        {dir: "cpu/all"},
	"mem":        {dir: "mem_u"},
	"swap":       {dir: "swap"},
	"proc":       {dir: "proc"},
	"block":      {dir: "block"},
	"netdev":     {dir: "netdev", devices: true},
	"netdev_err": {dir: "netdev_err", devices: true},
	"blockdev":   {dir: "blockdev", devices: true},
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
// network, block devices — over the last days, read from the whisper files the
// historical collector kept under stats/nodes/<node_id>.
func (a *Api) GetNodeStats(c echo.Context, nodeId string, params server.GetNodeStatsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeStats")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil || node == nil {
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
	days := nodeStatsDefaultDays
	if params.Days != nil {
		days = min(max(*params.Days, 1), nodeStatsMaxDays)
	}
	until := int(time.Now().Unix())
	from := until - days*24*3600
	// The node id comes from the database: a UUID, safe as a path element.
	base := filepath.Join(stats, "nodes", node.NodeID, filepath.FromSlash(group.dir))

	series := []server.NodeStatSeries{}
	if group.devices {
		entries, err := os.ReadDir(base)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					series = append(series, nodeStatsSeries(filepath.Join(base, entry.Name()), entry.Name(), from, until)...)
				}
			}
		}
	} else {
		series = nodeStatsSeries(base, "", from, until)
	}
	return c.JSON(http.StatusOK, server.NodeStatsResponse{Data: series})
}
