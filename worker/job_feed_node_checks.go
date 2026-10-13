package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/go-graphite/go-whisper"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/timeseries"
	"github.com/opensvc/oc3/util/logkey"
)

type (
	// jobFeedNodeChecks replaces the checks of a node with the ones it
	// reported, sets their thresholds, alerts the ones out of them, and
	// records their values in the timeseries, as the push_checks of the old
	// collector did.
	jobFeedNodeChecks struct {
		JobBase
		JobRedis
		JobDB
		JobEv
		JobUpload

		nodename  string
		nodeID    string
		clusterID string
		data      feeder.NodeChecks
	}
)

const (
	// checkTypeMaxLen and checkInstanceMaxLen are the sizes of the
	// chk_type and chk_instance columns.
	checkTypeMaxLen     = 10
	checkInstanceMaxLen = 180
)

func newNodeChecks(nodename, nodeID, clusterID string) *jobFeedNodeChecks {
	return &jobFeedNodeChecks{
		JobBase: JobBase{
			name:   jtNodeChecks,
			detail: "nodename: " + nodename + " nodeID: " + nodeID,
			logger: slog.With(logkey.NodeID, nodeID, logkey.ClusterID, clusterID, logkey.Nodename, nodename, logkey.JobName, jtNodeChecks),
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedNodeChecksPendingH,
			cachePendingIDX: nodename + "@" + nodeID + "@" + clusterID,
		},
		clusterID: clusterID,
		nodeID:    nodeID,
		nodename:  nodename,
	}
}

func (d *jobFeedNodeChecks) Operations() []operation {
	return []operation{
		{name: "dropPending", do: d.dropPending},
		// Blocking: the checks not in the feed read are purged, all of
		// them when it was not read.
		{name: "getData", do: d.getData, blocking: true},
		{name: "dbNow", do: d.dbNow, blocking: true},
		{name: "updateDB", do: d.updateDB, blocking: true},
		{name: "updateThresholds", do: d.updateThresholds, blocking: true},
		{name: "updateDashboard", do: d.updateDashboard, blocking: true},
		{name: "pushFromTableChanges", do: d.pushFromTableChanges},
		{name: "updateTimeseries", do: d.updateTimeseries, blocking: false},
		{name: "publishChange", do: d.publishChange, blocking: false},
	}
}

func (d *jobFeedNodeChecks) getData(ctx context.Context) error {
	result, err := d.redis.HGet(ctx, cachekeys.FeedNodeChecksH, d.cachePendingIDX).Result()
	switch err {
	case nil:
	case redis.Nil:
		return fmt.Errorf("HGET: no results")
	default:
		return fmt.Errorf("HGET: %w", err)
	}
	if err := json.Unmarshal([]byte(result), &d.data); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

// updateDB stores the checks reported, and removes the ones the node no
// longer reports. A partial feed, of a run where a checker failed, removes
// none: the checks of the failed checker are missing from it, and their last
// known values and alerts must stay until a complete run tells their state.
//
// A check is attributed to the object of its path. A check of the node
// itself, with no path, is attributed to the object running the node as a
// virtual machine, if any.
func (d *jobFeedNodeChecks) updateDB(ctx context.Context) error {
	var (
		pathToObjectID = make(map[string]string)
		vmObjectID     *string
	)
	rows := make([]cdb.CheckFeedRow, 0, len(d.data.Data))
	for _, c := range d.data.Data {
		row := cdb.CheckFeedRow{
			Type:     truncate(c.Type, checkTypeMaxLen),
			Instance: truncate(c.Instance, checkInstanceMaxLen),
			Value:    c.Value,
		}
		if row.Type == "" {
			continue
		}
		path := ""
		if c.Path != nil {
			path = *c.Path
		}
		switch {
		case path != "":
			objectID, ok := pathToObjectID[path]
			if !ok {
				_, id, err := d.oDb.ObjectIDFindOrCreate(ctx, path, d.clusterID)
				if err != nil {
					return fmt.Errorf("object id of %s: %w", path, err)
				}
				objectID = id
				pathToObjectID[path] = id
			}
			row.SvcID = objectID
		default:
			if vmObjectID == nil {
				id, err := d.oDb.ObjectIDOfVM(ctx, d.nodename)
				if err != nil {
					return err
				}
				vmObjectID = &id
			}
			row.SvcID = *vmObjectID
		}
		rows = append(rows, row)
	}
	if err := d.oDb.ChecksLiveUpsert(ctx, d.nodeID, rows, d.now); err != nil {
		return err
	}
	if d.data.Partial != nil && *d.data.Partial {
		d.Logger().Info("partial checks feed: keep the checks not reported")
		return nil
	}
	return d.oDb.ChecksLivePurgeBefore(ctx, d.nodeID, d.now)
}

func (d *jobFeedNodeChecks) updateThresholds(ctx context.Context) error {
	return d.oDb.ChecksLiveUpdateThresholds(ctx, d.nodeID)
}

func (d *jobFeedNodeChecks) updateDashboard(ctx context.Context) error {
	return d.oDb.DashboardUpdateChecksOutOfBounds(ctx, d.nodeID, d.now)
}

// updateTimeseries records the value of each check of the node, in
// <uploads>/stats/nodes/<node id>/checks/<object id>:<type>:<instance>.wsp,
// the instance base64 url encoded, as the old collector named them.
func (d *jobFeedNodeChecks) updateTimeseries(ctx context.Context) error {
	if d.UploadDir == "" {
		return nil
	}
	rows, err := d.oDb.ChecksLiveForNode(ctx, d.nodeID)
	if err != nil {
		return err
	}
	timestamp := int(d.now.Unix())
	dir := filepath.Join(d.UploadDir, "stats", "nodes", d.nodeID, "checks")
	var failed int
	for _, r := range rows {
		name := fmt.Sprintf("%s:%s:%s.wsp", r.SvcID, r.Type, base64.URLEncoding.EncodeToString([]byte(r.Instance)))
		if err := timeseries.Update(filepath.Join(dir, name), float64(r.Value), timestamp, timeseries.DefaultRetentions, whisper.Average, 0.5); err != nil {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d check timeseries not updated", failed, len(rows))
	}
	return nil
}

func (d *jobFeedNodeChecks) publishChange(ctx context.Context) error {
	if d.ev == nil {
		return nil
	}
	return d.ev.EventPublish("checks_change", map[string]any{"node_id": d.nodeID})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
