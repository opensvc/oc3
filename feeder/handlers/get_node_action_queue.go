package feederhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

// wakeCommandPattern is the wake of a node a peer runs, as the collector
// queues it: its target is the first group.
var wakeCommandPattern = regexp.MustCompile(`^wake --node ([A-Za-z0-9][A-Za-z0-9._-]*)$`)

func (a *Api) GetNodeActionQueued(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "GetNodeActionQueued")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}
	ctx := c.Request().Context()
	var l []cdb.ActionQueueNamed
	l, err := a.ODB.ActionQueueNamedByNodeID(ctx, nodeID, cdb.ActionQTypeFeed)
	if err != nil {
		log.Error("get node action queued", logkey.Error, err)
		return JSONError(c)
	}

	l, cancelled, err := a.cancelWakesOfNodesUp(ctx, log, nodeID, l)
	if err != nil {
		log.Error("cancel the wakes of nodes up", logkey.Error, err)
		return JSONError(c)
	}

	actions := make([]feeder.QueuedAction, len(l))
	ids := make([]int, len(l))
	for i, a := range l {
		actions[i] = feeder.QueuedAction{
			Command:    a.Command,
			DequeuedAt: a.DateDequeued,
			QueuedAt:   a.DateQueued,
			Id:         a.ID,
			SvcId:      a.SvcId,
			Svcname:    a.Svcname,
			Nodename:   a.Nodename,
			Status:     a.Status,
		}
		ids[i] = a.ID
	}
	if err := a.ODB.ActionQSetSent(ctx, ids...); err != nil {
		log.Error("set action sent", logkey.Error, err)
		return JSONError(c)
	}
	if len(ids) > 0 || cancelled > 0 {
		a.notifyChanges(c)
	}
	return c.JSON(200, feeder.QueuedActions{Actions: actions})
}

// cancelWakesOfNodesUp cancels the wakes of the actions a node dequeues whose
// target is up now, as the last status of its cluster says, and returns the
// other actions: the wake was queued while the target was down, on a peer
// down too that dequeues it once back, or the target came back by other
// means. Run, it would start the virtual machine of a node already up, on a
// node just back, joining its cluster.
func (a *Api) cancelWakesOfNodesUp(ctx context.Context, log *slog.Logger, nodeID string, l []cdb.ActionQueueNamed) ([]cdb.ActionQueueNamed, int, error) {
	var clusterID string
	kept := l[:0:0]
	cancelled := 0
	for _, action := range l {
		m := wakeCommandPattern.FindStringSubmatch(action.Command)
		if m == nil || action.SvcId != "" {
			kept = append(kept, action)
			continue
		}
		if clusterID == "" {
			node, err := a.ODB.NodeByNodeID(ctx, nodeID)
			if err != nil {
				return nil, 0, err
			}
			if node == nil || node.ClusterID == "" {
				return l, 0, nil
			}
			clusterID = node.ClusterID
		}
		targetID, err := a.ODB.NodeIDByNodenameAndCluster(ctx, m[1], clusterID)
		if err != nil {
			return nil, 0, err
		}
		up := false
		if targetID != "" {
			if up, err = a.ODB.NodeUp(ctx, targetID); err != nil {
				return nil, 0, err
			}
		}
		if !up {
			kept = append(kept, action)
			continue
		}
		reason := fmt.Sprintf("node %s is already up: wake cancelled", m[1])
		if err := a.ODB.ActionQSetCancelled(ctx, action.ID, reason); err != nil {
			return nil, 0, err
		}
		log.Info("wake of a node up cancelled", "action_id", action.ID, "target", m[1])
		cancelled++
	}
	return kept, cancelled, nil
}
