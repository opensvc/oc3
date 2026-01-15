package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type jobFeedActionEnd struct {
	*BaseJob

	nodeID    string
	clusterID string
	node      *cdb.DBNode

	// idX is the id of the posted action end with the pattern: <nodeID>@<clusterID>
	idX string

	objectName string
	objectID   string

	// data is the posted action end payload
	data *api.PutFeedActionEndJSONRequestBody

	rawData []byte
}

func newActionEnd(objectName, nodeID, clusterID string) *jobFeedActionEnd {
	idX := fmt.Sprintf("%s@%s@%s", objectName, nodeID, clusterID)
	return &jobFeedActionEnd{
		BaseJob: &BaseJob{
			name:            "actionEnd",
			detail:          "ID: " + idX,
			cachePendingH:   cachekeys.FeedActionEndPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedActionEnd) Operations() []operation {
	return []operation{
		{desc: "actionEnd/dropPending", do: d.dropPending},
		{desc: "actionEnd/findNodeFromDb", do: d.findNodeFromDb},
		{desc: "actionEnd/getData", do: d.getData},
		{desc: "actionEnd/findObjectFromDb", do: d.findObjectFromDb},
		{desc: "actionEnd/processAction", do: d.updateDb},
		{desc: "actionEnd/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedActionEnd) getData() error {
	var (
		data api.PutFeedActionEndJSONRequestBody
	)
	if b, err := d.redis.HGet(d.ctx, cachekeys.FeedActionEndH, d.idX).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedActionEndH, d.idX, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedActionEndH, d.idX, err)
	} else {
		d.rawData = b
		d.data = &data
	}

	slog.Info(fmt.Sprintf("got action end data for node %s:%#v", d.nodeID, d.data))
	return nil
}

func (d *jobFeedActionEnd) findNodeFromDb() error {
	if n, err := d.oDb.NodeByNodeID(d.ctx, d.nodeID); err != nil {
		return fmt.Errorf("findNodeFromDb: node %s: %w", d.nodeID, err)
	} else {
		d.node = n
	}
	slog.Info(fmt.Sprintf("jobFeedActionEnd found node %s for id %s", d.node.Nodename, d.nodeID))
	return nil
}

func (d *jobFeedActionEnd) findObjectFromDb() error {
	if isNew, objId, err := d.oDb.ObjectIDFindOrCreate(d.ctx, d.objectName, d.clusterID); err != nil {
		return fmt.Errorf("find or create object ID failed for %s: %w", d.objectName, err)
	} else if isNew {
		slog.Info(fmt.Sprintf("jobFeedActionEnd has created new object id %s@%s %s", d.objectName, d.clusterID, objId))
	} else {
		d.objectID = objId
		slog.Info(fmt.Sprintf("jobFeedActionEnd found object id %s@%s %s", d.objectName, d.clusterID, objId))
	}

	return nil
}

func (d *jobFeedActionEnd) updateDb() error {
	if d.data == nil || d.data.Path == "" {
		return fmt.Errorf("invalid action data: missing path")
	}

	beginTime, err := cdb.ParseTimeWithTimezone(d.data.Begin, d.node.Tz)
	if err != nil {
		return fmt.Errorf("invalid begin time format: %w", err)
	}

	endTime, err := cdb.ParseTimeWithTimezone(d.data.End, d.node.Tz)
	if err != nil {
		return fmt.Errorf("invalid end time format: %w", err)
	}

	actionId, err := d.oDb.FindActionID(d.ctx, d.nodeID, d.objectID, beginTime)
	if err != nil {
		return fmt.Errorf("find action ID failed: %w", err)
	}

	if err := d.oDb.EndSvcAction(d.ctx, actionId, endTime, d.data.Status); err != nil {
		return fmt.Errorf("end svc action failed: %w", err)
	}

	return nil
}
