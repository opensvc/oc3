package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type jobFeedActionBegin struct {
	*BaseJob

	nodeID    string
	clusterID string
	node      *cdb.DBNode

	// idX is the id of the posted action begin with the pattern: <objectName>@<nodeID>@<clusterID>:<action>
	idX string

	objectName string
	objectID   string

	// data is the posted action begin payload
	data *api.PostFeedActionBeginJSONRequestBody

	rawData []byte // necessaire ?
}

func newActionBegin(objectName, nodeID, clusterID, action string) *jobFeedActionBegin {
	idX := fmt.Sprintf("%s@%s@%s:%s", objectName, nodeID, clusterID, action)
	return &jobFeedActionBegin{
		BaseJob: &BaseJob{
			name:            "actionBegin",
			detail:          "ID: " + idX,
			cachePendingH:   cachekeys.FeedActionBeginPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedActionBegin) Operations() []operation {
	return []operation{
		{desc: "actionBegin/dropPending", do: d.dropPending},
		{desc: "actionBegin/findNodeFromDb", do: d.findNodeFromDb},
		{desc: "actionBegin/getData", do: d.getData},
		{desc: "actionBegin/findObjectFromDb", do: d.findObjectFromDb},
		{desc: "actionBegin/processAction", do: d.updateDB},
		{desc: "actionBegin/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedActionBegin) getData() error {
	var (
		data api.PostFeedActionBeginJSONRequestBody
	)
	if b, err := d.redis.HGet(d.ctx, cachekeys.FeedActionBeginH, d.idX).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedActionBeginH, d.idX, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedActionBeginH, d.idX, err)
	} else {
		d.rawData = b
		d.data = &data
	}

	slog.Info(fmt.Sprintf("got action begin data for node %s:%#v", d.nodeID, d.data))
	return nil
}

func (d *jobFeedActionBegin) findNodeFromDb() error {
	if n, err := d.oDb.NodeByNodeID(d.ctx, d.nodeID); err != nil {
		return fmt.Errorf("findNodeFromDb: node %s: %w", d.nodeID, err)
	} else {
		d.node = n
	}
	slog.Info(fmt.Sprintf("jobFeedActionBegin found node %s for id %s", d.node.Nodename, d.nodeID))
	return nil
}

func (d *jobFeedActionBegin) findObjectFromDb() error {
	if isNew, objId, err := d.oDb.ObjectIDFindOrCreate(d.ctx, d.objectName, d.clusterID); err != nil {
		return fmt.Errorf("find or create object ID failed for %s: %w", d.objectName, err)
	} else if isNew {
		slog.Info(fmt.Sprintf("jobFeedActionBegin has created new object id %s@%s %s", d.objectName, d.clusterID, objId))
	} else {
		d.objectID = objId
		slog.Info(fmt.Sprintf("jobFeedActionBegin found object id %s@%s %s", d.objectName, d.clusterID, objId))
	}

	return nil
}

func (d *jobFeedActionBegin) updateDB() error {
	// Log the action begin for audit/tracking purposes
	if d.data == nil || d.data.Path == "" {
		return fmt.Errorf("invalid action data: missing path")
	}

	// slog.Info(fmt.Sprintf("====> action begin on node %s: path=%s action=%s begin=%s node_id=%s object_id=%s",
	// 	d.node.Nodename,
	// 	d.data.Path,
	// 	d.data.Action,
	// 	d.data.Begin,
	// 	d.nodeID,
	// 	d.objectID,
	// ))

	// slog.Info(fmt.Sprintf("Object ID : %s", d.objectID))
	objectUUID, err := uuid.Parse(d.objectID)
	if err != nil {
		return fmt.Errorf("invalid object ID UUID: %w", err)
	}
	nodeUUID, err := uuid.Parse(d.nodeID)
	if err != nil {
		return fmt.Errorf("invalid node ID UUID: %w", err)
	}
	beginTime, err := cdb.ParseTimeWithTimezone(d.data.Begin, d.node.Tz)
	if err != nil {
		return fmt.Errorf("invalid begin time format: %w", err)
	}

	status_log := ""
	if len(d.data.Argv) > 0 {
		status_log = fmt.Sprintf("%s", d.data.Argv[0])
		for i := 1; i < len(d.data.Argv); i++ {
			status_log += " " + d.data.Argv[i]
		}
	}

	d.oDb.InsertSvcAction(d.ctx, objectUUID, nodeUUID, d.data.Action, beginTime, status_log, d.data.SessionUuid, d.data.Cron)

	return nil
}
