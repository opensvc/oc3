package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type jobFeedAction struct {
	*BaseJob

	nodeID    string
	clusterID string
	node      *cdb.DBNode

	// idX is the id of the posted action begin with the pattern: <objectName>@<nodeID>@<clusterID>:<uuid>
	idX string

	objectName string
	objectID   string

	// data is the posted action begin payload
	data *api.PostFeedActionJSONRequestBody

	rawData []byte
}

func newAction(objectName, nodeID, clusterID, uuid string) *jobFeedAction {
	idX := fmt.Sprintf("%s@%s@%s:%s", objectName, nodeID, clusterID, uuid)

	return &jobFeedAction{
		BaseJob: &BaseJob{
			name:            "action",
			detail:          "ID: " + idX,
			cachePendingH:   cachekeys.FeedActionPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedAction) Operations() []operation {
	return []operation{
		{desc: "actionBegin/dropPending", do: d.dropPending},
		{desc: "actionBegin/getData", do: d.getData},
		{desc: "actionBegin/findNodeFromDb", do: d.findNodeFromDb},
		{desc: "actionBegin/findObjectFromDb", do: d.findObjectFromDb},
		{desc: "actionBegin/processAction", do: d.updateDB},
		{desc: "actionBegin/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedAction) getData() error {
	var (
		data api.PostFeedActionJSONRequestBody
	)
	slog.Info(fmt.Sprintf("xxx - get - %S", d.idX))
	if b, err := d.redis.HGet(d.ctx, cachekeys.FeedActionH, d.idX).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedActionH, d.idX, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedActionH, d.idX, err)
	} else {
		d.rawData = b
		d.data = &data
	}

	slog.Info(fmt.Sprintf("got action begin data for node %s:%#v", d.nodeID, d.data))
	return nil
}

func (d *jobFeedAction) findNodeFromDb() error {
	slog.Info(fmt.Sprintf("xxx - NOdename - %s", d.nodeID))
	if n, err := d.oDb.NodeByNodeID(d.ctx, d.nodeID); err != nil {
		return fmt.Errorf("findNodeFromDb: node %s: %w", d.nodeID, err)
	} else {
		d.node = n
	}
	slog.Info(fmt.Sprintf("jobFeedActionBegin found node %s for id %s", d.node.Nodename, d.nodeID))
	return nil
}

func (d *jobFeedAction) findObjectFromDb() error {
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

func (d *jobFeedAction) updateDB() error {
	if d.data == nil || d.data.Path == "" {
		return fmt.Errorf("invalid action data: missing path")
	}

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

	if d.data.End != "" {
		// field End is present, process as action end
		endTime, err := cdb.ParseTimeWithTimezone(d.data.End, d.node.Tz)
		if err != nil {
			return fmt.Errorf("invalid end time format: %w", err)
		}

		actionId, err := d.oDb.FindActionID(d.ctx, d.nodeID, d.objectID, beginTime, d.data.Action)
		if err != nil {
			return fmt.Errorf("find action ID failed: %w", err)
		}

		if actionId == 0 {
			// begin not processed yet, insert full record
			if _, err := d.oDb.InsertSvcAction(d.ctx, objectUUID, nodeUUID, d.data.Action, beginTime, status_log, d.data.SessionUuid, d.data.Cron, endTime, d.data.Status); err != nil {
				return fmt.Errorf("insert svc action failed: %w", err)
			}
		} else {
			// begin already processed, update record with end info
			if err := d.oDb.UpdateSvcAction(d.ctx, actionId, endTime, d.data.Status); err != nil {
				return fmt.Errorf("end svc action failed: %w", err)
			}
		}

		if d.data.Status == "err" {
			if err := d.oDb.UpdateActionErrors(d.ctx, d.objectID, d.nodeID); err != nil {
				return fmt.Errorf("update action errors failed: %w", err)
			}
			if err := d.oDb.UpdateDashActionErrors(d.ctx, d.objectID, d.nodeID); err != nil {
				return fmt.Errorf("update dash action errors failed: %w", err)
			}
		}

	} else {
		// field End is not present, process as action begin
		d.oDb.InsertSvcAction(d.ctx, objectUUID, nodeUUID, d.data.Action, beginTime, status_log, d.data.SessionUuid, d.data.Cron, time.Time{}, "")
	}

	return nil
}
