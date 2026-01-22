package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type jobFeedInstanceAction struct {
	JobBase
	JobRedis
	JobDB

	nodeID    string
	clusterID string
	node      *cdb.DBNode

	// idX is the id of the posted action begin with the pattern: <objectName>@<nodeID>@<clusterID>:<uuid>
	idX string

	objectName string
	objectID   string

	// data is the posted action begin payload
	data *api.PostFeedInstanceActionJSONRequestBody

	rawData []byte
}

func newAction(objectName, nodeID, clusterID, uuid string) *jobFeedInstanceAction {
	idX := fmt.Sprintf("%s@%s@%s:%s", objectName, nodeID, clusterID, uuid)

	return &jobFeedInstanceAction{
		JobBase: JobBase{
			name:   "instanceAction",
			detail: "ID: " + idX,
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedInstanceActionPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedInstanceAction) Operations() []operation {
	return []operation{
		{desc: "instanceAction/dropPending", do: d.dropPending},
		{desc: "instanceAction/getData", do: d.getData},
		{desc: "instanceAction/findNodeFromDb", do: d.findNodeFromDb},
		{desc: "instanceAction/findObjectFromDb", do: d.findObjectFromDb},
		{desc: "instanceAction/processAction", do: d.updateDB},
		{desc: "instanceAction/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedInstanceAction) getData(ctx context.Context) error {
	var (
		data api.PostFeedInstanceActionJSONRequestBody
	)
	if b, err := d.redis.HGet(ctx, cachekeys.FeedInstanceActionH, d.idX).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedInstanceActionH, d.idX, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedInstanceActionH, d.idX, err)
	} else {
		d.rawData = b
		d.data = &data
	}

	slog.Debug(fmt.Sprintf("got action begin data for node %s:%#v", d.nodeID, d.data))
	return nil
}

func (d *jobFeedInstanceAction) findNodeFromDb(ctx context.Context) error {
	if n, err := d.oDb.NodeByNodeID(ctx, d.nodeID); err != nil {
		return fmt.Errorf("findNodeFromDb: node %s: %w", d.nodeID, err)
	} else {
		d.node = n
	}
	slog.Debug(fmt.Sprintf("jobFeedInstanceAction found node %s for id %s", d.node.Nodename, d.nodeID))
	return nil
}

func (d *jobFeedInstanceAction) findObjectFromDb(ctx context.Context) error {
	if isNew, objId, err := d.oDb.ObjectIDFindOrCreate(ctx, d.objectName, d.clusterID); err != nil {
		return fmt.Errorf("find or create object ID failed for %s: %w", d.objectName, err)
	} else if isNew {
		slog.Info(fmt.Sprintf("jobFeedInstanceAction has created new object id %s@%s %s", d.objectName, d.clusterID, objId))
	} else {
		d.objectID = objId
		slog.Debug(fmt.Sprintf("jobFeedInstanceAction found object id %s@%s %s", d.objectName, d.clusterID, objId))
	}

	return nil
}

func (d *jobFeedInstanceAction) updateDB(ctx context.Context) error {
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
	beginTime, err := time.Parse(time.RFC3339Nano, d.data.Begin)
	if err != nil {
		return fmt.Errorf("invalid begin time format: %w", err)
	}

	var statusLog string
	if d.data.StatusLog != nil && len(*d.data.StatusLog) > 0 {
		statusLog = *d.data.StatusLog
	} else if len(d.data.Argv) > 0 {
		statusLog = strings.Join(d.data.Argv, " ")
	}

	if d.data.End != "" {
		// field End is present, process as an end action
		endTime, err := time.Parse(time.RFC3339Nano, d.data.End)
		if err != nil {
			return fmt.Errorf("invalid end time format: %w", err)
		}

		actionId, err := d.oDb.FindActionID(ctx, d.nodeID, d.objectID, beginTime, d.data.Action)
		if err != nil {
			return fmt.Errorf("find action ID failed: %w", err)
		}

		if actionId == 0 {
			// begin not processed yet, insert full record
			if _, err := d.oDb.InsertSvcAction(ctx, objectUUID, nodeUUID, d.data.Action, beginTime, statusLog, d.data.SessionUuid, d.data.Cron, endTime, d.data.Status); err != nil {
				return fmt.Errorf("insert svc action failed: %w", err)
			}
		} else {
			// begin already processed, update record with end info
			if err := d.oDb.UpdateSvcAction(ctx, actionId, endTime, d.data.Status, statusLog); err != nil {
				return fmt.Errorf("end svc action failed: %w", err)
			}
		}

		if (d.data.Action == "start" || d.data.Action == "startcontainer") && d.data.Status == "ok" {
			err := d.oDb.UpdateVirtualAsset(ctx, d.objectID, d.nodeID)
			if err != nil {
				return fmt.Errorf("update virtual asset failed: %w", err)
			}
		}

		if d.data.Status == "err" {
			if err := d.oDb.UpdateActionErrors(ctx, d.objectID, d.nodeID); err != nil {
				return fmt.Errorf("update action errors failed: %w", err)
			}
			if err := d.oDb.UpdateDashActionErrors(ctx, d.objectID, d.nodeID); err != nil {
				return fmt.Errorf("update dash action errors failed: %w", err)
			}
		}

	} else {
		// field End is not present, process as action begin
		if _, err := d.oDb.InsertSvcAction(ctx, objectUUID, nodeUUID, d.data.Action, beginTime, statusLog, d.data.SessionUuid, d.data.Cron, time.Time{}, ""); err != nil {
			return fmt.Errorf("insert new action failed: %w", err)
		}
	}

	return nil
}
