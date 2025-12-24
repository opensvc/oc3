package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"

	redis "github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

type (
	jobFeedInstanceResourceInfo struct {
		*BaseJob

		// idX is the id of the posted instance config with the expected pattern: <objectName>@<nodeID>@<clusterID>
		idX string

		objectName string

		// objectID is db ID of the object found or created in database
		objectID string

		// nodeID is db ID of the node that have posted object config
		nodeID string

		// clusterID is the db cluster ID of the node that have posted object config
		clusterID string

		// data is the posted instance resource info
		data api.InstanceResourceInfo
	}
)

func newjobFeedInstanceResourceInfo(objectName, nodeID, clusterID string) *jobFeedInstanceResourceInfo {
	idX := fmt.Sprintf("%s@%s@%s", objectName, nodeID, clusterID)
	return &jobFeedInstanceResourceInfo{
		BaseJob: &BaseJob{
			name:   "instanceResourceInfo",
			detail: "ID: " + idX,

			cachePendingH:   cachekeys.FeedInstanceResourceInfoPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedInstanceResourceInfo) Operations() []operation {
	return []operation{
		{desc: "instanceResourceInfo/dropPending", do: d.dropPending},
		{desc: "instanceResourceInfo/getData", do: d.getData},
		{desc: "instanceResourceInfo/dbNow", do: d.dbNow},
		{desc: "instanceResourceInfo/updateDB", do: d.updateDB},
		{desc: "instanceResourceInfo/purgeDB", do: d.purgeDB},
		{desc: "instanceResourceInfo/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedInstanceResourceInfo) getData() error {
	cmd := d.redis.HGet(d.ctx, cachekeys.FeedInstanceResourceInfoH, d.idX)
	result, err := cmd.Result()
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

func (d *jobFeedInstanceResourceInfo) updateDB() (err error) {
	created, objectID, err := d.oDb.ObjectIDFindOrCreate(d.ctx, d.objectName, d.clusterID)
	if err != nil {
		return fmt.Errorf("ObjectIDFindOrCreate: %w", err)
	}
	if created {
		slog.Info(fmt.Sprintf("jobFeedInstanceResourceInfo has created new object id %s@%s %s", d.objectName, d.clusterID, objectID))
	}
	d.objectID = objectID
	err = d.oDb.InstanceResourceInfoUpdate(d.ctx, objectID, d.nodeID, d.data)
	if err != nil {
		return fmt.Errorf("InstanceResourceInfoUpdate: %w", err)
	}

	return nil
}

func (d *jobFeedInstanceResourceInfo) purgeDB() (err error) {
	created, objectID, err := d.oDb.ObjectIDFindOrCreate(d.ctx, d.objectName, d.clusterID)
	if err != nil {
		return fmt.Errorf("ObjectIDFindOrCreate: %w", err)
	}
	if created {
		slog.Info(fmt.Sprintf("jobFeedInstanceResourceInfo has created new object id %s@%s %s", d.objectName, d.clusterID, objectID))
	}
	err = d.oDb.InstanceResourceInfoDelete(d.ctx, objectID, d.nodeID, d.now)
	if err != nil {
		return fmt.Errorf("InstanceResourceInfoDelete: %w", err)
	}

	return nil
}
