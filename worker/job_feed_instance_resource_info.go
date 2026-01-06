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

func (j *jobFeedInstanceResourceInfo) Operations() []operation {
	return []operation{
		{desc: "instanceResourceInfo/dropPending", do: j.dropPending},
		{desc: "instanceResourceInfo/getData", do: j.getData},
		{desc: "instanceResourceInfo/dbNow", do: j.dbNow},
		{desc: "instanceResourceInfo/updateDB", do: j.updateDB},
		{desc: "instanceResourceInfo/purgeDB", do: j.purgeDB},
		{desc: "instanceResourceInfo/pushFromTableChanges", do: j.pushFromTableChanges},
	}
}

func (j *jobFeedInstanceResourceInfo) getData() error {
	cmd := j.redis.HGet(j.ctx, cachekeys.FeedInstanceResourceInfoH, j.idX)
	result, err := cmd.Result()
	switch err {
	case nil:
	case redis.Nil:
		return fmt.Errorf("HGET: no results")
	default:
		return fmt.Errorf("HGET: %w", err)
	}
	if err := json.Unmarshal([]byte(result), &j.data); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func (j *jobFeedInstanceResourceInfo) updateDB() (err error) {
	created, objectID, err := j.oDb.ObjectIDFindOrCreate(j.ctx, j.objectName, j.clusterID)
	if err != nil {
		return fmt.Errorf("ObjectIDFindOrCreate: %w", err)
	}
	if created {
		slog.Info(fmt.Sprintf("jobFeedInstanceResourceInfo has created new object id %s@%s %s", j.objectName, j.clusterID, objectID))
	}
	j.objectID = objectID
	err = j.oDb.InstanceResourceInfoUpdate(j.ctx, objectID, j.nodeID, j.data)
	if err != nil {
		return fmt.Errorf("InstanceResourceInfoUpdate: %w", err)
	}

	return nil
}

func (j *jobFeedInstanceResourceInfo) purgeDB() (err error) {
	if j.objectID == "" {
		return fmt.Errorf("purgeDB: objectID is empty")
	}
	err = j.oDb.InstanceResourceInfoDelete(j.ctx, j.objectID, j.nodeID, j.now)
	if err != nil {
		return fmt.Errorf("InstanceResourceInfoDelete: %w", err)
	}

	return nil
}
