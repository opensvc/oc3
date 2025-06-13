package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/mariadb"
)

type (
	jobFeedObjectConfig struct {
		*BaseJob

		// idX is the id of the posted object config with expected pattern: <objectName>@<nodeID>@<clusterID>
		idX string

		objectName string

		// nodeID is db ID of the node that have posted object config
		nodeID string

		// clusterID is the db cluster ID of the node that have posted object config
		clusterID string

		// data is the posted object config
		data map[string]any
	}
)

func newFeedObjectConfig(objectName, nodeID, clusterID string) *jobFeedObjectConfig {
	idX := fmt.Sprintf("%s@%s@%s", objectName, nodeID, clusterID)
	return &jobFeedObjectConfig{
		BaseJob: &BaseJob{
			name:   "objectConfig",
			detail: "ID: " + idX,

			cachePendingH:   cachekeys.FeedObjectConfigPendingH,
			cachePendingIDX: idX,
		},
		idX:        idX,
		nodeID:     nodeID,
		clusterID:  clusterID,
		objectName: objectName,
	}
}

func (d *jobFeedObjectConfig) Operations() []operation {
	return []operation{
		{desc: "objectConfig/dropPending", do: d.dropPending},
		{desc: "objectConfig/getData", do: d.getData},
		{desc: "objectConfig/dbNow", do: d.dbNow},
		{desc: "objectConfig/updateDB", do: d.updateDB},
		{desc: "objectConfig/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedObjectConfig) getData() error {
	cmd := d.redis.HGet(d.ctx, cachekeys.FeedObjectConfigH, d.idX)
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

func (d *jobFeedObjectConfig) updateDB() (err error) {
	// expected data
	//instanceConfigPost struct {
	//	Path string `json:"path"`
	//
	//	Topology string `json:"topology"`
	//
	//	Orchestrate string `json:"orchestrate"`
	//
	//	FlexMin    int `json:"flex_min"`
	//	FlexMax    int `json:"flex_max"`
	//	FlexTarget int `json:"flex_target"`
	//
	//	App string `json:"app"`
	//
	//	Env string `json:"env"`
	//
	//	Scope    []string `json:"scope"`
	//	DrpNode  string   `json:"drp_node"`
	//	DrpNodes []string `json:"drp_nodes"`
	//
	//	Comment string `json:"comment"`
	//
	//	RawConfig []byte `json:"raw_config"`
	//}
	if _, objectID, err := d.oDb.objectIDFindOrCreate(d.ctx, d.objectName, d.clusterID); err != nil {
		return err
	} else {
		slog.Debug(fmt.Sprintf("%s updateDB %s@%s will update found svc_id:%s", d.name, d.objectName, d.clusterID, objectID))
		d.data["svc_id"] = objectID
	}

	d.data["updated"] = d.now

	// Rules for svc_ha == 1: orchestrate is "ha" or topology == "flex" or monitored_resource_count > 0
	if mapToS(d.data, "", "orchestrate") == "ha" ||
		mapToS(d.data, "", "topology") == "flex" ||
		mapToInt(d.data, 0, "monitored_resource_count") > 0 {
		d.data["svc_ha"] = 1
	} else {
		d.data["svc_ha"] = 0
	}

	request := mariadb.InsertOrUpdate{
		Table: "services",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{From: "path", To: "svcname"},
			mariadb.Mapping{To: "svc_id"},
			mariadb.Mapping{From: "scope", To: "svc_nodes", Optional: true, Modify: mariadb.ModifierToString(" ")},
			mariadb.Mapping{From: "drpnode", To: "svc_drpnode", Optional: true},
			mariadb.Mapping{From: "drpnodes", To: "svc_drpnodes", Optional: true, Modify: mariadb.ModifierToString(" ")},
			mariadb.Mapping{From: "app", To: "svc_app", Optional: true},
			mariadb.Mapping{From: "env", To: "svc_env", Optional: true},
			mariadb.Mapping{From: "comment", To: "svc_comment", Optional: true},
			mariadb.Mapping{From: "flex_min", To: "svc_flex_min_nodes", Optional: true},
			mariadb.Mapping{From: "flex_max", To: "svc_flex_max_nodes", Optional: true},
			mariadb.Mapping{To: "svc_ha", Optional: true},
			mariadb.Mapping{From: "raw_config", To: "svc_config", Optional: true, Modify: mariadb.ModifyFromBase64ToString},
			mariadb.Mapping{To: "updated"},
		},
		Keys: []string{"svc_id"},
		Data: d.data,
	}
	if affected, err := request.ExecContextAndCountRowsAffected(d.ctx, d.db); err != nil {
		return err
	} else if affected > 0 {
		d.oDb.tableChange("services")
	}
	return nil
}
