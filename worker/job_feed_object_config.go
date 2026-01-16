package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type (
	jobFeedObjectConfig struct {
		JobBase
		JobRedis
		JobDB

		// idX is the id of the posted object config with the expected pattern: <objectName>@<nodeID>@<clusterID>
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
		JobBase: JobBase{
			name:   "objectConfig",
			detail: "ID: " + idX,
		},
		JobRedis: JobRedis{
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

func (d *jobFeedObjectConfig) getData(ctx context.Context) error {
	cmd := d.redis.HGet(ctx, cachekeys.FeedObjectConfigH, d.idX)
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

func (d *jobFeedObjectConfig) updateDB(ctx context.Context) (err error) {
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
	var cfg *cdb.DBObjectConfig
	if created, objectID, err := d.oDb.ObjectIDFindOrCreate(ctx, d.objectName, d.clusterID); err != nil {
		return err
	} else {
		if created {
			slog.Info(fmt.Sprintf("jobFeedObjectConfig will create service %s@%s with new svc_id: %s", d.objectName, d.clusterID, objectID))
		}
		slog.Debug(fmt.Sprintf("%s updateDB %s@%s will update found svc_id:%s", d.name, d.objectName, d.clusterID, objectID))

		var ha bool
		// Rules for svc_ha == 1: orchestrate is "ha" or topology == "flex" or monitored_resource_count > 0
		if mapToS(d.data, "", "orchestrate") == "ha" ||
			mapToS(d.data, "", "topology") == "flex" ||
			mapToInt(d.data, 0, "monitored_resource_count") > 0 {
			ha = true
		}
		cfg = &cdb.DBObjectConfig{
			Name:      d.objectName,
			SvcID:     objectID,
			ClusterID: d.clusterID,
			Updated:   d.now,
			Comment:   mapToS(d.data, "", "comment"),
			FlexMin:   mapToInt(d.data, 1, "flex_min"),
			FlexMax:   mapToInt(d.data, 0, "flex_max"),
			HA:        ha,
		}
	}

	if s := mapToS(d.data, "", "raw_config"); s != "" {
		if b, err := base64.StdEncoding.DecodeString(s); err != nil {
			return fmt.Errorf("can't decode raw_config: %w", err)
		} else {
			cfg.Config = string(b)
		}
	}
	var nilVal any
	if v, err := toString(mapToA(d.data, &nilVal, "scope"), " "); err == nil {
		cfg.Nodes = &v
	}

	if v, err := toString(mapToA(d.data, &nilVal, "drpnodes"), " "); err == nil {
		cfg.DrpNodes = &v
	}
	if s := mapToS(d.data, "", "drpnode"); s != "" {
		cfg.DrpNode = &s
	}
	if s := mapToS(d.data, "", "app"); s != "" {
		cfg.App = &s
	}
	if s := mapToS(d.data, "", "env"); s != "" {
		cfg.Env = &s
	}
	slog.Info(fmt.Sprintf("insertOrUpdateObjectConfig %s@%s@%s", cfg.Name, cfg.SvcID, cfg.ClusterID))
	if hasRowAffected, err := d.oDb.InsertOrUpdateObjectConfig(ctx, cfg); err != nil {
		return err
	} else if hasRowAffected {
		d.oDb.SetChange("services")
	}
	return nil
}

func toString(a any, sep string) (string, error) {
	switch v := a.(type) {
	case string:
		return v, nil
	case []string:
		return strings.Join(v, sep), nil
	case []interface{}:
		var l []string
		for _, i := range v {
			l = append(l, i.(string))
		}
		return strings.Join(l, sep), nil
	case []byte:
		return string(v), nil
	default:
		return "", fmt.Errorf("to string can't analyse type %s", reflect.TypeOf(a))
	}
}
