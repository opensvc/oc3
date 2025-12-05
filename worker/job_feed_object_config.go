package worker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	redis "github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
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
	var cfg *DBObjectConfig
	if created, objectID, err := d.oDb.objectIDFindOrCreate(d.ctx, d.objectName, d.clusterID); err != nil {
		return err
	} else {
		if created {
			slog.Info(fmt.Sprintf("obFeedObjectConfig will create service %s@%s with new svc_id: %s", d.objectName, d.clusterID, objectID))
		}
		slog.Debug(fmt.Sprintf("%s updateDB %s@%s will update found svc_id:%s", d.name, d.objectName, d.clusterID, objectID))

		var ha bool
		// Rules for svc_ha == 1: orchestrate is "ha" or topology == "flex" or monitored_resource_count > 0
		if mapToS(d.data, "", "orchestrate") == "ha" ||
			mapToS(d.data, "", "topology") == "flex" ||
			mapToInt(d.data, 0, "monitored_resource_count") > 0 {
			ha = true
		}
		cfg = &DBObjectConfig{
			name:      d.objectName,
			svcID:     objectID,
			clusterID: d.clusterID,
			updated:   d.now,
			comment:   mapToS(d.data, "", "comment"),
			flexMin:   mapToInt(d.data, 1, "flex_min"),
			flexMax:   mapToInt(d.data, 0, "flex_max"),
			ha:        ha,
		}
	}

	if s := mapToS(d.data, "", "raw_config"); s != "" {
		if b, err := base64.StdEncoding.DecodeString(s); err != nil {
			return fmt.Errorf("can't decode raw_config: %w", err)
		} else {
			cfg.config = string(b)
		}
	}
	var nilVal any
	if v, err := toString(mapToA(d.data, &nilVal, "scope"), " "); err == nil {
		cfg.nodes = &v
	}

	if v, err := toString(mapToA(d.data, &nilVal, "drpnodes"), " "); err == nil {
		cfg.drpNodes = &v
	}
	if s := mapToS(d.data, "", "drpnode"); s != "" {
		cfg.drpNode = &s
	}
	if s := mapToS(d.data, "", "app"); s != "" {
		cfg.app = &s
	}
	if s := mapToS(d.data, "", "env"); s != "" {
		cfg.env = &s
	}
	slog.Info(fmt.Sprintf("insertOrUpdateObjectConfig %s@%s@%sconfig: %s", cfg.name, cfg.svcID, cfg.clusterID, cfg.config))
	if hasRowAffected, err := d.oDb.insertOrUpdateObjectConfig(d.ctx, cfg); err != nil {
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
