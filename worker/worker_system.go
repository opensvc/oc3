package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"
	"github.com/opensvc/oc3/cache"
	"github.com/opensvc/oc3/mariadb"
)

func (t *Worker) handleSystemHardware(nodeID string, i any) error {
	data, ok := i.([]any)
	if !ok {
		slog.Warn("unsupported system hardware table format")
		return nil
	}

	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported system hardware entry format")
			return nil
		}
		line["node_id"] = nodeID
		line["updated"] = mariadb.Raw("NOW()")
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "node_hw",
		Mappings: mariadb.Mappings{
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewMapping("hw_type", "type"),
			mariadb.NewMapping("hw_path", "path"),
			mariadb.NewMapping("hw_class", "class"),
			mariadb.NewMapping("hw_description", "description"),
			mariadb.NewMapping("hw_driver", "driver"),
			mariadb.NewNaturalMapping("updated"),
		},
		Keys: []string{"node_id"},
		Data: data,
	}

	_, err := request.Query(t.DB)

	return err
}

func (t *Worker) handleSystemProperties(nodeID string, i any) error {
	data, ok := i.(map[string]any)
	if !ok {
		slog.Warn("unsupported system properties format")
		return nil
	}

	data["node_id"] = map[string]any{"value": nodeID}
	data["updated"] = mariadb.Raw("NOW()")

	request := mariadb.InsertOrUpdate{
		Table: "nodes",
		Mappings: mariadb.Mappings{
			mariadb.NewNaturalMapping("asset_env"),
			mariadb.NewNaturalMapping("bios_version"),
			mariadb.NewNaturalMapping("cluster_id"),
			mariadb.NewNaturalMapping("connect_to"),
			mariadb.NewNaturalMapping("cpu_cores"),
			mariadb.NewNaturalMapping("cpu_dies"),
			mariadb.NewNaturalMapping("cpu_freq"),
			mariadb.NewNaturalMapping("cpu_model"),
			mariadb.NewNaturalMapping("cpu_threads"),
			mariadb.NewNaturalMapping("enclosure"),
			mariadb.NewNaturalMapping("fqdn"),
			mariadb.NewNaturalMapping("last_boot"),
			mariadb.NewNaturalMapping("listener_port"),
			mariadb.NewNaturalMapping("loc_addr"),
			mariadb.NewNaturalMapping("loc_building"),
			mariadb.NewNaturalMapping("loc_city"),
			mariadb.NewNaturalMapping("loc_country"),
			mariadb.NewNaturalMapping("loc_floor"),
			mariadb.NewNaturalMapping("loc_rack"),
			mariadb.NewNaturalMapping("loc_room"),
			mariadb.NewNaturalMapping("loc_zip"),
			mariadb.NewNaturalMapping("manufacturer"),
			mariadb.NewNaturalMapping("mem_banks"),
			mariadb.NewNaturalMapping("mem_bytes"),
			mariadb.NewNaturalMapping("mem_slots"),
			mariadb.NewNaturalMapping("model"),
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("node_env"),
			mariadb.NewNaturalMapping("nodename"),
			mariadb.NewNaturalMapping("os_arch"),
			mariadb.NewNaturalMapping("os_kernel"),
			mariadb.NewNaturalMapping("os_name"),
			mariadb.NewNaturalMapping("os_vendor"),
			mariadb.NewNaturalMapping("sec_zone"),
			mariadb.NewNaturalMapping("serial"),
			mariadb.NewNaturalMapping("sp_version"),
			mariadb.NewNaturalMapping("team_integ"),
			mariadb.NewNaturalMapping("team_support"),
			mariadb.NewNaturalMapping("tz"),
			mariadb.NewNaturalMapping("version"),
		},
		Keys: []string{"node_id"},
		Accessor: func(v any) (any, error) {
			keyData, ok := v.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("unsupported system property format")
			}
			value, ok := keyData["value"]
			if !ok {
				return nil, fmt.Errorf("'value' key not found in property")
			}
			return value, nil
		},
		Data: data,
	}

	_, err := request.Query(t.DB)

	return err
}

func (t *Worker) handleSystem(nodeID string) error {
	cmd := t.Redis.HGet(context.Background(), cache.KeySystemHash, nodeID)
	result, err := cmd.Result()
	switch err {
	case nil:
	case redis.Nil:
		return nil
	default:
		return err
	}

	var v map[string]any
	if err := json.Unmarshal([]byte(result), &v); err != nil {
		return err
	}

	for k, i := range v {
		switch k {
		case "hardware":
			err = t.handleSystemHardware(nodeID, i)
		case "properties":
			err = t.handleSystemProperties(nodeID, i)
		default:
			slog.Warn(fmt.Sprintf("unsupported system sub: %s", k))
		}
		if err != nil {
			slog.Warn(fmt.Sprintf("%s: %s", k, err))
		}
	}
	return nil
}
