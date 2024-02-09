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

func (t *Worker) handleSystemProperties(nodeID string, i any) error {
	data, ok := i.(map[string]any)
	if !ok {
		slog.Warn("unsupported system properties format")
		return nil
	}

	request := mariadb.InsertOrUpdate{
		Table: "nodes",
		Columns: mariadb.Columns{
			mariadb.Column{Name: "asset_env"},
			mariadb.Column{Name: "bios_version"},
			mariadb.Column{Name: "cluster_id"},
			mariadb.Column{Name: "connect_to"},
			mariadb.Column{Name: "cpu_cores"},
			mariadb.Column{Name: "cpu_dies"},
			mariadb.Column{Name: "cpu_freq"},
			mariadb.Column{Name: "cpu_model"},
			mariadb.Column{Name: "cpu_threads"},
			mariadb.Column{Name: "enclosure"},
			mariadb.Column{Name: "fqdn"},
			mariadb.Column{Name: "last_boot"},
			mariadb.Column{Name: "listener_port"},
			mariadb.Column{Name: "loc_addr"},
			mariadb.Column{Name: "loc_building"},
			mariadb.Column{Name: "loc_city"},
			mariadb.Column{Name: "loc_country"},
			mariadb.Column{Name: "loc_floor"},
			mariadb.Column{Name: "loc_rack"},
			mariadb.Column{Name: "loc_room"},
			mariadb.Column{Name: "loc_zip"},
			mariadb.Column{Name: "manufacturer"},
			mariadb.Column{Name: "mem_banks"},
			mariadb.Column{Name: "mem_bytes"},
			mariadb.Column{Name: "mem_slots"},
			mariadb.Column{Name: "model"},
			mariadb.Column{Name: "node_id"},
			mariadb.Column{Name: "node_env"},
			mariadb.Column{Name: "nodename"},
			mariadb.Column{Name: "os_arch"},
			mariadb.Column{Name: "os_kernel"},
			mariadb.Column{Name: "os_name"},
			mariadb.Column{Name: "os_vendor"},
			mariadb.Column{Name: "sec_zone"},
			mariadb.Column{Name: "serial"},
			mariadb.Column{Name: "sp_version"},
			mariadb.Column{Name: "team_integ"},
			mariadb.Column{Name: "team_support"},
			mariadb.Column{Name: "tz"},
			mariadb.Column{Name: "version"},
		},
		Keys: []string{"node_id"},
	}
	request.Add("node_id", nodeID)
	request.AddString("updated", "NOW()")
	err := request.LoadWithAccessor(data, func(v any) (any, error) {
		keyData, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unsupported system property format")
		}
		value, ok := keyData["value"]
		if !ok {
			return nil, fmt.Errorf("unsupported system property format: key not found")
		}
		return value, nil
	})
	if err != nil {
		return err
	}

	_, err = request.Query(t.DB)

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
		case "properties":
			if err := t.handleSystemProperties(nodeID, i); err != nil {
				return err
			}
		default:
			slog.Warn(fmt.Sprintf("unsupported system sub: %s", k))
		}
	}
	return nil
}
