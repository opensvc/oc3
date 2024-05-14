package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cache"
	"github.com/opensvc/oc3/mariadb"
)

type (
	jobSystem struct {
		*BaseJob

		nodeID string
		data   map[string]any
	}
)

func newDaemonSystem(nodeID string) *jobSystem {
	return &jobSystem{
		BaseJob: &BaseJob{
			name:   "daemonSystem",
			detail: "nodeID: " + nodeID,
		},
		nodeID: nodeID,
	}
}

func (d *jobSystem) targets() error {
	data, ok := d.data["targets"].([]any)
	if !ok {
		slog.Warn("unsupported system targets data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported system targets entry format")
			return nil
		}
		line["node_id"] = nodeID
		line["updated"] = now
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "stor_zone",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "updated"},
			mariadb.Mapping{To: "hba_id"},
			mariadb.Mapping{To: "tgt_id"},
		},
		Keys: []string{"node_id", "hba_id", "tgt_id"},
		Data: data,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM stor_zone WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobSystem) hba() error {
	data, ok := d.data["hba"].([]any)
	if !ok {
		slog.Warn("unsupported system hba data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported system hba entry format")
			return nil
		}
		line["node_id"] = nodeID
		line["updated"] = now
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "node_hba",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "updated"},
			mariadb.Mapping{To: "hba_id"},
			mariadb.Mapping{To: "hba_type"},
		},
		Keys: []string{"node_id", "hba_id"},
		Data: data,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM node_hba WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobSystem) lan() error {
	var l []any
	data, ok := d.data["lan"].(map[string]any)
	if !ok {
		slog.Warn("unsupported system lan data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for mac, addressesInterface := range data {
		addresses, ok := addressesInterface.([]any)
		if !ok {
			slog.Warn("unsupported system lan addresses format")
			return nil
		}
		for _, addressInterface := range addresses {
			line, ok := addressInterface.(map[string]any)
			if !ok {
				slog.Warn("unsupported system lan address format")
				return nil
			}
			line["mac"] = mac
			line["node_id"] = nodeID
			line["updated"] = now
			l = append(l, line)
		}
	}

	request := mariadb.InsertOrUpdate{
		Table: "node_ip",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "updated"},
			mariadb.Mapping{To: "mac"},
			mariadb.Mapping{To: "intf"},
			mariadb.Mapping{To: "type"},
			mariadb.Mapping{To: "addr"},
			mariadb.Mapping{To: "mask"},
			mariadb.Mapping{To: "flag_deprecated"},
		},
		Keys: []string{"node_id"},
		Data: l,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM node_ip WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobSystem) groups() error {
	data, ok := d.data["gids"].([]any)
	if !ok {
		slog.Warn("unsupported system groups data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported system groups entry format")
			return nil
		}
		line["node_id"] = nodeID
		line["updated"] = now
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "node_groups",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "updated"},
			mariadb.Mapping{To: "group_id", From: "gid"},
			mariadb.Mapping{To: "group_name", From: "groupname"},
		},
		Keys: []string{"node_id", "group_id"},
		Data: data,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM node_groups WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobSystem) users() error {
	data, ok := d.data["uids"].([]any)
	if !ok {
		slog.Warn("unsupported system users data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported system users entry format")
			return nil
		}
		line["node_id"] = nodeID
		line["updated"] = now
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "node_users",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "updated"},
			mariadb.Mapping{To: "user_id", From: "uid"},
			mariadb.Mapping{To: "user_name", From: "username"},
		},
		Keys: []string{"node_id", "user_id"},
		Data: data,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM node_users WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobSystem) hardware() error {
	data, ok := d.data["hardware"].([]any)
	if !ok {
		slog.Warn("unsupported system hardware data format")
		return nil
	}
	nodeID := d.nodeID
	now := d.now
	for i, _ := range data {
		line, ok := data[i].(map[string]any)
		if !ok {
			slog.Warn("unsupported system hardware entry format")
			return nil
		}
		line["node_id"] = nodeID
		line["updated"] = now
		data[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "node_hw",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "hw_type", From: "type"},
			mariadb.Mapping{To: "hw_path", From: "path"},
			mariadb.Mapping{To: "hw_class", From: "class"},
			mariadb.Mapping{To: "hw_description", From: "description", Modify: mariadb.ModifierMaxLen(128)},
			mariadb.Mapping{To: "hw_driver", From: "driver"},
			mariadb.Mapping{To: "updated"},
		},
		Keys: []string{"node_id", "hw_type", "hw_path"},
		Data: data,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM node_hw WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobSystem) properties() error {
	data, ok := d.data["properties"].(map[string]any)
	if !ok {
		slog.Warn("unsupported system properties format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	data["node_id"] = map[string]any{"value": nodeID}
	data["updated"] = map[string]any{"value": now}

	get := func(v any) (any, error) {
		keyData, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unsupported system property format: %#v", v)
		}
		value, ok := keyData["value"]
		if !ok {
			return nil, fmt.Errorf("key 'value' not found in property %#v", v)
		}
		return value, nil
	}

	request := mariadb.InsertOrUpdate{
		Table: "nodes",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "asset_env", Get: get, Optional: true},
			mariadb.Mapping{To: "bios_version", Get: get},
			mariadb.Mapping{To: "cluster_id", Get: get},
			mariadb.Mapping{To: "connect_to", Get: get, Optional: true},
			mariadb.Mapping{To: "cpu_cores", Get: get},
			mariadb.Mapping{To: "cpu_dies", Get: get},
			mariadb.Mapping{To: "cpu_freq", Get: get},
			mariadb.Mapping{To: "cpu_model", Get: get},
			mariadb.Mapping{To: "cpu_threads", Get: get},
			mariadb.Mapping{To: "enclosure", Get: get},
			mariadb.Mapping{To: "fqdn", Get: get},
			mariadb.Mapping{To: "last_boot", Get: get, Modify: mariadb.ModifyDatetime},
			mariadb.Mapping{To: "listener_port", Get: get},
			mariadb.Mapping{To: "loc_addr", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_building", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_city", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_country", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_floor", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_rack", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_room", Get: get, Optional: true},
			mariadb.Mapping{To: "loc_zip", Get: get, Optional: true},
			mariadb.Mapping{To: "manufacturer", Get: get},
			mariadb.Mapping{To: "mem_banks", Get: get},
			mariadb.Mapping{To: "mem_bytes", Get: get},
			mariadb.Mapping{To: "mem_slots", Get: get},
			mariadb.Mapping{To: "model", Get: get},
			mariadb.Mapping{To: "node_id", Get: get},
			mariadb.Mapping{To: "node_env", Get: get},
			mariadb.Mapping{To: "nodename", Get: get},
			mariadb.Mapping{To: "os_arch", Get: get},
			mariadb.Mapping{To: "os_kernel", Get: get},
			mariadb.Mapping{To: "os_name", Get: get},
			mariadb.Mapping{To: "os_vendor", Get: get},
			mariadb.Mapping{To: "sec_zone", Get: get, Optional: true},
			mariadb.Mapping{To: "serial", Get: get},
			mariadb.Mapping{To: "sp_version", Get: get},
			mariadb.Mapping{To: "team_integ", Get: get, Optional: true},
			mariadb.Mapping{To: "team_support", Get: get, Optional: true},
			mariadb.Mapping{To: "tz", Get: get},
			mariadb.Mapping{To: "updated", Get: get},
			mariadb.Mapping{To: "version", Get: get, Modify: mariadb.ModifierMaxLen(20)},
		},
		Keys: []string{"node_id"},
		Data: data,
	}

	_, err := request.QueryContext(d.ctx, d.db)

	return err
}

func (d *jobSystem) Operations() []operation {
	hasProp := func(s string) func() bool {
		return func() bool {
			_, ok := d.data[s]
			return ok
		}
	}
	return []operation{
		{desc: "handleSystem/dropPending", do: d.dropPending},
		{desc: "handleSystem/getData", do: d.getData},
		{desc: "handleSystem/dbNow", do: d.dbNow},
		{desc: "handleSystem/hardware", do: d.hardware, skipOp: hasProp("hardware")},
		{desc: "handleSystem/properties", do: d.properties, skipOp: hasProp("properties")},
		{desc: "handleSystem/groups", do: d.groups, skipOp: hasProp("gids")},
		{desc: "handleSystem/users", do: d.users, skipOp: hasProp("uids")},
		{desc: "handleSystem/lan", do: d.lan, skipOp: hasProp("lan")},
		{desc: "handleSystem/hba", do: d.hba, skipOp: hasProp("hba")},
		{desc: "handleSystem/targets", do: d.targets, skipOp: hasProp("targets")},
	}
}

func (d *jobSystem) dropPending() error {
	if err := d.redis.HDel(d.ctx, cache.KeyDaemonSystemPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", cache.KeyDaemonSystemPending, d.nodeID, err)
	}
	return nil
}

func (d *jobSystem) getData() error {
	cmd := d.redis.HGet(d.ctx, cache.KeyDaemonSystemHash, d.nodeID)
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
	for k := range d.data {
		switch k {
		case "hardware":
		case "properties":
		case "gids":
		case "uids":
		case "lan":
		case "hba":
		case "targets":
		default:
			slog.Info(fmt.Sprintf("parse data: ignore key '%s'", k))
		}
	}
	return nil
}
