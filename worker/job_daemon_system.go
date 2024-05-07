package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cache"
	"github.com/opensvc/oc3/mariadb"
)

func (t *Worker) handleSystemTargets(ctx context.Context, nodeID string, i any, now time.Time) error {
	data, ok := i.([]any)
	if !ok {
		slog.Warn("unsupported system targets data format")
		return nil
	}
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

	if _, err := request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM stor_zone WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (t *Worker) handleSystemHBA(ctx context.Context, nodeID string, i any, now time.Time) error {
	data, ok := i.([]any)
	if !ok {
		slog.Warn("unsupported system hba data format")
		return nil
	}
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

	if _, err := request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM node_hba WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (t *Worker) handleSystemLAN(ctx context.Context, nodeID string, i any, now time.Time) error {
	var l []any
	data, ok := i.(map[string]any)
	if !ok {
		slog.Warn("unsupported system lan data format")
		return nil
	}
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

	if _, err := request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM node_ip WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (t *Worker) handleSystemGroups(ctx context.Context, nodeID string, i any, now time.Time) error {
	data, ok := i.([]any)
	if !ok {
		slog.Warn("unsupported system groups data format")
		return nil
	}

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

	if _, err := request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM node_groups WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (t *Worker) handleSystemUsers(ctx context.Context, nodeID string, i any, now time.Time) error {
	data, ok := i.([]any)
	if !ok {
		slog.Warn("unsupported system users data format")
		return nil
	}

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

	if _, err := request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM node_users WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (t *Worker) handleSystemHardware(ctx context.Context, nodeID string, i any, now time.Time) error {
	data, ok := i.([]any)
	if !ok {
		slog.Warn("unsupported system hardware data format")
		return nil
	}

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

	if _, err := request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM node_hw WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (t *Worker) handleSystemProperties(ctx context.Context, nodeID string, i any, now time.Time) error {
	data, ok := i.(map[string]any)
	if !ok {
		slog.Warn("unsupported system properties format")
		return nil
	}

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

	_, err := request.QueryContext(ctx, t.DB)

	return err
}

func (t *Worker) handleSystem(nodeID string) error {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Second*5)

	if err := t.Redis.HDel(ctx, cache.KeyDaemonSystemPending, nodeID).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", cache.KeyDaemonSystemPending, nodeID, err)
	}

	cmd := t.Redis.HGet(ctx, cache.KeyDaemonSystemHash, nodeID)
	result, err := cmd.Result()
	switch err {
	case nil:
	case redis.Nil:
		return nil
	default:
		return fmt.Errorf("system: HGET: %w", err)
	}

	var v map[string]any
	if err := json.Unmarshal([]byte(result), &v); err != nil {
		return fmt.Errorf("system: unmarshal: %w", err)
	}

	now, err := mariadb.Now(ctx, t.DB)
	if err != nil {
		return fmt.Errorf("system: now: %w", err)
	}

	for k, i := range v {
		switch k {
		case "hardware":
			err = t.handleSystemHardware(ctx, nodeID, i, now)
		case "properties":
			err = t.handleSystemProperties(ctx, nodeID, i, now)
		case "gids":
			err = t.handleSystemGroups(ctx, nodeID, i, now)
		case "uids":
			err = t.handleSystemUsers(ctx, nodeID, i, now)
		case "lan":
			err = t.handleSystemLAN(ctx, nodeID, i, now)
		case "hba":
			err = t.handleSystemHBA(ctx, nodeID, i, now)
		case "targets":
			err = t.handleSystemTargets(ctx, nodeID, i, now)
		default:
			slog.Info(fmt.Sprintf("system: %s: ignore key '%s'", nodeID, k))
		}
		if err != nil {
			slog.Warn(fmt.Sprintf("system: %s: %s: %s", nodeID, k, err))
		} else {
			slog.Info(fmt.Sprintf("system: %s: %s", nodeID, k))
		}
	}
	return nil
}
