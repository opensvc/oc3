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

func (t *Worker) handleSystemTargets(ctx context.Context, nodeID string, i any, now string) error {
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
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("updated"),
			mariadb.NewNaturalMapping("hba_id"),
			mariadb.NewNaturalMapping("tgt_id"),
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

func (t *Worker) handleSystemHBA(ctx context.Context, nodeID string, i any, now string) error {
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
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("updated"),
			mariadb.NewNaturalMapping("hba_id"),
			mariadb.NewNaturalMapping("hba_type"),
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

func (t *Worker) handleSystemLAN(ctx context.Context, nodeID string, i any, now string) error {
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
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("updated"),
			mariadb.NewNaturalMapping("mac"),
			mariadb.NewNaturalMapping("intf"),
			mariadb.NewNaturalMapping("type"),
			mariadb.NewNaturalMapping("addr"),
			mariadb.NewNaturalMapping("mask"),
			mariadb.NewNaturalMapping("flag_deprecated"),
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

func (t *Worker) handleSystemGroups(ctx context.Context, nodeID string, i any, now string) error {
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
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("updated"),
			mariadb.NewMapping("group_id", "gid"),
			mariadb.NewMapping("group_name", "groupname"),
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

func (t *Worker) handleSystemUsers(ctx context.Context, nodeID string, i any, now string) error {
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
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("updated"),
			mariadb.NewMapping("user_id", "uid"),
			mariadb.NewMapping("user_name", "username"),
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

func (t *Worker) handleSystemHardware(ctx context.Context, nodeID string, i any, now string) error {
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
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewMapping("hw_type", "type"),
			mariadb.NewMapping("hw_path", "path"),
			mariadb.NewMapping("hw_class", "class"),
			mariadb.NewMapping("hw_description", "description"),
			mariadb.NewMapping("hw_driver", "driver"),
			mariadb.NewNaturalMapping("updated"),
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

func (t *Worker) handleSystemProperties(ctx context.Context, nodeID string, i any, now string) error {
	data, ok := i.(map[string]any)
	if !ok {
		slog.Warn("unsupported system properties format")
		return nil
	}

	data["node_id"] = map[string]any{"value": nodeID}
	data["updated"] = map[string]any{"value": now}

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
			mariadb.NewNaturalMapping("updated"),
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

	_, err := request.QueryContext(ctx, t.DB)

	return err
}

func (t *Worker) handleSystem(nodeID string) error {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Second*5)

	cmd := t.Redis.HGet(ctx, cache.KeySystemHash, nodeID)
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
