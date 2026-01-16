package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/mariadb"
)

type (
	jobFeedSystem struct {
		JobBase
		JobRedis
		JobDB

		nodeID string
		data   map[string]any
	}
)

func newDaemonSystem(nodeID string) *jobFeedSystem {
	return &jobFeedSystem{
		JobBase: JobBase{
			name:   "daemonSystem",
			detail: "nodeID: " + nodeID,
		},
		JobRedis: JobRedis{
			cachePendingH:   cachekeys.FeedSystemPendingH,
			cachePendingIDX: nodeID,
		},
		nodeID: nodeID,
	}
}

func (d *jobFeedSystem) Operations() []operation {
	hasProp := func(s string) func() bool {
		return func() bool {
			_, ok := d.data[s]
			return ok
		}
	}
	return []operation{
		{desc: "system/dropPending", doCtx: d.dropPending},
		{desc: "system/getData", doCtx: d.getData},
		{desc: "system/dbNow", doCtx: d.dbNow},
		{desc: "system/hardware", doCtx: d.hardware, condition: hasProp("hardware"), blocking: true},
		{desc: "system/properties", doCtx: d.properties, condition: hasProp("properties"), blocking: true},
		{desc: "system/groups", doCtx: d.groups, condition: hasProp("gids"), blocking: true},
		{desc: "system/users", doCtx: d.users, condition: hasProp("uids"), blocking: true},
		{desc: "system/lan", doCtx: d.lan, condition: hasProp("lan"), blocking: true},
		{desc: "system/hba", doCtx: d.hba, condition: hasProp("hba"), blocking: true},
		{desc: "system/targets", doCtx: d.targets, condition: hasProp("targets"), blocking: true},
		{desc: "system/package", doCtx: d.pkg, condition: hasProp("package"), blocking: true},
	}
}

func (d *jobFeedSystem) pkg(ctx context.Context) error {
	pkgList, ok := d.data["package"].([]any)
	if !ok {
		slog.Warn(fmt.Sprint("unsupported json format for packages"))
		return nil
	}
	nodeID := d.nodeID
	now := d.now

	for i := range pkgList {
		line, ok := pkgList[i].(map[string]any)
		if !ok {
			slog.Warn(fmt.Sprint("unsupported package entry format"))
			return nil
		}
		line["node_id"] = nodeID
		line["pkg_updated"] = now
		pkgList[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "packages",
		Mappings: mariadb.Mappings{
			mariadb.Mapping{To: "node_id"},
			mariadb.Mapping{To: "pkg_updated"},
			mariadb.Mapping{To: "pkg_name", From: "name"},
			mariadb.Mapping{To: "pkg_version", From: "version"},
			mariadb.Mapping{To: "pkg_arch", From: "arch"},
			mariadb.Mapping{To: "pkg_type", From: "type"},
			mariadb.Mapping{To: "pkg_sig", From: "sig"},
			mariadb.Mapping{To: "pkg_install_date", From: "installed_at", Modify: mariadb.ModifyFromRFC3339},
		},
		Keys: []string{"node_id", "pkg_name", "pkg_arch", "pkg_version", "pkg_type"},
		Data: pkgList,
	}

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM packages WHERE node_id = ? AND pkg_updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}
	if err := d.oDb.DashboardUpdatePkgDiffForNode(ctx, nodeID); err != nil {
		return err
	}
	return nil
}

func (d *jobFeedSystem) targets(ctx context.Context) error {
	data, ok := d.data["targets"].([]any)
	if !ok {
		slog.Warn("unsupported system targets data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i := range data {
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

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM stor_zone WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}

	return nil
}

func (d *jobFeedSystem) hba(ctx context.Context) error {
	data, ok := d.data["hba"].([]any)
	if !ok {
		slog.Warn("unsupported system hba data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i := range data {
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

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM node_hba WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}

	return nil
}

func (d *jobFeedSystem) lan(ctx context.Context) error {
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

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM node_ip WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}

	return nil
}

func (d *jobFeedSystem) groups(ctx context.Context) error {
	data, ok := d.data["gids"].([]any)
	if !ok {
		slog.Warn("unsupported system groups data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i := range data {
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

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM node_groups WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}

	return nil
}

func (d *jobFeedSystem) users(ctx context.Context) error {
	data, ok := d.data["uids"].([]any)
	if !ok {
		slog.Warn("unsupported system users data format")
		return nil
	}

	nodeID := d.nodeID
	now := d.now
	for i := range data {
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

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM node_users WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}

	return nil
}

func (d *jobFeedSystem) hardware(ctx context.Context) error {
	data, ok := d.data["hardware"].([]any)
	if !ok {
		slog.Warn("unsupported system hardware data format")
		return nil
	}
	nodeID := d.nodeID
	now := d.now
	for i := range data {
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

	if _, err := request.QueryContext(ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(ctx, "DELETE FROM node_hw WHERE node_id = ? AND updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer func() { _ = rows.Close() }()
	}

	return nil
}

func (d *jobFeedSystem) properties(ctx context.Context) error {
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
			mariadb.Mapping{To: "last_boot", Get: get, Modify: mariadb.ModifyFromRFC3339},
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

	_, err := request.QueryContext(ctx, d.db)

	return err
}

func (d *jobFeedSystem) getData(ctx context.Context) error {
	cmd := d.redis.HGet(ctx, cachekeys.FeedSystemH, d.nodeID)
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
		case "package":
		default:
			slog.Info(fmt.Sprintf("parse data: ignore key '%s'", k))
		}
	}
	return nil
}
