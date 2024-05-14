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
	jobPackage struct {
		*BaseJob

		nodeID string
		data   map[string][]any
	}
)

func newJobPackage(nodeID string) *jobPackage {
	return &jobPackage{
		BaseJob: &BaseJob{
			name:   "package",
			detail: "nodeID: " + nodeID,
		},
		nodeID: nodeID,
	}
}

func (d *jobPackage) Operations() []operation {
	return []operation{
		{desc: "package/dropPending", do: d.dropPending},
		{desc: "package/getData", do: d.getData},
		{desc: "package/dbNow", do: d.dbNow},
		{desc: "package/dbUpdate", do: d.dbUpdate},
	}
}

func (d *jobPackage) dbUpdate() error {
	pkgList, ok := d.data["packages"]
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
			mariadb.Mapping{To: "pkg_install_date", From: "installed_at", Modify: mariadb.ModifyDatetime},
		},
		Keys: []string{"node_id", "pkg_name", "pkg_arch", "pkg_version", "pkg_type"},
		Data: pkgList,
	}

	if _, err := request.QueryContext(d.ctx, d.db); err != nil {
		return err
	}

	if rows, err := d.db.QueryContext(d.ctx, "DELETE FROM packages WHERE node_id = ? AND pkg_updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}

func (d *jobPackage) dropPending() error {
	if err := d.redis.HDel(d.ctx, cache.KeyPackagesPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", cache.KeyDaemonSystemPending, d.nodeID, err)
	}
	return nil
}

func (d *jobPackage) getData() error {
	cmd := d.redis.HGet(d.ctx, cache.KeyPackagesHash, d.nodeID)
	result, err := cmd.Bytes()
	switch err {
	case nil:
	case redis.Nil:
		return nil
	default:
		return err
	}

	if err := json.Unmarshal(result, &d.data); err != nil {
		slog.Error(fmt.Sprintf("unmarshalled data: %#v\n", d.data))
		return err
	}
	return nil
}
