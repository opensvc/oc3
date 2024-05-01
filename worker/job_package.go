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

func (t *Worker) handlePackage(nodeID string) error {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Second*5)

	_, err := t.Redis.HDel(ctx, cache.KeyPackagesPending, nodeID).Result()
	if err != nil {
		slog.Error(fmt.Sprintf("can't HDEL %s: %s", cache.KeyPackagesPending, nodeID))
		return err
	}

	cmd := t.Redis.HGet(ctx, cache.KeyPackagesHash, nodeID)
	result, err := cmd.Bytes()
	switch err {
	case nil:
	case redis.Nil:
		return nil
	default:
		return err
	}

	var data map[string][]any
	if err := json.Unmarshal(result, &data); err != nil {
		slog.Error(fmt.Sprintf("unmarshalled data: %#v\n", data))
		return err
	}

	now, err := mariadb.Now(ctx, t.DB)
	if err != nil {
		return fmt.Errorf("system: now: %w", err)
	}

	if _, ok := data["packages"]; !ok {
		slog.Warn(fmt.Sprint("unsupported json format for packages"))
		return nil
	}

	pkgList := data["packages"]

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
			mariadb.Mapping{To: "pkg_install_date", From: "install_date"},
		},
		Keys: []string{"node_id", "pkg_name", "pkg_arch", "pkg_version", "pkg_type"},
		Data: pkgList,
	}

	if _, err = request.QueryContext(ctx, t.DB); err != nil {
		return err
	}

	if rows, err := t.DB.QueryContext(ctx, "DELETE FROM packages WHERE node_id = ? AND pkg_updated < ?", nodeID, now); err != nil {
		return err
	} else {
		defer rows.Close()
	}

	return nil
}
