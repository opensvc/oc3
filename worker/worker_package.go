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

func (t *Worker) handlePackage(nodeID string) error {
	_, err := t.Redis.HDel(context.Background(), cache.KeyPackagesPending, nodeID).Result()
	if err != nil {
		slog.Error(fmt.Sprintf("can't HDEL %s: %s", cache.KeyPackagesPending, nodeID))
		return err
	}

	cmd := t.Redis.HGet(context.Background(), cache.KeyPackagesHash, nodeID)
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

	if _, ok := data["packages"]; !ok {
		slog.Warn(fmt.Sprint(`unsupported json format for packages, must be in the following format :\n
			{ "packages": [ { "name": "foo", "version": "3.1", "arch": "amd64", }, ... ], }`))
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
		line["pkg_updated"] = mariadb.Raw("NOW()")
		pkgList[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "packages",
		Mappings: mariadb.Mappings{
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("pkg_updated"),
			mariadb.NewMapping("pkg_name", "name"),
			mariadb.NewMapping("pkg_version", "version"),
			mariadb.NewMapping("pkg_arch", "arch"),
			mariadb.NewMapping("pkg_type", "type"),
			mariadb.NewMapping("pkg_sig", "sig"),
			mariadb.NewMapping("pkg_install_date", "install_date"),
		},
		Keys: []string{"node_id", "pkg_name", "pkg_arch", "pkg_version", "pkg_type"},
		Data: pkgList,
	}

	_, err = request.Query(t.DB)
	return err
}
