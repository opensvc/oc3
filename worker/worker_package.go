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

	var data map[string]any
	if err := json.Unmarshal(result, &data); err != nil {
		slog.Error(fmt.Sprintf("unmarshalled data: %#v\n", data))
		return err
	}

	if _, ok := data[`packages`]; !ok {
		slog.Warn(fmt.Sprint("unsupported json format for packages"))
		return nil
	}

	if _, ok := data[`keys`]; !ok {
		slog.Warn(fmt.Sprint("missing package keys"))
		return nil
	}

	pkgList, ok := data[`packages`].([]any)
	if !ok {
		slog.Warn(fmt.Sprint("unsupported package table format"))
		return nil
	}

	keys, ok := data[`keys`].([]interface{})
	if !ok {
		slog.Warn(fmt.Sprint("unsupported key table format"))
		return nil
	}

	for i := range pkgList {
		pkg, ok := pkgList[i].([]interface{})
		if !ok {
			slog.Warn(fmt.Sprint("unsupported package entry format"))
			return nil
		}

		if len(keys) != len(pkg) {
			slog.Warn(fmt.Sprintf("index of package out of range, %d keys for %d package elements", len(keys), len(pkg)))
			return nil
		}
		line := make(map[string]any)

		for e, v := range pkg {
			line["node_id"] = nodeID
			line["pkg_updated"] = mariadb.Raw("NOW()")
			key := keys[e].(string)
			if !ok {
				slog.Warn(fmt.Sprint("unsupported key entry format"))
				return nil
			}
			line[key] = v
		}
		pkgList[i] = line
	}

	request := mariadb.InsertOrUpdate{
		Table: "packages",
		Mappings: mariadb.Mappings{
			mariadb.NewNaturalMapping("node_id"),
			mariadb.NewNaturalMapping("pkg_updated"),
			mariadb.NewNaturalMapping("pkg_name"),
			mariadb.NewNaturalMapping("pkg_version"),
			mariadb.NewNaturalMapping("pkg_arch"),
			mariadb.NewNaturalMapping("pkg_type"),
			mariadb.NewNaturalMapping("pkg_sig"),
			mariadb.NewNaturalMapping("pkg_install_date"),
		},
		Keys: []string{`node_id`, `pkg_name`, `pkg_arch`, `pkg_version`, `pkg_type`},
		Data: pkgList,
	}

	_, err = request.Query(t.DB)
	if err != nil {
		return err
	}

	return nil
}
