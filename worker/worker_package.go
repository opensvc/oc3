package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/opensvc/oc3/cache"
	"github.com/opensvc/oc3/mariadb"
	"log/slog"
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
		slog.Error(fmt.Sprintf("unmarshaled data: %#v\n", data))
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

	line := make(map[string]any)

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

		for e, v := range pkg {
			line["node_id"] = nodeID
			line["updated"] = mariadb.Raw("NOW()")
			key := keys[e].(string)
			if !ok {
				slog.Warn(fmt.Sprint("unsupported key entry format"))
				return nil
			}
			line[key] = v
		}
		pkgList[i] = line
	}

	return err
}
