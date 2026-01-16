package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type (
	JobRedis struct {
		redis *redis.Client

		// cachePendingH is the cache hash used by BaseJob.dropPending:
		// HDEL <cachePendingH> <cachePendingIDX>
		cachePendingH string

		// cachePendingIDX is the cache id used by BaseJob.dropPending:
		// HDEL <cachePendingH> <cachePendingIDX>
		cachePendingIDX string
	}
)

func (j *JobRedis) SetRedis(r *redis.Client) {
	j.redis = r
}

// populateFeedObjectConfigForClusterIDH HSET FeedObjectConfigForClusterIDH <clusterID> with the names of objects
// without a config or HDEL FeedObjectConfigForClusterIDH <clusterID> if there are no missing configs.
func (d *JobRedis) populateFeedObjectConfigForClusterIDH(ctx context.Context, clusterID string, byObjectID map[string]*cdb.DBObject) ([]string, error) {
	needConfig := make(map[string]struct{})
	for _, obj := range byObjectID {
		if obj.NullConfig {
			objName := obj.Svcname
			// TODO: import om3 naming ?
			if strings.Contains(objName, "/svc/") ||
				strings.Contains(objName, "/vol/") ||
				strings.HasPrefix(objName, "svc/") ||
				strings.HasPrefix(objName, "vol/") ||
				!strings.Contains(objName, "/") {
				needConfig[objName] = struct{}{}
			}
		}
	}

	keyName := cachekeys.FeedObjectConfigForClusterIDH

	if len(needConfig) > 0 {
		l := make([]string, 0, len(needConfig))
		for k := range needConfig {
			l = append(l, k)
		}
		if err := d.redis.HSet(ctx, keyName, clusterID, strings.Join(l, " ")).Err(); err != nil {
			return l, fmt.Errorf("populateFeedObjectConfigForClusterIDH: HSet %s %s: %w", keyName, clusterID, err)
		}
		return l, nil
	} else {
		if err := d.redis.HDel(ctx, keyName, clusterID).Err(); err != nil {
			return nil, fmt.Errorf("populateFeedObjectConfigForClusterIDH: HDEL %s %s: %w", keyName, clusterID, err)
		}
	}
	return nil, nil
}

func (j *JobRedis) dropPending(ctx context.Context) error {
	if err := j.redis.HDel(ctx, j.cachePendingH, j.cachePendingIDX).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", j.cachePendingH, j.cachePendingIDX, err)
	}
	return nil
}
