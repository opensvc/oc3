package apihandlers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	redis "github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
)

func (a *Api) getObjectConfigToFeed(ctx context.Context, clusterID string) ([]string, error) {
	keyName := cachekeys.FeedObjectConfigForClusterIDH
	s, err := a.Redis.HGet(ctx, keyName, clusterID).Result()
	if err == nil || errors.Is(err, redis.Nil) {
		return strings.Fields(s), nil
	}
	return nil, fmt.Errorf("HGET %s %s: %s", keyName, clusterID, err)
}

func (a *Api) removeObjectConfigToFeed(ctx context.Context, clusterID string) error {
	keyName := cachekeys.FeedObjectConfigForClusterIDH
	if err := a.Redis.HDel(ctx, keyName, clusterID).Err(); err != nil {
		return fmt.Errorf("HDEL %s %s: %s", keyName, clusterID, err)
	}
	return nil
}
