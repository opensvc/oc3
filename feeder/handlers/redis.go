package feederhandlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"
)

// pushNotPending is an alternate version of pushUniqValue that may be more efficient:
//
// pendingKey is a hash on elements for queueKey.
// Consumers of queueKey must remove the pendingKey element when it pops a queueKey element.
//
// It uses HGET O(1) instead of LPOS O(n).
// LPOS requires redis 6.0.6,
func (a *Api) pushNotPending(ctx context.Context, log *slog.Logger, pendingKey, queueKey string, value string) error {
	log.Debug("pushNotPending HGet pendingKey")
	_, err := a.Redis.HGet(ctx, pendingKey, value).Result()
	switch err {
	case nil:
		// already in the list
		return nil
	case redis.Nil:
		// not in try push
		log.Debug("pushNotPending HSet pendingKey")
		if _, err := a.Redis.HSet(ctx, pendingKey, value, value).Result(); err != nil {
			return fmt.Errorf("Hset pendingKey: %w", err)
		}
		log.Debug("pushNotPending LPush queueKey")
		if _, err := a.Redis.LPush(ctx, queueKey, value).Result(); err != nil {
			return fmt.Errorf("LPush queueKey: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("HGet pendingKey: %w", err)
	}
}
