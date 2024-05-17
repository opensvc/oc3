package apihandlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"
)

func (a *Api) pushUniqValue(ctx context.Context, key string, value string) error {
	s := fmt.Sprintf("LPOS %s %s", key, value)
	slog.Info(s)
	_, err := a.Redis.LPos(ctx, key, value, redis.LPosArgs{}).Result()
	switch err {
	case nil:
		// already in queue
		return nil
	case redis.Nil:
		// not in list try push
		s = fmt.Sprintf("LPUSH %s %s", key, value)
		slog.Info(s)
		if _, err := a.Redis.LPush(ctx, key, value).Result(); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		return nil
	default:
		return fmt.Errorf("%s: %w", s, err)
	}
}

// pushNotPending is alternate version of pushUniqValue that may be more efficient:
//
// pendingKey is a hash on elements for queueKey.
// Consumers of queueKey must remove pendingKey element when it pops a queueKeyn element.
//
// It uses HGET O(1) instead of LPOS O(n).
// LPOS requires redis 6.0.6,
func (a *Api) pushNotPending(ctx context.Context, pendingKey, queueKey string, value string) error {
	s := fmt.Sprintf("HGET %s %s", pendingKey, value)
	slog.Info(s)
	_, err := a.Redis.HGet(ctx, pendingKey, value).Result()
	switch err {
	case nil:
		// already in list
		return nil
	case redis.Nil:
		// not in try push
		s = fmt.Sprintf("HSET %s %s %s", pendingKey, value, value)
		slog.Info(s)
		if _, err := a.Redis.HSet(ctx, pendingKey, value, value).Result(); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		s = fmt.Sprintf("LPUSH %s %s", queueKey, value)
		slog.Info(s)
		if _, err := a.Redis.LPush(ctx, queueKey, value).Result(); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
		return nil
	default:
		return fmt.Errorf("%s: %w", s, err)
	}
}
