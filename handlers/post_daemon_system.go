package handlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cache"
)

func (a *Api) PostDaemonSystem(c echo.Context) error {
	key := nodeIDFromContext(c)
	if key == "" {
		return JSONNodeAuthProblem(c)
	}

	b, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}

	reqCtx := c.Request().Context()

	if _, err := a.Redis.HSet(reqCtx, cache.KeyAssetHash, key, string(b)).Result(); err != nil {
		return fmt.Errorf("HSET %s %s: %w", cache.KeyAssetHash, key, err)
	}
	if position, err := a.Redis.LPos(reqCtx, cache.KeyAsset, key, redis.LPosArgs{}).Result(); err != nil {
		return fmt.Errorf("LPOS %s %s: %w", cache.KeyAsset, key, err)
	} else if position < 0 {
		if _, err := a.Redis.LPush(reqCtx, cache.KeyAsset, key).Result(); err != nil {
			return fmt.Errorf("LPUSH %s %s: %w", cache.KeyAsset, key, err)
		}
		return err
	}

	return c.NoContent(http.StatusNoContent)
}
