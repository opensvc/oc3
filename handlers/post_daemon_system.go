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
	var nodeUUID, nodeName string
	key := fmt.Sprintf("%s:%s", nodeUUID, nodeName)
	b, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}

	reqCtx := c.Request().Context()
	redCli := cache.ClientFromContext(c)

	if _, err := redCli.HSet(reqCtx, cache.KeyAssetHash, key, string(b)).Result(); err != nil {
		return fmt.Errorf("HSET %s %s: %w", cache.KeyAssetHash, key, err)
	}
	if position, err := redCli.LPos(reqCtx, cache.KeyAsset, key, redis.LPosArgs{}).Result(); err != nil {
		return fmt.Errorf("LPOS %s %s: %w", cache.KeyAsset, key, err)
	} else if position < 0 {
		if _, err := redCli.LPush(reqCtx, cache.KeyAsset, key).Result(); err != nil {
			return fmt.Errorf("LPUSH %s %s: %w", cache.KeyAsset, key, err)
		}
		return err
	}

	return c.NoContent(http.StatusNoContent)
}
