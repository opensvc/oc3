package handlers

import (
	"fmt"
	"io"
	"log/slog"
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
		return JSONProblemf(c, http.StatusInternalServerError, "", "read request body: %s", err)
	}

	reqCtx := c.Request().Context()

	s := fmt.Sprintf("HSET %s %s", cache.KeyAssetHash, key)
	slog.Info(s)
	if _, err := a.Redis.HSet(reqCtx, cache.KeyAssetHash, key, string(b)).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		slog.Error(s)
		return JSONProblem(c, http.StatusInternalServerError, "", s)
	}

	s = fmt.Sprintf("LPOS %s %s", cache.KeyAsset, key)
	slog.Info(s)
	if _, err := a.Redis.LPos(reqCtx, cache.KeyAsset, key, redis.LPosArgs{}).Result(); err != nil {
		switch err {
		case nil:
		case redis.Nil:
			s = fmt.Sprintf("LPUSH %s %s", cache.KeyAsset, key)
			slog.Info(s)
			if _, err := a.Redis.LPush(reqCtx, cache.KeyAsset, key).Result(); err != nil {
				s := fmt.Sprintf("%s: %s", s, err)
				slog.Error(s)
				return JSONProblemf(c, http.StatusInternalServerError, "", "%s", s)
			}
		default:
			s := fmt.Sprintf("%s: %s", s, err)
			slog.Error(s)
			return JSONProblemf(c, http.StatusInternalServerError, "", "%s", s)
		}

	}

	return c.NoContent(http.StatusNoContent)
}
