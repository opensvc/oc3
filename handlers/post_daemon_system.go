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

	s := fmt.Sprintf("HSET %s %s", cache.KeySystemHash, key)
	slog.Info(s)
	if _, err := a.Redis.HSet(reqCtx, cache.KeySystemHash, key, string(b)).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		slog.Error(s)
		return JSONProblem(c, http.StatusInternalServerError, "", s)
	}

	s = fmt.Sprintf("LPOS %s %s", cache.KeySystem, key)
	slog.Info(s)
	if _, err := a.Redis.LPos(reqCtx, cache.KeySystem, key, redis.LPosArgs{}).Result(); err != nil {
		switch err {
		case nil:
		case redis.Nil:
			s = fmt.Sprintf("LPUSH %s %s", cache.KeySystem, key)
			slog.Info(s)
			if _, err := a.Redis.LPush(reqCtx, cache.KeySystem, key).Result(); err != nil {
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
