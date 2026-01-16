package apihandlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

// {
//   "action": "thaw",
//   "argv": [
//     "foo",
//     "thaw",
//     "--local"
//   ],
//   "begin": "2026-01-12 10:57:12",
//   "cron": false,
//   "path": "foo",
//   "session_uuid": "b9d795bc-498e-4c20-aada-9feec2eaa947",
//   "version": "2.1-1977"
// }

// PostFeedActionBegin handles POST /action/begin
func (a *Api) PostFeedActionBegin(c echo.Context) error {
	keyH := cachekeys.FeedActionBeginH
	keyQ := cachekeys.FeedActionBeginQ
	keyPendingH := cachekeys.FeedActionBeginPendingH

	log := getLog(c)

	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	ClusterID := clusterIDFromContext(c)
	if ClusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "Refused", "authenticated node doesn't define cluster id")
	}

	var payload api.PostFeedActionBeginJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}

	if !strings.HasPrefix(payload.Version, "2.") && !strings.HasPrefix(payload.Version, "3.") {
		log.Error(fmt.Sprintf("unexpected version %s", payload.Version))
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unsupported data client version: %s", payload.Version)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}

	reqCtx := c.Request().Context()

	idx := fmt.Sprintf("%s@%s@%s:%s", payload.Path, nodeID, ClusterID, payload.Action)

	s := fmt.Sprintf("HSET %s %s", keyH, idx)
	if _, err := a.Redis.HSet(reqCtx, keyH, idx, b).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		log.Error(s)
		return JSONProblem(c, http.StatusInternalServerError, "", s)
	}

	if err := a.pushNotPending(reqCtx, keyPendingH, keyQ, idx); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", keyQ, idx, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", keyQ, idx, err)
	}

	log.Debug("action begin accepted")
	return c.NoContent(http.StatusAccepted)
}
