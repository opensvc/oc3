package apihandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

// {
//     "uuid": "ea9a8373-3dda-4fe7-8c4b-08f5290c6c8b",
//     "path": "foo",
//     "action": "thaw",
//     "begin": "2026-01-16 15:05:00",
//     "end": "2026-01-16 15:06:00",
//     "cron": false,
//     "session_uuid": "7f2df7b2-8a4a-4bc1-9a8b-03acffaacd45",
//     "actionlogfile": "/var/tmp/opensvc/foo.freezeupdfl98l",
//     "status": "ok"
// }

// PutFeedActionEnd handles PUT /feed/action
func (a *Api) PutFeedActionEnd(c echo.Context) error {
	keyH := cachekeys.FeedActionH
	keyQ := cachekeys.FeedActionQ
	keyPendingH := cachekeys.FeedActionPendingH

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

	var payload api.PutFeedActionEndJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}

	reqCtx := c.Request().Context()

	idx := fmt.Sprintf("%s@%s@%s:%s", payload.Path, nodeID, ClusterID, payload.Uuid)

	// if action is pending, update the stored begin action with end info to avoid begin and end processing
	if n, err := a.Redis.HExists(reqCtx, keyPendingH, idx).Result(); err == nil && n {
		if currentBytes, err := a.Redis.HGet(reqCtx, keyH, idx).Bytes(); err == nil {
			var currentAction api.Action
			if err := json.Unmarshal(currentBytes, &currentAction); err == nil {
				currentAction.End = payload.End
				currentAction.Status = payload.Status
				currentAction.Actionlogfile = payload.Actionlogfile

				if updatedBytes, err := json.Marshal(currentAction); err == nil {
					b = updatedBytes
				}
			}
		}
	}

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

	log.Debug("action end accepted")
	return c.NoContent(http.StatusAccepted)
}
