package apifeederhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	api "github.com/opensvc/oc3/apifeeder"
	"github.com/opensvc/oc3/cachekeys"
)

/*
{
  "action": "thaw",
  "argv": [
    "foo",
    "thaw",
    "--local"
  ],
  "begin": "2026-01-21T18:00:00.357669+01:00",
  "cron": false,
  "path": "foo",
  "session_uuid": "b9d795bc-498e-4c20-aada-9feec2eaa947",
  "version": "2.1-1977"
}
*/

// PostFeedInstanceAction handles POST /action/begin
func (a *Api) PostFeedInstanceAction(c echo.Context) error {
	keyH := cachekeys.FeedInstanceActionH
	keyQ := cachekeys.FeedInstanceActionQ
	keyPendingH := cachekeys.FeedInstanceActionPendingH

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

	var payload api.PostFeedInstanceActionJSONRequestBody
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

	uuid := uuid.New().String()
	idx := fmt.Sprintf("%s@%s@%s:%s", payload.Path, nodeID, ClusterID, uuid)

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
	return c.JSON(http.StatusAccepted, api.ActionRequestAccepted{Uuid: uuid})
}
