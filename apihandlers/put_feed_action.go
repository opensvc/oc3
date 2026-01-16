package apihandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

// PutFeedActionEnd handles PUT /feed/action
func (a *Api) PutFeedActionEnd(c echo.Context) error {
	keyH := cachekeys.FeedActionEndH
	keyQ := cachekeys.FeedActionEndQ
	keyPendingH := cachekeys.FeedActionEndPendingH

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

	log.Debug("action end accepted")
	return c.NoContent(http.StatusAccepted)
}
