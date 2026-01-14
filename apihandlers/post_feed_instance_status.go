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

func (a *Api) PostFeedInstanceStatus(c echo.Context, params api.PostFeedInstanceStatusParams) error {
	keyH := cachekeys.FeedInstanceStatusH
	keyQ := cachekeys.FeedInstanceStatusQ
	keyPendingH := cachekeys.FeedInstanceStatusPendingH

	log := getLog(c)

	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	clusterID := clusterIDFromContext(c)
	if clusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "Refused", "authenticated node doesn't define cluster id")
	}

	var payload api.PostFeedInstanceStatusJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}

	if !strings.HasPrefix(payload.Version, "2.") {
		log.Error(fmt.Sprintf("unexpected version %s", payload.Version))
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unsupported data client version: %s", payload.Version)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}

	id := fmt.Sprintf("%s@%s@%s", payload.Path, nodeID, clusterID)

	reqCtx := c.Request().Context()

	// Store data in Redis hash with generated ID as key
	s := fmt.Sprintf("HSET %s %s", keyH, id)

	if _, err := a.Redis.HSet(reqCtx, keyH, id, b).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		log.Error(s)
		return JSONProblem(c, http.StatusInternalServerError, "", s)
	}

	if err := a.pushNotPending(reqCtx, keyPendingH, keyQ, id); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", keyQ, id, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", keyQ, id, err)
	}

	if params.Sync != nil && *params.Sync {
		return c.NoContent(http.StatusOK)
	} else {
		return c.NoContent(http.StatusAccepted)
	}
}
