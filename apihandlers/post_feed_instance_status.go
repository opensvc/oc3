package apihandlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

func (a *Api) PostFeedInstanceStatus(c echo.Context) error {
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

	if !strings.HasPrefix(payload.Version, "2.") && !strings.HasPrefix(payload.Version, "3.") {
		log.Error(fmt.Sprintf("unexpected version %s", payload.Version))
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unsupported data client version: %s", payload.Version)
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}

	// Generate unique ID for this status submission
	requestId := uuid.New().String()
	id := fmt.Sprintf("%s@%s@%s:%s", payload.Path, nodeID, clusterID, requestId)

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

	// Return the ID to the client
	return c.JSON(http.StatusAccepted, map[string]string{
		"id": id,
	})
}
