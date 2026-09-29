package feederhandlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

// PostSANSwitch will populate FeedSANSwitchH <type>@<name> with the posted
// switch configuration, and queue its parsing.
//
// A switch is keyed by its name and not by the node reporting it: the
// switches of a fabric are inventoried by a couple of nodes of the whole
// infrastructure, and the latest report of a switch is the one to parse,
// whichever node sent it.
func (a *Api) PostSANSwitch(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "PostSANSwitch")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	var payload feeder.PostSANSwitchJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	switch {
	case payload.Name == "", strings.Contains(payload.Name, "@"):
		return JSONProblemf(c, http.StatusBadRequest, "invalid switch name: %q", payload.Name)
	case !payload.Type.Valid():
		return JSONProblemf(c, http.StatusBadRequest, "unsupported switch type: %q", payload.Type)
	}
	b, err := json.Marshal(payload)
	if err != nil {
		log.Warn("Marshal", logkey.Error, err)
		return JSONError(c)
	}
	ctx := c.Request().Context()
	idx := string(payload.Type) + "@" + payload.Name
	log.Debug("HSet keyH")
	if err := a.Redis.HSet(ctx, cachekeys.FeedSANSwitchH, idx, b).Err(); err != nil {
		log.Error("HSet keyH", logkey.Error, err)
		return JSONError(c)
	}

	if err := a.pushNotPending(ctx, log, cachekeys.FeedSANSwitchPendingH, cachekeys.FeedSANSwitchQ, idx); err != nil {
		log.Error("pushNotPending", logkey.Error, err)
		return JSONError(c)
	}
	return c.JSON(http.StatusAccepted, nil)
}
