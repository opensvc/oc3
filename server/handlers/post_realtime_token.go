package serverhandlers

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// realtimeGroup is the messenger group the change events are published to.
const realtimeGroup = "generic"

// PostRealtimeToken handles POST /realtime/token: a one-time token for the
// websocket of the messenger, which broadcasts the "<table>_change" events. The
// signed-in user then connects to /realtime/<group>/<token>; a messenger that
// requires tokens lets in only those the server registered, so only
// authenticated users listen. The token is random, single use, and says nothing
// of the user.
func (a *Api) PostRealtimeToken(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostRealtimeToken")
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if a.Realtime == nil {
		return JSONProblemf(c, http.StatusServiceUnavailable, "no messenger is configured")
	}
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		log.Error("cannot generate token", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot generate a token")
	}
	token := hex.EncodeToString(raw)
	if err := a.Realtime.RegisterToken(token); err != nil {
		log.Error("cannot register token with the messenger", logkey.Error, err)
		return JSONProblemf(c, http.StatusServiceUnavailable, "the messenger cannot be reached")
	}
	return c.JSON(http.StatusOK, map[string]any{"data": map[string]string{"token": token, "group": realtimeGroup}})
}
