package feederhandlers

import (
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// notifyChanges announces the tables the request changed, for the clients
// following the collector live. Without a messenger nothing is published; a
// failure to publish is logged and does not fail the request of the agent.
func (a *Api) notifyChanges(c echo.Context) {
	if err := a.ODB.Session.NotifyChanges(c.Request().Context()); err != nil {
		echolog.GetLog(c).Debug("cannot notify changes", logkey.Error, err)
	}
}
