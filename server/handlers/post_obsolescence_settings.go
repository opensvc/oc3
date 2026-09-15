package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostObsolescenceSettings handles POST /obsolescence/settings
func (a *Api) PostObsolescenceSettings(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostObsolescenceSettings")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireObsManager(c); err != nil {
		return err
	}

	var body server.PostObsolescenceSettingsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Id == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'id' key is mandatory")
	}

	return a.postObsolescenceSettingUpdate(c, ctx, log, body.Id, body.ObsWarnDate, body.ObsAlertDate)
}
