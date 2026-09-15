package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostTagsServices handles POST /tags/services
func (a *Api) PostTagsServices(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostTagsServices")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.PostTagsServicesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.SvcId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'svc_id' key is mandatory")
	}
	if body.TagId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'tag_id' key is mandatory")
	}

	log.Info("called", logkey.TagID, body.TagId, "svc_id", body.SvcId)

	tag, err := a.resolveTagByKey(c, log, ctx, body.TagId)
	if err != nil {
		return err
	}

	return a.attachTagService(c, log, ctx, tag, body.SvcId, body.TagAttachData)
}
