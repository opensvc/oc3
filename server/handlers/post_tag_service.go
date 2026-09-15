package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostTagService handles POST /tags/{tag_id}/services/{svc_id}: attach a tag to a service.
func (a *Api) PostTagService(c echo.Context, tagIdParam int, svcId string) error {
	log := echolog.GetLogHandler(c, "PostTagService")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, "svc_id", svcId)

	var body server.PostTagServiceJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	tag, err := a.resolveTagByRecordID(c, log, ctx, tagIdParam)
	if err != nil {
		return err
	}

	return a.attachTagService(c, log, ctx, tag, svcId, body.TagAttachData)
}
