package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteTagService handles DELETE /tags/{tag_id}/services/{svc_id}: detach a tag from a service.
func (a *Api) DeleteTagService(c echo.Context, tagIdParam int, svcId string) error {
	log := echolog.GetLogHandler(c, "DeleteTagService")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, "svc_id", svcId)

	tag, err := a.resolveTagByRecordID(c, log, ctx, tagIdParam)
	if err != nil {
		return err
	}

	return a.detachTagService(c, log, ctx, tag, svcId)
}
