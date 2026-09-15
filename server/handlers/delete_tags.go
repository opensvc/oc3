package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteTags handles DELETE /tags
func (a *Api) DeleteTags(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteTags")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireTagManager(c); err != nil {
		return err
	}

	var body server.DeleteTagsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.TagId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'tag_id' key is mandatory")
	}

	log.Info("called", logkey.TagID, body.TagId)

	tag, err := a.resolveTagByKey(c, log, ctx, body.TagId)
	if err != nil {
		return err
	}

	return a.deleteTagCascade(c, log, ctx, tag)
}
