package serverhandlers

import (
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetTag handles GET /tags/{tag_id}
func (a *Api) GetTag(c echo.Context, tagIdParam string, params server.GetTagParams) error {
	props, err := buildProps(params.Props, propsMapping["tag"])
	if err != nil {
		return JSONProblem(c, 400, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetTag")
	log.Info("called", logkey.TagID, tagIdParam, "props", props)
	tag, err := a.resolveTagByKey(c, log, c.Request().Context(), tagIdParam)
	if err != nil {
		return err
	}
	return a.handleGetTags(c, &tag.ID, ListQueryParameters{
		Props: props,
	}, nil)
}
