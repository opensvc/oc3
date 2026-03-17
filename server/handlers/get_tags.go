package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// handleGetTags is the common logic for getting tags
func (a *Api) handleGetTags(c echo.Context, tagID *int, page PageParams) error {
	log := echolog.GetLogHandler(c, "handleGetTags")
	odb := a.getODB()
	ctx := c.Request().Context()

	tags, err := odb.GetTags(ctx, tagID, page.Limit, page.Offset)
	if err != nil {
		log.Error("cannot get tags", logkey.TagID, tagID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get tags")
	}

	if tagID != nil {
		// Single tag requested
		if len(tags) == 0 {
			return JSONProblemf(c, http.StatusNotFound, "tag %d not found", *tagID)
		}
		return c.JSON(http.StatusOK, tags[0])
	}

	// All tags requested
	return c.JSON(http.StatusOK, tags)
}

// GetTags handles GET /tags
func (a *Api) GetTags(c echo.Context, params server.GetTagsParams) error {
	page := buildPageParams(params.Limit, params.Offset)
	log := echolog.GetLogHandler(c, "GetTags")
	log.Info("called", "limit", page.Limit, "offset", page.Offset)
	return a.handleGetTags(c, nil, page)
}
