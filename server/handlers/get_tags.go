package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// handleGetTags is the common logic for getting tags
func (a *Api) handleGetTags(c echo.Context, tagID *int) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()

	tags, err := odb.GetTags(ctx, tagID)
	if err != nil {
		log.Error("handleGetTags: cannot get tags", "tag_id", tagID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot get tags")
	}

	if tagID != nil {
		// Single tag requested
		if len(tags) == 0 {
			return JSONProblemf(c, http.StatusNotFound, "NotFound", "tag %d not found", *tagID)
		}
		return c.JSON(http.StatusOK, tags[0])
	}

	// All tags requested
	return c.JSON(http.StatusOK, tags)
}

// GetTags handles GET /tags
func (a *Api) GetTags(c echo.Context) error {
	log := getLog(c)
	log.Info("GetTags called")
	return a.handleGetTags(c, nil)
}
