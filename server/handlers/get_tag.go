package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// GetTag handles GET /tags/{tag_id}
func (a *Api) GetTag(c echo.Context, tagIdParam int) error {
	log := getLog(c)
	log.Info("GetTag called", "tag_id", tagIdParam)
	return a.handleGetTags(c, &tagIdParam)
}
