package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// GetTagNodes handles GET /tags/{tag_id}/nodes
func (a *Api) GetTagNodes(c echo.Context, tagIdParam int) error {
	log := getLog(c)

	log.Info("GetTagNodes called", "tag_id", tagIdParam)

	// TODO

	return c.JSON(200, []interface{}{})
}
