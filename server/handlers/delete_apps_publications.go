package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// DeleteAppsPublications handles DELETE /apps_publications (bulk).
func (a *Api) DeleteAppsPublications(c echo.Context) error {
	appId, groupId, err := decodeAppGroupKeys(c)
	if err != nil {
		return err
	}
	return a.deleteAppPublication(c, "DeleteAppsPublications", appId, groupId)
}
