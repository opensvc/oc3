package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// PostAppsPublications handles POST /apps_publications (bulk).
func (a *Api) PostAppsPublications(c echo.Context) error {
	appId, groupId, err := decodeAppGroupKeys(c)
	if err != nil {
		return err
	}
	return a.postAppPublication(c, "PostAppsPublications", appId, groupId)
}
