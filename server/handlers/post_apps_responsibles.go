package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// PostAppsResponsibles handles POST /apps_responsibles (bulk).
func (a *Api) PostAppsResponsibles(c echo.Context) error {
	appId, groupId, err := decodeAppGroupKeys(c)
	if err != nil {
		return err
	}
	return a.postAppResponsible(c, "PostAppsResponsibles", appId, groupId)
}
