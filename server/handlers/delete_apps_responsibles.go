package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// DeleteAppsResponsibles handles DELETE /apps_responsibles (bulk).
func (a *Api) DeleteAppsResponsibles(c echo.Context) error {
	appId, groupId, err := decodeAppGroupKeys(c)
	if err != nil {
		return err
	}
	return a.deleteAppResponsible(c, "DeleteAppsResponsibles", appId, groupId)
}
