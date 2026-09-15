package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteApps handles DELETE /apps (bulk).
func (a *Api) DeleteApps(c echo.Context) error {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawID, ok := body["id"]
	if !ok {
		rawID, ok = body["app"]
	}
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "Either the 'id' or 'app' key is mandatory")
	}
	return a.deleteAppByID(c, "DeleteApps", fmt.Sprintf("%v", rawID))
}
