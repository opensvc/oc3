package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteGroups handles DELETE /groups
func (a *Api) DeleteGroups(c echo.Context) error {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawID, ok := body["id"]
	if !ok {
		rawID, ok = body["role"]
	}
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "Either the 'id' or 'role' key is mandatory")
	}
	return a.DeleteGroup(c, fmt.Sprintf("%v", rawID))
}
