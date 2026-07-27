package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteIps handles DELETE /ips
func (a *Api) DeleteIps(c echo.Context) error {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawID, ok := body["id"]
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "The 'id' key is mandatory")
	}
	return a.deleteIPByID(c, "DeleteIps", fmt.Sprintf("%v", rawID))
}
