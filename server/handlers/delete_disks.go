package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteDisks handles DELETE /disks (bulk).
func (a *Api) DeleteDisks(c echo.Context) error {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawID, ok := body["disk_id"]
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "The 'disk_id' key is mandatory")
	}
	return a.deleteDiskByID(c, "DeleteDisks", fmt.Sprintf("%v", rawID))
}
