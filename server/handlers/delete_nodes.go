package serverhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"
)

// DeleteNodes handles DELETE /nodes
func (a *Api) DeleteNodes(c echo.Context) error {
	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	rawID, ok := body["node_id"]
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "The 'node_id' key must be specified")
	}
	return a.deleteNodeByID(c, "DeleteNodes", fmt.Sprintf("%v", rawID))
}
