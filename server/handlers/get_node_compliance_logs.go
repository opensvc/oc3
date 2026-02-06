package serverhandlers

import (
	"github.com/labstack/echo/v4"
)

// GetNodeComplianceLogs handles GET /nodes/{node_id}/compliance/logs
func (a *Api) GetNodeComplianceLogs(c echo.Context, nodeId string) error {
	log := getLog(c)

	log.Info("GetNodeComplianceLogs called", "node_id", nodeId)

	// TODO

	return c.JSON(200, []any{})
}
