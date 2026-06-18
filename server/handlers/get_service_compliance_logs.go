package serverhandlers

import (
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetServiceComplianceLogs handles GET /services/{svc_id}/compliance/logs
func (a *Api) GetServiceComplianceLogs(c echo.Context, svcId string, params server.GetServiceComplianceLogsParams) error {
	log := echolog.GetLogHandler(c, "GetServiceComplianceLogs")

	log.Info("called", "svc_id", svcId, "props", params.Props)

	// TODO

	return c.JSON(200, []any{})
}
