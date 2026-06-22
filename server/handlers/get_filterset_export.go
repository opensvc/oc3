package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetFiltersetExport handles GET /filtersets/{filterset_id}/export
func (a *Api) GetFiltersetExport(c echo.Context, filtersetId string) error {
	log := echolog.GetLogHandler(c, "GetFiltersetExport")
	odb := a.ODB
	ctx := c.Request().Context()

	fsetID, _, err := odb.FiltersetByIDOrName(ctx, filtersetId)
	if err != nil {
		log.Error("cannot lookup filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup filterset %s", filtersetId)
	}
	if fsetID == 0 {
		return c.JSON(http.StatusOK, map[string]any{"error": "filterset not found"})
	}

	data, err := odb.ExportFiltersets(ctx, []int{fsetID})
	if err != nil {
		log.Error("cannot export filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot export filterset %s", filtersetId)
	}
	return c.JSON(http.StatusOK, data)
}
