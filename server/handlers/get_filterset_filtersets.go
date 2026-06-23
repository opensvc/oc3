package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetFiltersetFiltersets handles GET /filtersets/{filterset_id}/filtersets
func (a *Api) GetFiltersetFiltersets(c echo.Context, filtersetId string, params server.GetFiltersetFiltersetsParams) error {
	log := echolog.GetLogHandler(c, "GetFiltersetFiltersets")
	odb := a.ODB
	ctx := c.Request().Context()

	fsetID, _, err := odb.FiltersetByIDOrName(ctx, filtersetId)
	if err != nil {
		log.Error("cannot lookup filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup filterset %s", filtersetId)
	}
	if fsetID == 0 {
		return JSONProblemf(c, http.StatusNotFound, "fset %s does not exist", filtersetId)
	}

	return a.handleList(c, "GetFiltersetFiltersets", "filterset", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFiltersetEncapFiltersets(ctx, fsetID, p)
	})
}
