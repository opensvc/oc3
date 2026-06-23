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

// GetFiltersetServices handles GET /filtersets/{filterset_id}/services
func (a *Api) GetFiltersetServices(c echo.Context, filtersetId string, params server.GetFiltersetServicesParams) error {
	log := echolog.GetLogHandler(c, "GetFiltersetServices")
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

	svcIDs, err := odb.ResolveFilterset(ctx, fsetID, "svc_id")
	if err != nil {
		log.Error("cannot resolve filterset to svc_ids", "filterset_id", fsetID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset %s", filtersetId)
	}

	return a.handleList(c, "GetFiltersetServices", "service", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetServicesByIDs(ctx, svcIDs, p)
	})
}
