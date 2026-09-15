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

// GetLogs handles GET /logs
func (a *Api) GetLogs(c echo.Context, params server.GetLogsParams) error {
	log := echolog.GetLogHandler(c, "GetLogs")
	ctx := c.Request().Context()

	var fset cdb.LogsFiltersetFilter
	if params.FsetId != nil && *params.FsetId != "" {
		fsetID, _, err := a.ODB.FiltersetByIDOrName(ctx, *params.FsetId)
		if err != nil {
			log.Error("cannot lookup filterset", "fset_id", *params.FsetId, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup filterset %s", *params.FsetId)
		}
		if fsetID == 0 {
			return JSONProblemf(c, http.StatusNotFound, "fset %s does not exist", *params.FsetId)
		}
		nodeIDs, err := a.ODB.ResolveFilterset(ctx, fsetID, "node_id")
		if err != nil {
			log.Error("cannot resolve filterset node_ids", "fset_id", fsetID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset %s", *params.FsetId)
		}
		svcIDs, err := a.ODB.ResolveFilterset(ctx, fsetID, "svc_id")
		if err != nil {
			log.Error("cannot resolve filterset svc_ids", "fset_id", fsetID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset %s", *params.FsetId)
		}
		fset = cdb.LogsFiltersetFilter{Active: true, NodeIDs: nodeIDs, SvcIDs: svcIDs}
	}

	return a.handleList(c, "GetLogs", "log_event", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetLogs(ctx, p, fset)
	})
}
