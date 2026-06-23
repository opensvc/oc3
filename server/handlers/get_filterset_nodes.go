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

// GetFiltersetNodes handles GET /filtersets/{filterset_id}/nodes
func (a *Api) GetFiltersetNodes(c echo.Context, filtersetId string, params server.GetFiltersetNodesParams) error {
	log := echolog.GetLogHandler(c, "GetFiltersetNodes")
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

	nodeIDs, err := odb.ResolveFilterset(ctx, fsetID, "node_id")
	if err != nil {
		log.Error("cannot resolve filterset to node_ids", "filterset_id", fsetID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset %s", filtersetId)
	}

	return a.handleList(c, "GetFiltersetNodes", "node", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetNodesByIDs(ctx, nodeIDs, p)
	})
}
