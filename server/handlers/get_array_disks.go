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

// GetArrayDisks handles GET /arrays/{array_id}/disks: the luns of an array named
// or numbered array_id, with their uses by nodes and services, as the historical
// rest_get_array_disks listed them.
func (a *Api) GetArrayDisks(c echo.Context, arrayId string, params server.GetArrayDisksParams) error {
	log := echolog.GetLogHandler(c, "GetArrayDisks")
	ctx := c.Request().Context()
	id, ok, err := a.ODB.ArrayIDByIdent(ctx, arrayId)
	if err != nil {
		log.Error("cannot resolve the array", "array_id", arrayId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve array %s", arrayId)
	}
	if !ok {
		return JSONProblemf(c, http.StatusNotFound, "array %s not found", arrayId)
	}
	return a.handleList(c, "GetArrayDisks", "array_disk", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetArrayDisks(ctx, id, p)
	})
}
