package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetClusters handles GET /clusters
func (a *Api) GetClusters(c echo.Context, params server.GetClustersParams) error {
	return a.handleList(c, "GetClusters", "clusterList", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetClusters(ctx, p)
	})
}

// GetCluster handles GET /clusters/{cluster_id}
func (a *Api) GetCluster(c echo.Context, clusterId string, params server.GetClusterParams) error {
	return a.handleItem(c, "GetCluster", "clusterList", "cluster_id", clusterId, listEndpointParams{props: params.Props},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetCluster(ctx, clusterId, p)
		})
}
