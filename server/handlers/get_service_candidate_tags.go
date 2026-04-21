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

// GetServiceCandidateTags handles GET /services/{svc_id}/candidate_tags
func (a *Api) GetServiceCandidateTags(c echo.Context, svcId string, params server.GetServiceCandidateTagsParams) error {
	log := echolog.GetLogHandler(c, "GetServiceCandidateTags")
	odb := a.getODB()
	ctx := c.Request().Context()

	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	log.Info("called", "svc_id", svcId, "is_manager", isManager)

	// Verify service exists and is accessible to this user
	svcs, err := odb.GetService(ctx, svcId, cdb.ListParams{Limit: 1, Groups: groups, IsManager: isManager})
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service")
	}
	if len(svcs) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	return a.handleList(c, "GetServiceCandidateTags", "tag", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetServiceCandidateTags(ctx, svcId, p)
	})
}
