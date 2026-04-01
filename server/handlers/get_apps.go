package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetApps handles GET /apps
func (a *Api) GetApps(c echo.Context, params server.GetAppsParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, propsMapping["app"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetApps")
	odb := a.getODB()
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "limit", query.Page.Limit, "offset", query.Page.Offset, "props", query.Props, "meta", query.WithMeta, "stats", query.WithStats, "is_manager", isManager)

	apps, err := odb.GetApps(ctx, groups, isManager, query.Page.Limit, query.Page.Offset)
	if err != nil {
		log.Error("cannot get apps", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get apps")
	}

	filteredItems, err := filterItemsFields(apps, query.Props)
	if err != nil {
		log.Error("cannot project app props", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot project app props")
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["app"], query))
}
