package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetDisk handles GET /disks/{disk_id}
func (a *Api) GetDisk(c echo.Context, diskId string, params server.GetDiskParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["disk"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetDisk")
	odb := a.getODB()
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "disk_id", diskId, "props", query.Props, "is_manager", isManager)

	selectExprs, err := buildSelectClause(query.Props, propsMapping["disk"])
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	disks, err := odb.GetDisk(ctx, diskId, cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
	})
	if err != nil {
		log.Error("cannot get disk", "disk_id", diskId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get disk")
	}
	if len(disks) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "disk %s not found", diskId)
	}

	return c.JSON(http.StatusOK, newListResponse(disks, propsMapping["disk"], query))
}
