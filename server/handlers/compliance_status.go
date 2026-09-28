package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// parseCompID reads the integer id of a compliance record from a path.
func parseCompID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, httpErrorf(http.StatusBadRequest, "invalid id %q: an integer is expected", raw)
	}
	return id, nil
}

// GetComplianceLogs handles GET /compliance/logs, as the historical
// rest_get_compliance_logs: the runs of the nodes the caller may see, of the
// nodes of the fset_id filterset when one is given.
func (a *Api) GetComplianceLogs(c echo.Context, params server.GetComplianceLogsParams) error {
	var fsetNodeIDs []string
	if params.FsetId != nil && *params.FsetId != "" {
		ids, err := a.filtersetNodeIDs(c.Request().Context(), *params.FsetId)
		if err != nil {
			return httpProblem(c, err)
		}
		fsetNodeIDs = ids
	}
	return a.handleList(c, "GetComplianceLogs", "comp_log", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceLogs(ctx, nil, fsetNodeIDs, p)
	})
}

// filtersetNodeIDs resolves a filterset given by id or name to the ids of the
// nodes it selects, never nil: an empty selection matches no node.
func (a *Api) filtersetNodeIDs(ctx context.Context, ref string) ([]string, error) {
	fsetID, _, err := a.ODB.FiltersetByIDOrName(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("filtersetNodeIDs: %w", err)
	}
	if fsetID == 0 {
		return nil, httpErrorf(http.StatusNotFound, "fset %s does not exist", ref)
	}
	ids, err := a.ODB.ResolveFilterset(ctx, fsetID, "node_id")
	if err != nil {
		return nil, fmt.Errorf("filtersetNodeIDs: %w", err)
	}
	if ids == nil {
		ids = []string{}
	}
	return ids, nil
}

// GetComplianceLog handles GET /compliance/logs/{log_id}.
func (a *Api) GetComplianceLog(c echo.Context, logId string, params server.GetComplianceLogParams) error {
	id, err := parseCompID(logId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.handleItem(c, "GetComplianceLog", "comp_log", "id", logId, listEndpointParams{
		props: params.Props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceLogs(ctx, &id, nil, p)
	})
}

// GetComplianceStatus handles GET /compliance/status, as the historical
// rest_get_compliance_status: the last runs of the nodes the caller may see.
func (a *Api) GetComplianceStatus(c echo.Context, params server.GetComplianceStatusParams) error {
	return a.handleList(c, "GetComplianceStatus", "comp_status", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceStatus(ctx, nil, p)
	})
}

// GetComplianceStatusRun handles GET /compliance/status/{status_id}.
func (a *Api) GetComplianceStatusRun(c echo.Context, statusId string, params server.GetComplianceStatusRunParams) error {
	id, err := parseCompID(statusId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.handleItem(c, "GetComplianceStatusRun", "comp_status", "id", statusId, listEndpointParams{
		props: params.Props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetComplianceStatus(ctx, &id, p)
	})
}

// DeleteComplianceStatusRun handles DELETE /compliance/status/{status_id}.
func (a *Api) DeleteComplianceStatusRun(c echo.Context, statusId string) error {
	id, err := parseCompID(statusId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.deleteComplianceStatusRun(c, id)
}

// DeleteComplianceStatus handles DELETE /compliance/status, the bulk form taking
// the id of the run in the body, as the historical rest_delete_compliance_status_runs.
func (a *Api) DeleteComplianceStatus(c echo.Context) error {
	var body server.DeleteComplianceStatusJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Id == nil {
		return JSONProblemf(c, http.StatusBadRequest, "The 'id' key is mandatory")
	}
	return a.deleteComplianceStatusRun(c, int64(*body.Id))
}

// deleteComplianceStatusRun deletes the last check run of a module-node-service
// tuple, as the historical rest_delete_compliance_status_run: the CompExec
// privilege and the responsibility of the node are required, and the deletion is
// logged.
func (a *Api) deleteComplianceStatusRun(c echo.Context, id int64) error {
	log := echolog.GetLogHandler(c, "DeleteComplianceStatusRun")
	ctx := c.Request().Context()
	if !IsManager(c) && !HasGroup(c, "CompExec") {
		return JSONProblemf(c, http.StatusForbidden, "the CompExec privilege is required")
	}
	groups := UserGroupsFromContext(c)
	run, err := a.ODB.CompStatusRunVisible(ctx, id, groups, IsManager(c))
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the run", err))
	}
	if run == nil {
		return c.JSON(http.StatusOK, map[string]string{"info": fmt.Sprintf("Run %d not found or you are not responsible for the node", id)})
	}
	if run.NodeID != "" {
		responsible, err := a.ODB.NodeResponsible(ctx, run.NodeID, groups, IsManager(c))
		if err != nil {
			return httpProblem(c, httpInternal(log, "cannot check the node responsibility", err))
		}
		if !responsible {
			return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", run.NodeID)
		}
	}
	if err := a.ODB.DeleteCompStatusRun(ctx, id); err != nil {
		return httpProblem(c, httpInternal(log, "cannot delete the run", err))
	}
	target := "node " + run.Nodename
	if run.SvcID != "" {
		target = "service " + run.Svcname + " on " + target
	}
	a.compLog(c, "compliance.status.delete", "deleted run module %(module)s on %(target)s",
		map[string]any{"module": run.RunModule, "target": target}, run.NodeID, run.SvcID)
	a.notifyChanges(log, ctx)
	return c.JSON(http.StatusOK, map[string]string{"info": fmt.Sprintf("Run %d deleted", id)})
}

// compLog writes a compliance change to the collector log, as _log() does in the
// historical collector.
func (a *Api) compLog(c echo.Context, action, format string, dict map[string]any, nodeID, svcID string) {
	userEmail, _ := c.Get(XUserEmail).(string)
	entry := cdb.LogEntry{Action: action, User: userEmail, Fmt: format, Dict: dict, Level: "info"}
	if id, err := uuid.Parse(nodeID); err == nil {
		entry.NodeID = &id
	}
	if id, err := uuid.Parse(svcID); err == nil {
		entry.SvcID = &id
	}
	if err := a.ODB.Log(c.Request().Context(), entry); err != nil {
		echolog.GetLogHandler(c, action).Error("cannot write audit log", logkey.Error, err)
	}
}
