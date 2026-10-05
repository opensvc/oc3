package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/schema"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetUserSelfFilterset handles GET /users/self/filterset: the session filterset of
// the caller, the one every list is narrowed to, or null.
func (a *Api) GetUserSelfFilterset(c echo.Context) error {
	log := echolog.GetLogHandler(c, "GetUserSelfFilterset")
	userID := authUserID(c)
	if !IsAuthByUser(c) || userID == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	id, name, err := a.ODB.UserFilterset(c.Request().Context(), *userID)
	if err != nil {
		log.Error("cannot read the session filterset", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the session filterset")
	}
	return c.JSON(http.StatusOK, sessionFiltersetResponse(id, name))
}

func sessionFiltersetResponse(id int, name string) server.SessionFiltersetResponse {
	if id == 0 {
		return server.SessionFiltersetResponse{}
	}
	return server.SessionFiltersetResponse{Data: &server.SessionFilterset{FsetId: id, FsetName: name}}
}

// PutUserSelfFilterset handles PUT /users/self/filterset: the caller chooses their
// session filterset, by id or name, as the filterset selector of the historical
// collector does. Not while acting as another user: the choice is theirs.
func (a *Api) PutUserSelfFilterset(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PutUserSelfFilterset")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	userID, problem := a.selfFiltersetWriter(c)
	if problem != nil {
		return problem
	}
	var body server.PutUserSelfFiltersetJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ref := strings.TrimSpace(body.FsetId)
	if ref == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the fset_id property is mandatory")
	}
	id, name, err := a.ODB.FiltersetByIDOrName(ctx, ref)
	if err != nil {
		log.Error("cannot lookup filterset", "filterset_id", ref, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup filterset %s", ref)
	}
	if id == 0 {
		return JSONProblemf(c, http.StatusNotFound, "filterset %s not found", ref)
	}
	if err := a.ODB.SetUserFilterset(ctx, userID, id); err != nil {
		log.Error("cannot set the session filterset", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot set the session filterset")
	}
	a.notifySessionFilterset(ctx, log)
	return c.JSON(http.StatusOK, sessionFiltersetResponse(id, name))
}

// DeleteUserSelfFilterset handles DELETE /users/self/filterset: no session
// filterset any more, the lists show everything the caller may see.
func (a *Api) DeleteUserSelfFilterset(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteUserSelfFilterset")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	userID, problem := a.selfFiltersetWriter(c)
	if problem != nil {
		return problem
	}
	if err := a.ODB.ClearUserFilterset(ctx, userID); err != nil {
		log.Error("cannot clear the session filterset", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot clear the session filterset")
	}
	a.notifySessionFilterset(ctx, log)
	return c.JSON(http.StatusOK, sessionFiltersetResponse(0, ""))
}

// selfFiltersetWriter returns the user changing their session filterset, or the
// problem answered when they may not.
func (a *Api) selfFiltersetWriter(c echo.Context) (int64, error) {
	userID := authUserID(c)
	if !IsAuthByUser(c) || userID == nil {
		return 0, denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if IsImpersonating(c) {
		return 0, denyRequest(c, http.StatusForbidden, "the session filterset cannot be changed while acting as another user")
	}
	return *userID, nil
}

func (a *Api) notifySessionFilterset(ctx context.Context, log *slog.Logger) {
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
}

// sessionFilters narrows a list to the session filterset of the caller, as
// apply_filters_id() of the historical collector does on every table: the rows
// whose node is one of the filterset's nodes, or whose service one of its
// services. A list naming both keeps the rows without a node or without a service
// on the other side, as an alert of a node, which has no service. A list with
// neither, or a caller without a session filterset, is left as it is, and so are
// the lists of one record (a route with a path parameter: the alerts of a node,
// the nodes of a filterset): what a record holds is shown whole, as the record is.
func (a *Api) sessionFilters(c echo.Context, mapping propMapping) ([]cdb.ColumnFilter, error) {
	nodeCol, svcCol := sessionFilterColumns(mapping)
	if (nodeCol == nil && svcCol == nil) || len(c.ParamNames()) > 0 {
		return nil, nil
	}
	userID := authUserID(c)
	if !IsAuthByUser(c) || userID == nil {
		return nil, nil
	}
	ctx := c.Request().Context()
	fsetID, _, err := a.ODB.UserFilterset(ctx, *userID)
	if err != nil || fsetID == 0 {
		return nil, err
	}
	var nodeIDs, svcIDs []string
	if nodeCol != nil {
		if nodeIDs, err = a.ODB.ResolveFilterset(ctx, fsetID, "node_id"); err != nil {
			return nil, err
		}
	}
	if svcCol != nil {
		if svcIDs, err = a.ODB.ResolveFilterset(ctx, fsetID, "svc_id"); err != nil {
			return nil, err
		}
	}
	return sessionFilter(nodeCol, nodeIDs, svcCol, svcIDs), nil
}

// sessionFilterColumns are the columns of a list naming a node and a service, those
// the session filterset applies to; nil when the list has none.
func sessionFilterColumns(mapping propMapping) (nodeCol, svcCol *schema.Col) {
	if def, ok := mapping.Props["node_id"]; ok {
		nodeCol = def.Col
	}
	if def, ok := mapping.Props["svc_id"]; ok {
		svcCol = def.Col
	}
	return
}

// sessionFilter is the condition of a session filterset on the node and service
// columns of a list, with the ids the filterset resolves to, one filter per column
// so that each brings its join. No id on a side: no row matches on that side.
func sessionFilter(nodeCol *schema.Col, nodeIDs []string, svcCol *schema.Col, svcIDs []string) []cdb.ColumnFilter {
	both := nodeCol != nil && svcCol != nil
	one := func(col *schema.Col, ids []string) cdb.ColumnFilter {
		cond, args := "1=0", []any(nil)
		if len(ids) > 0 {
			args = make([]any, len(ids))
			for i, id := range ids {
				args[i] = id
			}
			cond = fmt.Sprintf("%s IN (%s)", col.Qualified(), cdb.Placeholders(len(ids)))
		}
		if both {
			// A row without a node, or without a service, is judged on the other side.
			cond = fmt.Sprintf("(COALESCE(%s, '') = '' OR %s)", col.Qualified(), cond)
		}
		return cdb.ColumnFilter{Col: col, Expr: cond, Args: args}
	}
	var out []cdb.ColumnFilter
	if nodeCol != nil {
		out = append(out, one(nodeCol, nodeIDs))
	}
	if svcCol != nil {
		out = append(out, one(svcCol, svcIDs))
	}
	return out
}

// sessionScope is what the session filterset of the caller resolves to, nodes and
// services; both nil when the caller has none. Never nil with one: an empty list
// means a filterset matching nothing.
func (a *Api) sessionScope(c echo.Context) ([]string, []string, error) {
	userID := authUserID(c)
	if !IsAuthByUser(c) || userID == nil {
		return nil, nil, nil
	}
	ctx := c.Request().Context()
	fsetID, _, err := a.ODB.UserFilterset(ctx, *userID)
	if err != nil || fsetID == 0 {
		return nil, nil, err
	}
	nodeIDs, err := a.ODB.ResolveFilterset(ctx, fsetID, "node_id")
	if err != nil {
		return nil, nil, err
	}
	svcIDs, err := a.ODB.ResolveFilterset(ctx, fsetID, "svc_id")
	if err != nil {
		return nil, nil, err
	}
	if nodeIDs == nil {
		nodeIDs = []string{}
	}
	if svcIDs == nil {
		svcIDs = []string{}
	}
	return nodeIDs, svcIDs, nil
}
