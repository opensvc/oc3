package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostServicesActions handles POST /services_actions: the bulk form of the
// acknowledgement, the action named by the 'id' key of the body.
func (a *Api) PostServicesActions(c echo.Context) error {
	var body server.PostServicesActionsJSONBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Id == 0 {
		return JSONProblemf(c, http.StatusBadRequest, "the 'id' key must be specified")
	}
	return a.ackServiceAction(c, body.Id, int(body.Ack), body.AckedComment)
}

// PostServicesAction handles POST /services_actions/{action_id}
func (a *Api) PostServicesAction(c echo.Context, actionId int64) error {
	var body server.ServiceActionAckRequest
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	return a.ackServiceAction(c, actionId, int(body.Ack), body.AckedComment)
}

// ackServiceAction acknowledges a failed action, as the historical
// rest_post_services_action: the responsibility of the service is required, an
// action not failed is skipped, and the count of the unacknowledged errors of
// the instance and its "action errors" alert follow. Unlike the historical
// collector, the failed lines the action logged are acknowledged with it: the
// count includes them, and an acknowledged action left it above zero.
func (a *Api) ackServiceAction(c echo.Context, id int64, ack int, comment *string) error {
	log := echolog.GetLogHandler(c, "PostServicesAction")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if ack != 1 {
		return JSONProblemf(c, http.StatusBadRequest, "ack must be 1: an acknowledgement is not withdrawn")
	}
	odb := a.ODB
	action, err := odb.ServiceActionForAck(ctx, id)
	if err != nil {
		log.Error("cannot read the action", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the action")
	}
	if action == nil {
		return JSONProblemf(c, http.StatusNotFound, "log entry %d does not exist", id)
	}
	responsible, err := odb.ServiceResponsible(ctx, action.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check service responsibility", "svc_id", action.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check service responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for the service of action %d", id)
	}
	if action.Status != "err" {
		return c.JSON(http.StatusOK, infoAnswer("skip acknowledge of action %d in status '%s'", id, action.Status))
	}
	if action.Acked {
		return c.JSON(http.StatusOK, infoAnswer("skip acknowledge of action %d: already acknowledged", id))
	}
	text := ""
	if comment != nil {
		text = *comment
	}
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := odb.AckServiceAction(ctx, action, userEmail, text); err != nil {
		log.Error("cannot acknowledge the action", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot acknowledge the action")
	}
	entry := cdb.LogEntry{
		Action: "actions.change",
		User:   userEmail,
		Fmt:    "changed action %(id)s: %(data)s",
		Dict:   map[string]any{"id": fmt.Sprint(id), "data": fmt.Sprintf("ack: 1, acked_comment: %s", text)},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(action.NodeID); err == nil {
		entry.NodeID = &parsed
	}
	if parsed, err := uuid.Parse(action.SvcID); err == nil {
		entry.SvcID = &parsed
	}
	if err := odb.Log(ctx, entry); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	// The count of the instance, then its alert, as the feed of the actions does.
	if err := odb.UpdateActionErrors(ctx, action.SvcID, action.NodeID); err != nil {
		log.Error("cannot count the action errors", "svc_id", action.SvcID, logkey.Error, err)
	} else if err := odb.UpdateDashActionErrors(ctx, action.SvcID, action.NodeID); err != nil {
		log.Error("cannot update the action errors alert", "svc_id", action.SvcID, logkey.Error, err)
	}
	odb.SetChange("b_action_errors", "dashboard")
	a.notifyChanges(log, ctx)
	return c.JSON(http.StatusOK, infoAnswer("action %d acknowledged", id))
}
