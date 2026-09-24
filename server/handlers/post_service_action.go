package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// serviceActions are the agent actions the API accepts on a service or on one of
// its instances. As for nodes, only what does not interrupt the service is exposed:
// start, stop, restart, switch, giveback, takeover, the sync and provisioning
// actions are deliberately left out for now.
var serviceActions = map[string]bool{
	"push resinfo": true,
	"push config":  true,
	"freeze":       true,
	"thaw":         true,
}

// agentAtLeast19 reports whether the agent understands --local, as the python
// collector decides it from the leading digits of the reported version.
func agentAtLeast19(version string) bool {
	if len(version) < 3 {
		return false
	}
	v, err := strconv.ParseFloat(version[:3], 64)
	if err != nil {
		return false
	}
	return v >= 1.9
}

// serviceActionCommand builds the queued command, as fmt_svc_action() does. A pull
// or feed node reads the bare action from its queue; a push node is reached over
// ssh. `local` restricts the action to that one instance.
func serviceActionCommand(action, svcname, actionType, connectTo, agentVersion string, local bool) string {
	var cmd []string
	if actionType != "pull" && actionType != "feed" {
		cmd = append(cmd, sshCmd...)
		cmd = append(cmd, "opensvc@"+connectTo, "--", "sudo", "svcmgr", "--service", svcname)
	}
	cmd = append(cmd, action)
	if local && agentAtLeast19(agentVersion) {
		cmd = append(cmd, "--local")
	}
	return strings.Join(cmd, " ")
}

// queueServiceAction is the part shared by the service and the instance endpoints.
func (a *Api) queueServiceAction(
	c echo.Context, log *slog.Logger, ctx context.Context,
	svc *cdb.DBService, target *cdb.NodeActionTarget, agentVersion, action string, local bool,
) error {
	odb := a.ODB

	// A node feeding another collector queues its actions there; oc3 has no such
	// push channel, so the caller is told rather than left with a dead entry.
	if target.Collector != "" {
		return JSONProblemf(c, http.StatusBadRequest,
			"node %s is attached to collector %s: queue the action there", target.Nodename, target.Collector)
	}

	connectTo, err := odb.NodeReachableAddress(ctx, target)
	if err != nil {
		log.Error("cannot find a reachable address", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot find an address for node %s", target.Nodename)
	}

	// An unset action_type means pull, as get_action_type() does.
	actionType := target.ActionType
	if actionType == "" {
		actionType = "pull"
	}
	command := serviceActionCommand(action, svc.Svcname, actionType, connectTo, agentVersion, local)

	id, err := odb.EnqueueServiceAction(ctx, target.NodeID, svc.SvcID, actionType, command, connectTo, authUserID(c))
	if err != nil {
		log.Error("cannot queue the action", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot queue action %s", action)
	}

	log.Info("action queued", "svc_id", svc.SvcID, logkey.NodeID, target.NodeID, "action", action, "action_id", id)

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "service.action",
		User:   userEmail,
		Fmt:    "run %(action)s",
		Dict:   map[string]any{"action": action},
		Level:  "info",
	}
	if parsed, parseErr := uuid.Parse(svc.SvcID); parseErr == nil {
		logEntry.SvcID = &parsed
	}
	if parsed, parseErr := uuid.Parse(target.NodeID); parseErr == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"id":      id,
		"command": command,
		"node_id": target.NodeID,
		"info":    "action " + action + " queued on service " + svc.Svcname + " from node " + target.Nodename,
	})
}

// checkServiceAction validates the body and the caller rights shared by both endpoints.
func (a *Api) checkServiceAction(c echo.Context, log *slog.Logger, ctx context.Context, svcId, action string) (*cdb.DBService, error) {
	if !serviceActions[action] {
		return nil, denyRequest(c, http.StatusBadRequest, "unsupported action %q", action)
	}
	if !IsManager(c) && !HasGroup(c, "NodeExec") {
		return nil, denyRequest(c, http.StatusForbidden, "user has no NodeExec privilege")
	}

	svc, err := a.resolveServiceRow(c, log, ctx, svcId)
	if err != nil {
		return nil, err
	}

	responsible, err := a.ODB.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check service responsibility", logkey.Error, err)
		return nil, denyRequest(c, http.StatusInternalServerError, "cannot check service responsibility")
	}
	if !responsible {
		return nil, denyRequest(c, http.StatusForbidden, "user is not responsible for service %s", svcId)
	}
	return svc, nil
}

// PostServiceAction handles POST /services/{svc_id}/actions: queue an action on the
// whole service, from one of its live nodes.
func (a *Api) PostServiceAction(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "PostServiceAction")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.PostServiceActionJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	action := string(body.Action)

	svc, err := a.checkServiceAction(c, log, ctx, svcId, action)
	if err != nil {
		return err
	}

	target, agentVersion, err := a.ODB.ServiceLiveNode(ctx, svc.SvcID)
	if err != nil {
		log.Error("cannot look for a live node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot look for a live node of service %s", svcId)
	}
	if target == nil {
		return JSONProblemf(c, http.StatusConflict,
			"no node of service %s has been seen in the last 15 minutes", svc.Svcname)
	}

	// The action is for the service as a whole: no --local, as do_svc_action does.
	return a.queueServiceAction(c, log, ctx, svc, target, agentVersion, action, false)
}

// PostServiceInstanceAction handles POST /services/{svc_id}/instances/{node_id}/actions:
// queue an action on one instance, acting on that node only.
func (a *Api) PostServiceInstanceAction(c echo.Context, svcId string, nodeId server.InPathNodeId) error {
	log := echolog.GetLogHandler(c, "PostServiceInstanceAction")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.PostServiceInstanceActionJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	action := string(body.Action)

	svc, err := a.checkServiceAction(c, log, ctx, svcId, action)
	if err != nil {
		return err
	}

	node, err := odb.NodeByNodeIDOrNodename(ctx, string(nodeId))
	if err != nil {
		log.Error("cannot lookup node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	responsible, err := odb.NodeResponsible(ctx, node.NodeID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check node responsibility", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", nodeId)
	}

	found, err := odb.HasServiceInstance(ctx, svc.SvcID, node.NodeID)
	if err != nil {
		log.Error("cannot check the instance", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check instance %s@%s", svcId, nodeId)
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "service %s has no instance on node %s", svc.Svcname, node.NodeID)
	}

	target, err := odb.NodeActionTargetByID(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot read node action fields", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read node %s", nodeId)
	}
	if target == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	agentVersion, err := odb.NodeAgentVersion(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot read the agent version", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read node %s", nodeId)
	}

	return a.queueServiceAction(c, log, ctx, svc, target, agentVersion, action, true)
}
