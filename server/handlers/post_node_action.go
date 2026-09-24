package serverhandlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// nodeActions are the agent actions the API accepts, mirroring the node agent
// entries of the historical collector action menu. Actions that interrupt service
// (reboot, shutdown, drain, updatepkg...) are deliberately left out for now.
var nodeActions = map[string]bool{
	"pushasset": true,
	"pushdisks": true,
	"pushpkg":   true,
	"pushpatch": true,
	"pushstats": true,
	"checks":    true,
	"sysreport": true,
	"scanscsi":  true,
	"freeze":    true,
	"thaw":      true,
}

// sshCmd is the ssh invocation the python collector uses to reach a push node
// (get_ssh_cmd, with the default of its ssh_cmd).
var sshCmd = []string{
	"ssh",
	"-o", "StrictHostKeyChecking=no",
	"-o", "CheckHostIP=no",
	"-o", "ForwardX11=no",
	"-o", "ConnectTimeout=5",
	"-o", "PasswordAuthentication=no",
}

// nodeActionCommand builds the queued command, as fmt_node_action() does: a pull or
// feed node reads the bare action from its queue, a push node is reached over ssh.
func nodeActionCommand(action, actionType, connectTo string) string {
	var cmd []string
	if actionType != "pull" && actionType != "feed" {
		cmd = append(cmd, sshCmd...)
		cmd = append(cmd, "opensvc@"+connectTo, "--", "sudo", "nodemgr")
	}
	cmd = append(cmd, action)
	if action == "freeze" || action == "thaw" {
		cmd = append(cmd, "--local")
	}
	return strings.Join(cmd, " ")
}

// PostNodeAction handles POST /nodes/{node_id}/actions: queue an agent action.
func (a *Api) PostNodeAction(c echo.Context, nodeId server.InPathNodeId) error {
	log := echolog.GetLogHandler(c, "PostNodeAction")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.PostNodeActionJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	action := string(body.Action)
	if !nodeActions[action] {
		return JSONProblemf(c, http.StatusBadRequest, "unsupported action %q", action)
	}

	if !IsManager(c) && !HasGroup(c, "NodeExec") {
		return JSONProblemf(c, http.StatusForbidden, "user has no NodeExec privilege")
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

	target, err := odb.NodeActionTargetByID(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot read node action fields", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read node %s", nodeId)
	}
	if target == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	// A node feeding another collector queues its actions there; oc3 has no such
	// push channel, so the caller is told rather than left with a dead entry.
	if target.Collector != "" {
		return JSONProblemf(c, http.StatusBadRequest,
			"node %s is attached to collector %s: queue the action there", target.Nodename, target.Collector)
	}

	connectTo, err := odb.NodeReachableAddress(ctx, target)
	if err != nil {
		log.Error("cannot find a reachable address", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot find an address for node %s", nodeId)
	}

	// An unset action_type means pull, as get_action_type() does.
	actionType := target.ActionType
	if actionType == "" {
		actionType = "pull"
	}
	command := nodeActionCommand(action, actionType, connectTo)

	// Who asked, as the python collector records it: the queue keeps the caller.
	id, err := odb.EnqueueNodeAction(ctx, node.NodeID, actionType, command, connectTo, authUserID(c))
	if err != nil {
		log.Error("cannot queue the action", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot queue action %s", action)
	}

	log.Info("action queued", logkey.NodeID, node.NodeID, "action", action, "action_id", id)

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "node.action",
		User:   userEmail,
		Fmt:    "run %(action)s",
		Dict:   map[string]any{"action": action},
		Level:  "info",
	}
	if parsed, parseErr := uuid.Parse(node.NodeID); parseErr == nil {
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
		"info":    "action " + action + " queued on node " + target.Nodename,
	})
}
