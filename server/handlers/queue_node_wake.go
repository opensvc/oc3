package serverhandlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/google/uuid"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// wakeNodenamePattern are the node names a wake command is queued for: a host
// name, so the command an agent splits in words names one node.
var wakeNodenamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// wakeCommand is the node action a peer up runs to wake the node: om node
// wake asks the collector first, which starts the container of a virtual
// machine node on its hypervisor, then sends Wake-on-LAN magic packets, for a
// physical node.
func wakeCommand(nodename string) (string, error) {
	if !wakeNodenamePattern.MatchString(nodename) {
		return "", refuseAction(http.StatusBadRequest, "node name %q can not be woken: not a host name", nodename)
	}
	return "wake --node " + nodename, nil
}

// queueNodeWake queues the wake of a node down, as the node can not dequeue
// its own action: the om node wake command on a peer of its cluster up now.
// With no peer up, for a virtual machine node, it queues the start of its
// container on its hypervisor instead. A node up has nothing queued, a notice
// saying so. The caller checked the privilege and the responsibility for the
// node.
//
// Up is what the last status of the cluster says, not the last_comm of the
// node: the pings of the cluster refresh the one of every node, a peer
// powered off included, which would be picked to run a wake it never
// dequeues.
func (a *Api) queueNodeWake(act actionActor, log *slog.Logger, ctx context.Context, node *cdb.DBNode) (*queuedAction, error) {
	odb := a.ODB
	command, err := wakeCommand(node.Nodename)
	if err != nil {
		return nil, err
	}
	if node.ClusterID == "" {
		return nil, refuseAction(http.StatusConflict, "node %s belongs to no cluster: no peer to wake it from", node.Nodename)
	}

	up, err := odb.NodeUp(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot tell whether the node is up", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the status of node %s", node.Nodename)
	}
	if up {
		return &queuedAction{Command: command, Notice: "node " + node.Nodename + " is already up: nothing queued"}, nil
	}

	peer, err := odb.ClusterUpNodeExcept(ctx, node.ClusterID, node.NodeID)
	if err != nil {
		log.Error("cannot find a peer up", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot find a node up in the cluster of %s", node.Nodename)
	}
	if peer == nil {
		return a.queueNodeWakeByHypervisor(act, log, ctx, node)
	}
	if peer.Collector != "" {
		return nil, refuseAction(http.StatusBadRequest,
			"node %s is attached to collector %s: queue the action there", peer.Nodename, peer.Collector)
	}
	connectTo, err := odb.NodeReachableAddress(ctx, peer)
	if err != nil {
		log.Error("cannot find a reachable address", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot find an address for node %s", peer.Nodename)
	}
	actionType := actionTypeOf(peer)
	if act.dryRun {
		return &queuedAction{Command: command, NodeID: peer.NodeID}, nil
	}

	// Asked twice, as from a multi-selection of nodes of the same cluster
	// selected again: the wake queued before and not run yet answers.
	pendingID, err := odb.PendingNodeActionID(ctx, peer.NodeID, command)
	if err != nil {
		log.Error("cannot read the action queue", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the action queue")
	}
	if pendingID > 0 {
		return &queuedAction{
			ID:      pendingID,
			Command: command,
			NodeID:  peer.NodeID,
			Info:    "wake of node " + node.Nodename + " already queued on node " + peer.Nodename,
		}, nil
	}

	id, err := odb.EnqueueNodeAction(ctx, peer.NodeID, actionType, command, connectTo, act.userID)
	if err != nil {
		log.Error("cannot queue the action", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot queue the wake of node %s", node.Nodename)
	}
	log.Info("wake queued", logkey.NodeID, node.NodeID, "peer", peer.Nodename, "action_id", id)
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}

	// The log entry is the one of the node woken, naming the peer running it.
	logEntry := cdb.LogEntry{
		Action: "node.action",
		User:   act.email,
		Fmt:    "run %(action)s from %(peer)s",
		Dict:   map[string]any{"action": "wake", "peer": peer.Nodename},
		Level:  "info",
	}
	if parsed, parseErr := uuid.Parse(node.NodeID); parseErr == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	return &queuedAction{
		ID:      id,
		Command: command,
		NodeID:  peer.NodeID,
		Info:    "wake of node " + node.Nodename + " queued on node " + peer.Nodename,
	}, nil
}

// queueNodeWakeByHypervisor wakes a node of a cluster with no node up, as
// every node of a two nodes cluster powered off: the start of the container
// running it, for a virtual machine node. A physical node has no way.
func (a *Api) queueNodeWakeByHypervisor(act actionActor, log *slog.Logger, ctx context.Context, node *cdb.DBNode) (*queuedAction, error) {
	start, err := a.queueWakeContainerStart(act, log, ctx, node.Nodename, node.NodeID)
	if err != nil {
		var refusal *actionRefusal
		if errors.As(err, &refusal) && refusal.status != http.StatusInternalServerError {
			return nil, refuseAction(http.StatusConflict,
				"no node up in the cluster of %s to wake it from, and no container to start: %s", node.Nodename, refusal.msg)
		}
		return nil, err
	}
	info := "start of " + start.Path + " " + start.RID + " queued on node " + start.Hypervisor + " to wake node " + node.Nodename
	if !start.Queued {
		info = "start of " + start.Path + " " + start.RID + " already queued on node " + start.Hypervisor + " to wake node " + node.Nodename
	}
	return &queuedAction{ID: start.ActionID, Command: "start --rid " + start.RID, Info: info}, nil
}
