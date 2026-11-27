package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// wakeStart is the start of the container running a virtual machine node,
// queued on its hypervisor, or found queued before and not done yet.
type wakeStart struct {
	Hypervisor string
	Path       string
	RID        string
	ActionID   int64
	Queued     bool
}

// PostAgentWake handles POST /agent/wake: a node asks to wake a powered off
// virtual machine node of its own cluster. The collector finds the container
// resource whose virtual machine has the machine uuid the node reported, on
// a hypervisor reporting to this collector, and queues its start there.
//
// The caller is a node, authenticated as such: it can only name a node of its
// own cluster, and only the start of the one container running that node is
// queued, never another action. A start already queued and not done yet is
// returned rather than queued twice, as every relay of the cluster may ask.
// A node up, as the last status of its cluster says, has nothing queued: the
// answer says it is awake.
func (a *Api) PostAgentWake(c echo.Context, params server.PostAgentWakeParams) error {
	log := echolog.GetLogHandler(c, "PostAgentWake")
	ctx := c.Request().Context()

	if !IsAuthByNode(c) {
		return JSONProblem(c, http.StatusForbidden, "node authentication required")
	}
	if params.Nodename == "" {
		return JSONProblem(c, http.StatusBadRequest, "nodename is required")
	}
	callerID, _ := c.Get(XNodeID).(string)
	if callerID == "" {
		return JSONProblem(c, http.StatusForbidden, "node authentication required")
	}
	caller, err := a.ODB.NodeByNodeID(ctx, callerID)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the calling node", err))
	}
	if caller == nil || caller.ClusterID == "" {
		return JSONProblemf(c, http.StatusForbidden, "the calling node has no cluster")
	}

	// A node of another cluster is answered as one that does not exist: the
	// caller learns nothing of the nodes it can not wake.
	targetID, err := a.ODB.NodeIDByNodenameAndCluster(ctx, params.Nodename, caller.ClusterID)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the node to wake", err))
	}
	if targetID == "" {
		return JSONProblemf(c, http.StatusNotFound, "no node %s in the cluster of %s", params.Nodename, caller.Nodename)
	}

	// A node up has no container to start: every relay of a wake asks, and
	// one may ask once the node is back, as a node itself just back would.
	up, err := a.ODB.NodeUp(ctx, targetID)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the status of the node to wake", err))
	}
	if up {
		awake := true
		return c.JSON(http.StatusOK, server.WakeAgentInfo{Nodename: params.Nodename, Awake: &awake})
	}

	start, err := a.queueWakeContainerStart(actionActor{email: "node " + caller.Nodename}, log, ctx, params.Nodename, targetID)
	if err != nil {
		return actionProblem(c, err)
	}
	return c.JSON(http.StatusOK, server.WakeAgentInfo{
		Nodename:   params.Nodename,
		Hypervisor: start.Hypervisor,
		Path:       start.Path,
		Rid:        start.RID,
		ActionId:   start.ActionID,
		Queued:     start.Queued,
	})
}

// queueWakeContainerStart queues the start of the container running the
// virtual machine node targetID, on its hypervisor, found by the machine uuid
// the node reported. A start queued before and not done yet is returned
// instead, with Queued false. The refusals are actionRefusal errors.
//
// It checks no privilege: the callers did, a node of the cluster of the node
// to wake, or a user responsible for it.
func (a *Api) queueWakeContainerStart(act actionActor, log *slog.Logger, ctx context.Context, nodename, targetID string) (*wakeStart, error) {
	machineUUID, err := a.ODB.NodeMachineUUID(ctx, targetID)
	if err != nil {
		log.Error("cannot read the machine uuid", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the machine uuid of node %s", nodename)
	}
	if machineUUID == "" {
		return nil, refuseAction(http.StatusConflict, "node %s reported no machine uuid: run om node push asset on it while it is up", nodename)
	}

	containers, err := a.ODB.ContainersByMachineUUID(ctx, machineUUID)
	if err != nil {
		log.Error("cannot read the resource info", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the resource info")
	}
	container, err := pickWakeContainer(containers, targetID, nodename, machineUUID)
	if err != nil {
		return nil, err
	}

	hv, err := a.ODB.NodeActionTargetByID(ctx, container.NodeID)
	if err != nil {
		log.Error("cannot read the hypervisor node", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the hypervisor node")
	}
	if hv == nil {
		return nil, refuseAction(http.StatusNotFound, "the hypervisor node of %s is unknown", nodename)
	}
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, container.SvcID, nil, true)
	if err != nil {
		log.Error("cannot read the hypervisor service", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the hypervisor service")
	}
	if svc == nil {
		return nil, refuseAction(http.StatusNotFound, "the service holding the container of %s is unknown", nodename)
	}
	agentVersion, err := a.ODB.NodeAgentVersion(ctx, hv.NodeID)
	if err != nil {
		log.Error("cannot read the agent version", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the hypervisor node")
	}

	start := &wakeStart{Hypervisor: hv.Nodename, Path: svc.Svcname, RID: container.RID}

	// The command the start would queue, every check of the queueing passed,
	// to find one queued before.
	dry := act
	dry.dryRun = true
	planned, err := a.enqueueServiceCommand(dry, log, ctx, svc, hv, agentVersion, "start", container.RID, true)
	if err != nil {
		return nil, err
	}
	if act.dryRun {
		return start, nil
	}
	pendingID, err := a.ODB.PendingServiceActionID(ctx, hv.NodeID, svc.SvcID, planned.Command)
	if err != nil {
		log.Error("cannot read the action queue", logkey.Error, err)
		return nil, refuseAction(http.StatusInternalServerError, "cannot read the action queue")
	}
	if pendingID > 0 {
		start.ActionID = pendingID
		return start, nil
	}

	queued, err := a.enqueueServiceCommand(act, log, ctx, svc, hv, agentVersion, "start", container.RID, true)
	if err != nil {
		return nil, err
	}
	log.Info("wake queued", "nodename", nodename, "hypervisor", hv.Nodename, "path", svc.Svcname, "rid", container.RID, "action_id", queued.ID)
	start.ActionID = queued.ID
	start.Queued = true
	return start, nil
}

// pickWakeContainer returns the one container running the node: none is a
// 404, several a 409 naming them, as starting one of them at random could
// start the wrong virtual machine. A row of the node itself is not one: a node
// does not run in its own container.
func pickWakeContainer(containers []cdb.WakeContainer, targetID, nodename, machineUUID string) (cdb.WakeContainer, error) {
	var l []cdb.WakeContainer
	for _, w := range containers {
		if w.NodeID == targetID || w.RID == "" {
			continue
		}
		l = append(l, w)
	}
	switch len(l) {
	case 0:
		return cdb.WakeContainer{}, refuseAction(http.StatusNotFound,
			"no container reports the machine uuid %s of node %s: its hypervisor must run opensvc and report to this collector", machineUUID, nodename)
	case 1:
		return l[0], nil
	default:
		names := make([]string, len(l))
		for i, w := range l {
			names[i] = fmt.Sprintf("%s:%s@%s", w.SvcID, w.RID, w.NodeID)
		}
		return cdb.WakeContainer{}, refuseAction(http.StatusConflict,
			"%d containers report the machine uuid %s of node %s: %s", len(l), machineUUID, nodename, strings.Join(names, ", "))
	}
}
