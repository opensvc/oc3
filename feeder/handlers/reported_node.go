package feederhandlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/logkey"
)

type (
	// clusterNodeIDLookup returns the node id of the node nodename of the
	// cluster clusterID.
	clusterNodeIDLookup func(ctx context.Context, clusterID, nodename string) (string, error)
)

var (
	errNodeNotInCluster = errors.New("not a node of the cluster of the authenticated node")
)

// reportedNodeID returns the node id of the node data is reported for.
//
// A node reports its own data, and the authenticated node is the one. The
// om3 collector speaker also reports the data of the other nodes of its
// cluster, naming the node in the payload: its node id is then looked up
// among the nodes of the cluster of the authenticated node, so a node can
// not report for a node of another cluster.
func reportedNodeID(ctx context.Context, lookup clusterNodeIDLookup, authNodeID, authNodename, clusterID string, nodename *string) (string, error) {
	if nodename == nil || *nodename == "" || strings.EqualFold(*nodename, authNodename) {
		return authNodeID, nil
	}
	if clusterID == "" {
		return "", fmt.Errorf("%s: %w", *nodename, errNodeNotInCluster)
	}
	nodeID, err := lookup(ctx, clusterID, *nodename)
	if err != nil {
		return "", err
	}
	if nodeID == "" {
		return "", fmt.Errorf("%s: %w", *nodename, errNodeNotInCluster)
	}
	return nodeID, nil
}

// lookupClusterNodeID returns the node id of the node nodename of the
// cluster clusterID, and an empty string when the cluster has no such node.
func (a *Api) lookupClusterNodeID(ctx context.Context, clusterID, nodename string) (string, error) {
	nodes, err := a.ODB.NodesFromClusterIDWithNodenames(ctx, clusterID, []string{nodename})
	if err != nil {
		return "", err
	}
	switch len(nodes) {
	case 0:
		return "", nil
	case 1:
		return nodes[0].NodeID, nil
	default:
		return "", fmt.Errorf("%s: %d nodes of cluster %s have this name", nodename, len(nodes), clusterID)
	}
}

// reportedNodeIDOrProblem returns the node id of the node data is reported
// for, or, when it is not ok, the response the handler must return: 403
// for a node out of the cluster of the authenticated node.
func (a *Api) reportedNodeIDOrProblem(c echo.Context, log *slog.Logger, authNodeID, clusterID string, nodename *string) (string, bool, error) {
	nodeID, err := reportedNodeID(c.Request().Context(), a.lookupClusterNodeID, authNodeID, nodenameFromContext(c), clusterID, nodename)
	switch {
	case errors.Is(err, errNodeNotInCluster):
		log.Info("refused report for another node", logkey.Error, err)
		return "", false, JSONProblem(c, http.StatusForbidden, err.Error())
	case err != nil:
		log.Error("lookup the reported node", logkey.Error, err)
		return "", false, JSONError(c)
	}
	return nodeID, true, nil
}
