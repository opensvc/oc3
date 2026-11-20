package serverhandlers

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteCluster handles DELETE /clusters/{cluster_id}: the cluster, its nodes
// and its services, with what refers to them (see cdb.DeleteClusterCascade).
// The caller holds NodeManager, as to delete a node, and is responsible for
// every node and every service of the cluster, as all of them go.
func (a *Api) DeleteCluster(c echo.Context, clusterId string) error {
	log := echolog.GetLogHandler(c, "DeleteCluster")
	if err := checkNodeDeleter(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	odb := a.ODB
	name, found, err := odb.ClusterName(ctx, clusterId)
	if err != nil {
		log.Error("cannot lookup cluster", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup cluster")
	} else if !found {
		return JSONProblemf(c, http.StatusNotFound, "cluster %s not found", clusterId)
	}
	groups, manager := UserGroupsFromContext(c), IsManager(c)
	if _, notResponsible, err := odb.ClusterNodesResponsibility(ctx, clusterId, groups, manager); err != nil {
		log.Error("cannot check cluster responsibility", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check cluster responsibility")
	} else if len(notResponsible) > 0 {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for every node of cluster %s", name)
	}
	if notResponsible, err := odb.ClusterServicesResponsibility(ctx, clusterId, groups, manager); err != nil {
		log.Error("cannot check cluster responsibility", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check cluster responsibility")
	} else if len(notResponsible) > 0 {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for every service of cluster %s", name)
	}

	log.Info("called", "cluster_id", clusterId)
	var nodeIDs []string
	var services int
	err = odb.RunInTx(ctx, &sql.TxOptions{}, func(tx *cdb.DB) error {
		var err error
		nodeIDs, services, err = tx.DeleteClusterCascade(ctx, clusterId)
		return err
	})
	if err != nil {
		log.Error("cannot delete cluster", "cluster_id", clusterId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete cluster %s", name)
	}
	// The cluster is deleted: a file left behind is logged, not reported to
	// the caller, and is removed again by a deletion of a node of the same id.
	for _, nodeID := range nodeIDs {
		if err := nodeFilesDirs().Remove(nodeID); err != nil {
			log.Warn("cannot remove the node files", logkey.NodeID, nodeID, logkey.Error, err)
		}
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "cluster.delete",
		Fmt:    "delete cluster %(name)s (%(cluster_id)s): %(nodes)s nodes, %(services)s services",
		Dict:   map[string]any{"name": name, "cluster_id": clusterId, "nodes": len(nodeIDs), "services": services},
	})
	return c.NoContent(http.StatusNoContent)
}
