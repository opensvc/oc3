package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/logkey"
)

type (
	// nodeEnroll is the registration of a joining node by the enroller, a
	// node of the cluster it joins.
	nodeEnroll struct {
		// nodename is the name of the joining node.
		nodename string

		// prevUUID is the uuid the joining node had before the join.
		prevUUID string

		// app, teamResponsible and clusterID are the ones of the enroller,
		// which the joining node takes.
		app             string
		teamResponsible string
		clusterID       string
	}

	// nodeEnrollStore is the database access of enrollNode.
	nodeEnrollStore interface {
		NodeIDsByNodenameAndAuthUUID(ctx context.Context, nodename, nodeUUID string) ([]string, error)
		DetachNodenameFromCluster(ctx context.Context, nodename, clusterID, keepNodeID string) error
		UpdateNodeFields(ctx context.Context, nodeID string, fields map[string]any) error
		SetAuthNodeUUID(ctx context.Context, nodeID, nodename, nodeUUID string) error
	}
)

var (
	// errEnrollNotRegistered is a joining node not found by its nodename and
	// its previous uuid: an enroller may only register a node already
	// registered, not create any, nor take one it has no credentials of.
	errEnrollNotRegistered = errors.New("the joining node is not registered: register it with 'om node register'")

	// errEnrollAmbiguous is a joining node found more than once by its
	// nodename and its previous uuid.
	errEnrollAmbiguous = errors.New("the joining node is registered more than once")
)

// enrollNode moves the registered joining node to the app, the team
// responsible and the cluster of the enroller, and gives it a new uuid. It
// returns the node id and the new uuid of the joining node.
//
// The joining node is found by its nodename and its previous uuid.
func enrollNode(ctx context.Context, store nodeEnrollStore, e nodeEnroll, newUUID func() string) (nodeID, nodeUUID string, err error) {
	nodeID, err = enrollFindNode(ctx, store, e)
	if err != nil {
		return "", "", err
	}
	// A stale node row may have the nodename in the enroller cluster: a
	// former member, never cleaned. The cluster nodes are resolved by
	// cluster_id and nodename, which must designate one node: the feeder
	// refuses the reports of a nodename found twice, and the worker fails the
	// daemon status of the whole cluster on a duplicate nodename. So the
	// stale rows leave the cluster before the joining node enters it.
	if err := store.DetachNodenameFromCluster(ctx, e.nodename, e.clusterID, nodeID); err != nil {
		return "", "", err
	}
	fields := map[string]any{
		"app":              e.app,
		"team_responsible": e.teamResponsible,
		"cluster_id":       e.clusterID,
	}
	if err := store.UpdateNodeFields(ctx, nodeID, fields); err != nil {
		return "", "", err
	}
	nodeUUID = newUUID()
	if err := store.SetAuthNodeUUID(ctx, nodeID, e.nodename, nodeUUID); err != nil {
		return "", "", err
	}
	return nodeID, nodeUUID, nil
}

// enrollFindNode returns the node id of the joining node: the one with its
// nodename authenticated by its previous uuid. The uuid alone is not a key:
// it is a password, which other nodes may share.
func enrollFindNode(ctx context.Context, store nodeEnrollStore, e nodeEnroll) (string, error) {
	if e.prevUUID == "" {
		return "", errEnrollNotRegistered
	}
	nodeIDs, err := store.NodeIDsByNodenameAndAuthUUID(ctx, e.nodename, e.prevUUID)
	if err != nil {
		return "", err
	}
	switch len(nodeIDs) {
	case 0:
		return "", errEnrollNotRegistered
	case 1:
		return nodeIDs[0], nil
	default:
		return "", fmt.Errorf("%w: %d nodes named %s with its previous uuid", errEnrollAmbiguous, len(nodeIDs), e.nodename)
	}
}

// postAuthNodeEnroll handles the POST /auth/node of an enroller registering
// a joining node of its cluster.
func (a *Api) postAuthNodeEnroll(c echo.Context, log *slog.Logger, body server.PostAuthNodeJSONBody) error {
	if body.Uuid == nil || *body.Uuid == "" {
		return JSONProblemf(c, http.StatusBadRequest, "uuid is required to register a joining node")
	}
	enrollerNodeID, _ := c.Get(XNodeID).(string)
	enroller, _ := c.Get(XNodename).(string)
	clusterID, _ := c.Get(XClusterID).(string)
	app, _ := c.Get(XApp).(string)
	if clusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "refused: authenticated node doesn't define cluster id")
	}
	if !viper.GetBool("server.allow_node_enroll") {
		// A node the collector bootstrapped into the cluster of the enroller
		// is enrolled all the same: the collector provisioned it for that
		// cluster, a member enrolling it from its enrollment.
		if ok, err := a.bootstrappedJoiningNode(c, body.Nodename, *body.Uuid, clusterID); err != nil {
			log.Error("cannot check the bootstrap of the joining node", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "enroller lookup failed")
		} else if !ok {
			return JSONProblemf(c, http.StatusForbidden, "node enrollment is disabled")
		}
	}
	if app == "" {
		return JSONProblemf(c, http.StatusConflict, "refused: authenticated node doesn't define app")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	teamResponsible, _, err := a.ODB.NodeTeamResponsible(ctx, enrollerNodeID)
	if err != nil {
		log.Error("failed to find the enroller team responsible", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "enroller lookup failed")
	}
	e := nodeEnroll{
		nodename:        body.Nodename,
		prevUUID:        *body.Uuid,
		app:             app,
		teamResponsible: teamResponsible,
		clusterID:       clusterID,
	}

	// In a transaction run again on a deadlock: the update of the cluster id
	// of the joining node may meet the worker writing the nodes rows of the
	// first feeds of its peers. RunInTx returns the error of a failed commit:
	// the joining node must not get a uuid the collector does not have.
	var nodeID, nodeUUID string
	err = a.ODB.RunInTx(ctx, &sql.TxOptions{}, func(tx *cdb.DB) error {
		var err error
		nodeID, nodeUUID, err = enrollNode(ctx, tx, e, uuid.NewString)
		return err
	})
	switch {
	case errors.Is(err, errEnrollNotRegistered):
		log.Info("enroll refused", "nodename", e.nodename, logkey.Error, err)
		a.nodeRegisterLog(c, ctx, log, "warning", "node %(node)s enrollment by node %(enroller)s refused: %(reason)s", "",
			map[string]any{"node": e.nodename, "enroller": enroller, "reason": err.Error()})
		return JSONProblem(c, http.StatusForbidden, err.Error())
	case errors.Is(err, errEnrollAmbiguous):
		log.Warn("enroll refused", "nodename", e.nodename, logkey.Error, err)
		a.nodeRegisterLog(c, ctx, log, "warning", "node %(node)s enrollment by node %(enroller)s refused: %(reason)s", "",
			map[string]any{"node": e.nodename, "enroller": enroller, "reason": err.Error()})
		return JSONProblem(c, http.StatusConflict, err.Error())
	case err != nil:
		log.Error("failed to enroll node", "nodename", e.nodename, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot enroll node")
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("enroll node can't notify changes", logkey.Error, err)
	}
	log.Info("node is enrolled", "nodename", e.nodename, logkey.NodeID, nodeID, "cluster_id", clusterID, "app", app)
	a.nodeRegisterLog(c, ctx, log, "info", "node %(node)s enrolled into cluster %(cluster_id)s by node %(enroller)s", nodeID,
		map[string]any{"node": e.nodename, "cluster_id": clusterID, "enroller": enroller})
	return c.JSON(http.StatusOK, map[string]any{
		"uuid": nodeUUID,
		"info": "node is enrolled",
	})
}

// bootstrappedJoiningNode returns whether the joining node, found by its
// nodename and its uuid, was bootstrapped by the collector into clusterID.
func (a *Api) bootstrappedJoiningNode(c echo.Context, nodename, nodeUUID, clusterID string) (bool, error) {
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	nodeIDs, err := a.ODB.NodeIDsByNodenameAndAuthUUID(ctx, nodename, nodeUUID)
	if err != nil || len(nodeIDs) != 1 {
		return false, err
	}
	return a.ODB.NodeBootstrappedInto(ctx, nodeIDs[0], clusterID)
}
