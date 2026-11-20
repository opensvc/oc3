package serverhandlers

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// The provisioning of clusters and nodes by the collector: an operator declares
// a cluster and its nodes and issues a bootstrap token per node; a node being
// provisioned trades its token for its collector credentials and the
// configuration of its cluster (POST /auth/node/bootstrap). The first one
// creates the cluster, the seed; the others hand a join token of their daemon
// (POST /clusters/{id}/enrollments), which a member of the cluster claims to
// enroll them. The collector holds no secret of the clusters: the nodes trust
// each other by the join of om3.

const (
	// provisioningPrivilege is the privilege of the operators declaring the
	// clusters and their nodes, besides Manager.
	provisioningPrivilege = "NodeManager"

	bootstrapTokenTTLDefault = 60 * time.Minute
	bootstrapTokenTTLMax     = 24 * time.Hour

	// enrollmentTTL is how long a pending enrollment waits for a member, and
	// the cache keeps its join token: the token of the join role an om3
	// daemon creates lives about as long.
	enrollmentTTL = 15 * time.Minute

	// enrollmentsListLimit is the number of enrollments listed.
	enrollmentsListLimit = 100

	// joinTokenMaxLen bounds a join token: a jwt with the certificate chain
	// of the cluster ca.
	joinTokenMaxLen = 64 * 1024
)

var (
	// clusterNameRegexp is a name om3 accepts for a cluster.
	clusterNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

	// heartbeatKeywordRegexp is a keyword of a hb section.
	heartbeatKeywordRegexp = regexp.MustCompile(`^[a-z][a-z0-9_]*(@[a-zA-Z0-9._-]+)?$`)

	// errBootstrapDenied is the answer to every bootstrap with a token not
	// valid for the nodename: unknown, used, expired or issued for another
	// node, not told apart.
	errBootstrapDenied = errors.New("invalid bootstrap token")
)

// requireProvisioning checks the caller may declare clusters and nodes.
func requireProvisioning(c echo.Context) error {
	return requirePrivilege(c, provisioningPrivilege)
}

// hashBootstrapToken is the hash a bootstrap token is kept by.
func hashBootstrapToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// newBootstrapToken returns a random token of 256 bits, url safe.
func newBootstrapToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// checkHeartbeats validates the hb sections of a declaration: a type each,
// keywords as om3 names them, values bounded.
func checkHeartbeats(l []server.ClusterHeartbeat) error {
	for i, hb := range l {
		if strings.TrimSpace(hb.Type) == "" {
			return fmt.Errorf("heartbeat %d: type is required", i+1)
		}
		for k, v := range hb.AdditionalProperties {
			if !heartbeatKeywordRegexp.MatchString(k) {
				return fmt.Errorf("heartbeat %d: invalid keyword %q", i+1, k)
			}
			if len(v) > 4096 {
				return fmt.Errorf("heartbeat %d: value of %s too long", i+1, k)
			}
		}
	}
	return nil
}

// encodeHeartbeats is the JSON the heartbeats of a declaration are kept as.
func encodeHeartbeats(l *[]server.ClusterHeartbeat) (string, error) {
	hbs := []server.ClusterHeartbeat{}
	if l != nil {
		hbs = *l
	}
	if err := checkHeartbeats(hbs); err != nil {
		return "", err
	}
	b, err := json.Marshal(hbs)
	return string(b), err
}

// decodeHeartbeats reads the heartbeats of a declaration.
func decodeHeartbeats(s string) []server.ClusterHeartbeat {
	hbs := []server.ClusterHeartbeat{}
	if s != "" {
		_ = json.Unmarshal([]byte(s), &hbs)
	}
	return hbs
}

func clusterDeclarationResponse(d *cdb.DBClusterDeclaration) server.ClusterDeclaration {
	return server.ClusterDeclaration{
		ClusterId:    d.ClusterID,
		ClusterName:  d.ClusterName,
		App:          d.App,
		Heartbeats:   decodeHeartbeats(d.Heartbeats),
		SeedNodeId:   d.SeedNodeID,
		SeedNodename: d.SeedNodename,
		CreatedBy:    d.CreatedBy,
		Created:      d.Created,
		Updated:      d.Updated,
	}
}

func clusterEnrollmentResponse(e *cdb.DBClusterEnrollment) server.ClusterEnrollment {
	return server.ClusterEnrollment{
		Id:        e.ID,
		ClusterId: e.ClusterID,
		NodeId:    e.NodeID,
		Nodename:  e.Nodename,
		NodeAddr:  e.NodeAddr,
		State:     server.ClusterEnrollmentState(e.State),
		ClaimedBy: e.ClaimedBy,
		Message:   e.Message,
		Created:   e.Created,
		Updated:   e.Updated,
		Expires:   e.Expires,
	}
}

// PostClusters handles POST /clusters: the declaration of a cluster.
func (a *Api) PostClusters(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostClusters")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.ClusterDeclarationInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if !clusterNameRegexp.MatchString(body.ClusterName) {
		return JSONProblemf(c, http.StatusBadRequest, "invalid cluster_name %q", body.ClusterName)
	}
	heartbeats, err := encodeHeartbeats(body.Heartbeats)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	d := cdb.DBClusterDeclaration{ClusterName: body.ClusterName, Heartbeats: heartbeats}
	d.CreatedBy, _ = c.Get(XUserEmail).(string)
	if body.App != nil && *body.App != "" {
		if ok, err := a.ODB.AppExists(ctx, *body.App); err != nil {
			log.Error("cannot verify app", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot verify app")
		} else if !ok {
			return JSONProblemf(c, http.StatusBadRequest, "unknown app %s", *body.App)
		}
		d.App = *body.App
	}
	if body.ClusterId != nil && *body.ClusterId != "" {
		id, err := uuid.Parse(*body.ClusterId)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid cluster_id: %s", err)
		}
		d.ClusterID = id.String()
		// A cluster reporting already is created: its nodes are the seed, the
		// nodes bootstrapped join them.
		name, reporting, err := a.ODB.ClusterReporting(ctx, d.ClusterID)
		if err != nil {
			log.Error("cannot look up the cluster", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot look up the cluster")
		}
		if reporting {
			if name != d.ClusterName {
				return JSONProblemf(c, http.StatusBadRequest, "the cluster %s reports the name %s", d.ClusterID, name)
			}
			seed, err := a.ODB.ClusterReportingNodeID(ctx, d.ClusterID)
			if err != nil {
				log.Error("cannot look up the nodes of the cluster", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot look up the nodes of the cluster")
			}
			if seed != "" {
				d.SeedNodeID = &seed
			}
		}
	} else {
		d.ClusterID = uuid.New().String()
	}
	// The members of a cluster reporting already enroll the nodes
	// bootstrapped into it: declaring it takes the responsibility of its
	// nodes, as of the app of the nodes declared.
	if err := requireProvisioningApps(c, ctx, log, a.ODB, d.ClusterID, d.App); err != nil {
		return err
	}
	if err := a.ODB.InsertClusterDeclaration(ctx, d); errors.Is(err, cdb.ErrClusterDeclared) {
		return JSONProblemf(c, http.StatusConflict, "the cluster %s is declared already", d.ClusterID)
	} else if err != nil {
		log.Error("cannot declare the cluster", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot declare the cluster")
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "cluster.declare", Fmt: "cluster %(cluster)s %(cluster_id)s declared",
		Dict: map[string]any{"cluster": d.ClusterName, "cluster_id": d.ClusterID},
	})
	saved, err := a.ODB.ClusterDeclaration(ctx, d.ClusterID)
	if err != nil || saved == nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	}
	return c.JSON(http.StatusCreated, clusterDeclarationResponse(saved))
}

// GetClusterDeclaration handles GET /clusters/{cluster_id}/declaration.
func (a *Api) GetClusterDeclaration(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "GetClusterDeclaration")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	d, err := a.ODB.ClusterDeclaration(ctx, clusterId)
	if err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	if err := requireProvisioningVisible(c, ctx, log, a.ODB, clusterId, d.App); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, clusterDeclarationResponse(d))
}

// PutClusterDeclaration handles PUT /clusters/{cluster_id}/declaration.
func (a *Api) PutClusterDeclaration(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "PutClusterDeclaration")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var body server.ClusterDeclarationUpdate
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	d, err := a.ODB.ClusterDeclaration(ctx, clusterId)
	if err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	app, heartbeats := d.App, d.Heartbeats
	if body.App != nil {
		app = *body.App
	}
	if err := requireProvisioningApps(c, ctx, log, a.ODB, clusterId, d.App, app); err != nil {
		return err
	}
	if body.App != nil {
		if app != "" {
			if ok, err := a.ODB.AppExists(ctx, app); err != nil {
				log.Error("cannot verify app", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot verify app")
			} else if !ok {
				return JSONProblemf(c, http.StatusBadRequest, "unknown app %s", app)
			}
		}
	}
	if body.Heartbeats != nil {
		if heartbeats, err = encodeHeartbeats(body.Heartbeats); err != nil {
			return JSONProblem(c, http.StatusBadRequest, err.Error())
		}
	}
	if found, err := a.ODB.UpdateClusterDeclaration(ctx, clusterId, app, heartbeats); err != nil {
		log.Error("cannot update the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update the declaration")
	} else if !found {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "cluster.declare", Fmt: "cluster %(cluster)s %(cluster_id)s declaration changed",
		Dict: map[string]any{"cluster": d.ClusterName, "cluster_id": d.ClusterID},
	})
	if d, err = a.ODB.ClusterDeclaration(ctx, clusterId); err != nil || d == nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	}
	return c.JSON(http.StatusOK, clusterDeclarationResponse(d))
}

// DeleteClusterDeclaration handles DELETE /clusters/{cluster_id}/declaration.
func (a *Api) DeleteClusterDeclaration(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "DeleteClusterDeclaration")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if d, err := a.ODB.ClusterDeclaration(ctx, clusterId); err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	} else if err := requireProvisioningApps(c, ctx, log, a.ODB, clusterId, d.App); err != nil {
		return err
	}
	found, err := a.ODB.DeleteClusterDeclaration(ctx, clusterId)
	if err != nil {
		log.Error("cannot delete the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the declaration")
	} else if !found {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "cluster.declare", Fmt: "cluster %(cluster_id)s declaration deleted",
		Dict: map[string]any{"cluster_id": clusterId},
	})
	return c.NoContent(http.StatusNoContent)
}

// PostNodeBootstrapToken handles POST /nodes/{node_id}/bootstrap_token.
func (a *Api) PostNodeBootstrapToken(c echo.Context, nodeId server.InPathNodeId) error {
	log := echolog.GetLogHandler(c, "PostNodeBootstrapToken")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var body server.NodeBootstrapTokenRequest
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&body); err != nil {
			return JSONProblem(c, http.StatusBadRequest, err.Error())
		}
	}
	ttl := bootstrapTokenTTLDefault
	if body.TtlMinutes != nil {
		ttl = time.Duration(*body.TtlMinutes) * time.Minute
		if ttl <= 0 || ttl > bootstrapTokenTTLMax {
			return JSONProblemf(c, http.StatusBadRequest, "ttl_minutes must be between 1 and %d", int(bootstrapTokenTTLMax/time.Minute))
		}
	}
	node, err := a.ODB.NodeByNodeID(ctx, nodeId)
	if err != nil {
		log.Error("cannot look up the node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot look up the node")
	} else if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	if node.ClusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "the node %s has no cluster: set its cluster_id", node.Nodename)
	}
	if d, err := a.ODB.ClusterDeclaration(ctx, node.ClusterID); err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusConflict, "the cluster %s of the node is not declared", node.ClusterID)
	} else if err := requireProvisioningApps(c, ctx, log, a.ODB, node.ClusterID, d.App, node.App); err != nil {
		// The token gives whoever holds it the identity of the node.
		return err
	}
	token, err := newBootstrapToken()
	if err != nil {
		log.Error("cannot generate a token", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot generate a token")
	}
	createdBy, _ := c.Get(XUserEmail).(string)
	expires, err := a.ODB.InsertNodeBootstrapToken(ctx, node.NodeID, node.ClusterID, hashBootstrapToken(token), ttl, createdBy)
	if err != nil {
		log.Error("cannot record the token", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot record the token")
	}
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "node.bootstrap", Fmt: "bootstrap token issued for node %(node)s, valid until %(expires)s",
		Dict:   map[string]any{"node": node.Nodename, "expires": expires.Format(time.RFC3339)},
		NodeID: uuidRef(node.NodeID),
	})
	return c.JSON(http.StatusCreated, server.NodeBootstrapToken{Token: token, Expires: expires})
}

// bootstrapStore is the database access of bootstrapNode.
type bootstrapStore interface {
	BootstrapTokenNode(ctx context.Context, tokenHash string) (*cdb.DBBootstrapNode, error)
	ClusterDeclaration(ctx context.Context, clusterID string) (*cdb.DBClusterDeclaration, error)
	UseBootstrapToken(ctx context.Context, tokenHash string) (bool, error)
	ElectClusterSeed(ctx context.Context, clusterID, nodeID string) (string, error)
	SetAuthNodeUUID(ctx context.Context, nodeID, nodename, nodeUUID string) error
	UpdateNodeFields(ctx context.Context, nodeID string, fields map[string]any) error
}

// bootstrapNode trades a bootstrap token for the credentials of the node it
// was issued for and the configuration of its cluster. errBootstrapDenied for
// a token not valid for nodename, which is not used then.
func bootstrapNode(ctx context.Context, store bootstrapStore, nodename, token string, newUUID func() string) (*server.NodeBootstrap, error) {
	if nodename == "" || token == "" {
		return nil, errBootstrapDenied
	}
	tokenHash := hashBootstrapToken(token)
	node, err := store.BootstrapTokenNode(ctx, tokenHash)
	if err != nil {
		return nil, err
	} else if node == nil || !strings.EqualFold(node.Nodename, nodename) {
		return nil, errBootstrapDenied
	}
	d, err := store.ClusterDeclaration(ctx, node.ClusterID)
	if err != nil {
		return nil, err
	} else if d == nil {
		return nil, httpErrorf(http.StatusConflict, "the cluster %s of the node is no longer declared", node.ClusterID)
	}
	if used, err := store.UseBootstrapToken(ctx, tokenHash); err != nil {
		return nil, err
	} else if !used {
		return nil, errBootstrapDenied
	}
	seed, err := store.ElectClusterSeed(ctx, d.ClusterID, node.NodeID)
	if err != nil {
		return nil, err
	}
	cluster := server.NodeBootstrapCluster{
		Id:         d.ClusterID,
		Name:       d.ClusterName,
		Role:       server.Seed,
		Heartbeats: decodeHeartbeats(d.Heartbeats),
	}
	if seed != node.NodeID {
		cluster.Role = server.Join
		// The declaration again, for the name of the seed elected meanwhile.
		if d, err = store.ClusterDeclaration(ctx, d.ClusterID); err != nil {
			return nil, err
		} else if d != nil && d.SeedNodename != nil {
			cluster.SeedNodename = d.SeedNodename
		}
	}
	nodeUUID := newUUID()
	if err := store.SetAuthNodeUUID(ctx, node.NodeID, node.Nodename, nodeUUID); err != nil {
		return nil, err
	}
	// The node row names the cluster declared, should it have changed since
	// the token was issued.
	if err := store.UpdateNodeFields(ctx, node.NodeID, map[string]any{"cluster_id": d.ClusterID}); err != nil {
		return nil, err
	}
	app := node.App
	if app == "" {
		app = d.App
	}
	return &server.NodeBootstrap{
		NodeId:   node.NodeID,
		Nodename: node.Nodename,
		Uuid:     nodeUUID,
		App:      app,
		Cluster:  cluster,
	}, nil
}

// PostAuthNodeBootstrap handles POST /auth/node/bootstrap.
func (a *Api) PostAuthNodeBootstrap(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostAuthNodeBootstrap")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var body server.NodeBootstrapRequest
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	result, err := bootstrapNode(ctx, a.ODB, body.Nodename, body.Token, func() string { return uuid.New().String() })
	var httpErr *httpError
	switch {
	case errors.Is(err, errBootstrapDenied):
		log.Info("bootstrap denied", "nodename", body.Nodename)
		return JSONProblem(c, http.StatusForbidden, err.Error())
	case errors.As(err, &httpErr):
		return httpProblem(c, err)
	case err != nil:
		log.Error("cannot bootstrap the node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot bootstrap the node")
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	a.nodeRegisterLog(c, ctx, log, "info", "node %(node)s bootstrapped to %(role)s cluster %(cluster)s", result.NodeId,
		map[string]any{"node": result.Nodename, "role": string(result.Cluster.Role), "cluster": result.Cluster.Name})
	return c.JSON(http.StatusOK, result)
}

// callerNode returns the node authenticated, "" for another caller.
func callerNode(c echo.Context) (nodeID, nodename, clusterID string) {
	if !IsAuthByNode(c) {
		return "", "", ""
	}
	nodeID, _ = c.Get(XNodeID).(string)
	nodename, _ = c.Get(XNodename).(string)
	clusterID, _ = c.Get(XClusterID).(string)
	return
}

// mayReadEnrollments checks the caller may read the enrollments of a cluster:
// a node of the cluster, or one bootstrapped into it; an operator; a user who
// sees the cluster.
func (a *Api) mayReadEnrollments(c echo.Context, ctx context.Context, clusterID string) (bool, error) {
	if nodeID, _, nodeClusterID := callerNode(c); nodeID != "" {
		if nodeClusterID == clusterID {
			return true, nil
		}
		return a.ODB.NodeBootstrappedInto(ctx, nodeID, clusterID)
	}
	if !IsAuthByUser(c) {
		return false, nil
	}
	if IsManager(c) || HasGroup(c, provisioningPrivilege) {
		return true, nil
	}
	rows, err := a.ODB.GetCluster(ctx, clusterID, cdb.ListParams{
		Groups: UserGroupsFromContext(c), Props: []string{"cluster_id"},
		SelectExprs: []string{"clusters.cluster_id"}, Limit: 1,
	})
	return len(rows) > 0, err
}

// GetClusterEnrollments handles GET /clusters/{cluster_id}/enrollments.
func (a *Api) GetClusterEnrollments(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "GetClusterEnrollments")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if ok, err := a.mayReadEnrollments(c, ctx, clusterId); err != nil {
		log.Error("cannot check access", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check access")
	} else if !ok {
		return JSONProblemf(c, http.StatusForbidden, "not allowed to read the enrollments of cluster %s", clusterId)
	}
	if err := a.ODB.ExpireClusterEnrollments(ctx, clusterId); err != nil {
		log.Error("cannot expire the enrollments", logkey.Error, err)
	}
	l, err := a.ODB.ClusterEnrollments(ctx, clusterId, enrollmentsListLimit)
	if err != nil {
		log.Error("cannot list the enrollments", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list the enrollments")
	}
	data := make([]server.ClusterEnrollment, len(l))
	for i, e := range l {
		data[i] = clusterEnrollmentResponse(e)
	}
	return c.JSON(http.StatusOK, server.ClusterEnrollmentList{Data: data})
}

// GetClusterEnrollment handles GET /clusters/{cluster_id}/enrollments/{enrollment_id}.
func (a *Api) GetClusterEnrollment(c echo.Context, clusterId server.InPathClusterId, enrollmentId server.InPathEnrollmentId) error {
	log := echolog.GetLogHandler(c, "GetClusterEnrollment")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if ok, err := a.mayReadEnrollments(c, ctx, clusterId); err != nil {
		log.Error("cannot check access", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check access")
	} else if !ok {
		return JSONProblemf(c, http.StatusForbidden, "not allowed to read the enrollments of cluster %s", clusterId)
	}
	if err := a.ODB.ExpireClusterEnrollments(ctx, clusterId); err != nil {
		log.Error("cannot expire the enrollments", logkey.Error, err)
	}
	e, err := a.ODB.ClusterEnrollment(ctx, clusterId, enrollmentId)
	if err != nil {
		log.Error("cannot read the enrollment", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the enrollment")
	} else if e == nil {
		return JSONProblemf(c, http.StatusNotFound, "enrollment %d not found", enrollmentId)
	}
	return c.JSON(http.StatusOK, clusterEnrollmentResponse(e))
}

// PostClusterEnrollment handles POST /clusters/{cluster_id}/enrollments: a
// bootstrapped node asks to join its cluster.
func (a *Api) PostClusterEnrollment(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "PostClusterEnrollment")
	nodeID, nodename, _ := callerNode(c)
	if nodeID == "" {
		return JSONProblemf(c, http.StatusUnauthorized, "node authentication required")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var body server.ClusterEnrollmentRequest
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.JoinToken == "" || len(body.JoinToken) > joinTokenMaxLen {
		return JSONProblemf(c, http.StatusBadRequest, "join_token is required, %d bytes at most", joinTokenMaxLen)
	}
	nodeAddr := nodename
	if body.NodeAddr != nil && strings.TrimSpace(*body.NodeAddr) != "" {
		nodeAddr = strings.TrimSpace(*body.NodeAddr)
	}
	if len(nodeAddr) > 255 {
		return JSONProblemf(c, http.StatusBadRequest, "node_addr too long")
	}
	d, err := a.ODB.ClusterDeclaration(ctx, clusterId)
	if err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	if ok, err := a.ODB.NodeBootstrappedInto(ctx, nodeID, clusterId); err != nil {
		log.Error("cannot check the bootstrap of the node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check the bootstrap of the node")
	} else if !ok {
		return JSONProblemf(c, http.StatusForbidden, "the node %s was not bootstrapped into cluster %s", nodename, clusterId)
	}
	if d.SeedNodeID != nil && *d.SeedNodeID == nodeID {
		return JSONProblemf(c, http.StatusForbidden, "the node %s is the seed of cluster %s: it creates it", nodename, clusterId)
	}
	e, err := a.ODB.InsertClusterEnrollment(ctx, clusterId, nodeID, nodename, nodeAddr, enrollmentTTL)
	if err != nil || e == nil {
		log.Error("cannot record the enrollment", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot record the enrollment")
	}
	key := cachekeys.EnrollmentTokenPrefix + strconv.FormatInt(e.ID, 10)
	if err := a.Redis.Set(ctx, key, body.JoinToken, enrollmentTTL).Err(); err != nil {
		// The enrollment stays pending without token: a member claiming it
		// fails it, or it expires.
		log.Error("cannot keep the join token", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot keep the join token")
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	return c.JSON(http.StatusCreated, clusterEnrollmentResponse(e))
}

// enrollmentClaimStore is the access of claimEnrollment to the database and
// to the cache holding the join tokens.
type enrollmentClaimStore interface {
	ExpireClusterEnrollments(ctx context.Context, clusterID string) error
	NextPendingClusterEnrollment(ctx context.Context, clusterID, nodeID string) (int64, error)
	ClaimClusterEnrollment(ctx context.Context, id int64, nodeID, nodename string) (bool, error)
	SetClusterEnrollmentResult(ctx context.Context, id int64, nodeID, state, message string) (bool, error)
	ClusterEnrollment(ctx context.Context, clusterID string, id int64) (*cdb.DBClusterEnrollment, error)
	// takeJoinToken returns and forgets the join token of an enrollment, ""
	// when the cache no longer has it.
	takeJoinToken(ctx context.Context, id int64) (string, error)
}

// claimEnrollment claims for a member the oldest pending enrollment of
// another node of its cluster, with its join token: nil when none. An
// enrollment claimed by another member meanwhile is skipped; one whose token
// the cache lost is failed, and the next one tried.
func claimEnrollment(ctx context.Context, store enrollmentClaimStore, clusterID, nodeID, nodename string) (*cdb.DBClusterEnrollment, string, error) {
	if err := store.ExpireClusterEnrollments(ctx, clusterID); err != nil {
		return nil, "", err
	}
	for range 10 {
		id, err := store.NextPendingClusterEnrollment(ctx, clusterID, nodeID)
		if err != nil || id == 0 {
			return nil, "", err
		}
		claimed, err := store.ClaimClusterEnrollment(ctx, id, nodeID, nodename)
		if err != nil {
			return nil, "", err
		} else if !claimed {
			continue
		}
		token, err := store.takeJoinToken(ctx, id)
		if err != nil {
			return nil, "", err
		}
		if token == "" {
			if _, err := store.SetClusterEnrollmentResult(ctx, id, nodeID, "failed", "the join token expired"); err != nil {
				return nil, "", err
			}
			continue
		}
		e, err := store.ClusterEnrollment(ctx, clusterID, id)
		if err != nil {
			return nil, "", err
		} else if e == nil {
			continue
		}
		return e, token, nil
	}
	return nil, "", nil
}

// apiClaimStore is the enrollmentClaimStore of the api.
type apiClaimStore struct {
	*cdb.DB
	a *Api
}

func (s apiClaimStore) takeJoinToken(ctx context.Context, id int64) (string, error) {
	token, err := s.a.Redis.GetDel(ctx, cachekeys.EnrollmentTokenPrefix+strconv.FormatInt(id, 10)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return token, err
}

// PostClusterEnrollmentClaim handles POST /clusters/{cluster_id}/enrollments/claim.
func (a *Api) PostClusterEnrollmentClaim(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "PostClusterEnrollmentClaim")
	nodeID, nodename, nodeClusterID := callerNode(c)
	if nodeID == "" {
		return JSONProblemf(c, http.StatusUnauthorized, "node authentication required")
	}
	if nodeClusterID != clusterId {
		return JSONProblemf(c, http.StatusForbidden, "the node %s is not a member of cluster %s", nodename, clusterId)
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	e, token, err := claimEnrollment(ctx, apiClaimStore{DB: a.ODB, a: a}, clusterId, nodeID, nodename)
	if err != nil {
		log.Error("cannot claim an enrollment", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot claim an enrollment")
	} else if e == nil {
		return c.NoContent(http.StatusNoContent)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	return c.JSON(http.StatusOK, server.ClusterEnrollmentClaim{Enrollment: clusterEnrollmentResponse(e), JoinToken: token})
}

// PostClusterEnrollmentResult handles POST /clusters/{cluster_id}/enrollments/{enrollment_id}/result.
func (a *Api) PostClusterEnrollmentResult(c echo.Context, clusterId server.InPathClusterId, enrollmentId server.InPathEnrollmentId) error {
	log := echolog.GetLogHandler(c, "PostClusterEnrollmentResult")
	nodeID, nodename, _ := callerNode(c)
	if nodeID == "" {
		return JSONProblemf(c, http.StatusUnauthorized, "node authentication required")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var body server.ClusterEnrollmentResult
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.State != server.ClusterEnrollmentResultStateDone && body.State != server.ClusterEnrollmentResultStateFailed {
		return JSONProblemf(c, http.StatusBadRequest, "state must be done or failed")
	}
	message := ""
	if body.Message != nil {
		message = *body.Message
		if len(message) > 4096 {
			message = message[:4096]
		}
	}
	e, err := a.ODB.ClusterEnrollment(ctx, clusterId, enrollmentId)
	if err != nil {
		log.Error("cannot read the enrollment", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the enrollment")
	} else if e == nil {
		return JSONProblemf(c, http.StatusNotFound, "enrollment %d not found", enrollmentId)
	}
	if e.ClaimedByNodeID == nil || *e.ClaimedByNodeID != nodeID {
		return JSONProblemf(c, http.StatusForbidden, "the enrollment %d is not claimed by node %s", enrollmentId, nodename)
	}
	if ok, err := a.ODB.SetClusterEnrollmentResult(ctx, enrollmentId, nodeID, string(body.State), message); err != nil {
		log.Error("cannot record the result", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot record the result")
	} else if !ok {
		return JSONProblemf(c, http.StatusConflict, "the enrollment %d is %s", enrollmentId, e.State)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	level := "info"
	if body.State == server.ClusterEnrollmentResultStateFailed {
		level = "error"
	}
	a.nodeRegisterLog(c, ctx, log, level, "node %(node)s enrollment by %(member)s: %(state)s %(message)s", e.NodeID,
		map[string]any{"node": e.Nodename, "member": nodename, "state": string(body.State), "message": message})
	if e, err = a.ODB.ClusterEnrollment(ctx, clusterId, enrollmentId); err != nil || e == nil {
		log.Error("cannot read the enrollment", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the enrollment")
	}
	return c.JSON(http.StatusOK, clusterEnrollmentResponse(e))
}

// GetClusterDeclarations handles GET /cluster_declarations.
func (a *Api) GetClusterDeclarations(c echo.Context) error {
	log := echolog.GetLogHandler(c, "GetClusterDeclarations")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	l, err := a.ODB.ClusterDeclarations(ctx)
	if err != nil {
		log.Error("cannot list the declarations", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list the declarations")
	}
	// The declarations of the clusters the user may provision only.
	scope, err := newProvisioningScope(c, ctx, a.ODB)
	if err != nil {
		log.Error("cannot list the apps of the user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check the responsibility")
	}
	data := make([]server.ClusterDeclarationSummary, 0, len(l))
	for _, d := range l {
		if why, err := scope.allows(ctx, d.ClusterID, d.App); err != nil {
			log.Error("cannot list the apps of the cluster nodes", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check the responsibility")
		} else if why != "" {
			continue
		}
		r := clusterDeclarationResponse(&d.DBClusterDeclaration)
		data = append(data, server.ClusterDeclarationSummary{
			ClusterId: r.ClusterId, ClusterName: r.ClusterName, App: r.App, Heartbeats: r.Heartbeats,
			SeedNodeId: r.SeedNodeId, SeedNodename: r.SeedNodename,
			CreatedBy: r.CreatedBy, Created: r.Created, Updated: r.Updated,
			NodeCount: d.NodeCount, ReportingCount: d.ReportingCount, PendingEnrollments: d.PendingEnrollments,
		})
	}
	return c.JSON(http.StatusOK, server.ClusterDeclarationList{Data: data})
}

// GetClusterDeclarationNodes handles GET /clusters/{cluster_id}/declaration/nodes.
func (a *Api) GetClusterDeclarationNodes(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "GetClusterDeclarationNodes")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	d, err := a.ODB.ClusterDeclaration(ctx, clusterId)
	if err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	if err := requireProvisioningVisible(c, ctx, log, a.ODB, clusterId, d.App); err != nil {
		return err
	}
	l, err := a.ODB.ClusterDeclarationNodes(ctx, clusterId)
	if err != nil {
		log.Error("cannot list the nodes", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list the nodes")
	}
	data := make([]server.ClusterDeclarationNode, len(l))
	for i, n := range l {
		data[i] = server.ClusterDeclarationNode{
			NodeId: n.NodeID, Nodename: n.Nodename, App: n.App, ClusterId: n.ClusterID,
			Seed:     d.SeedNodeID != nil && *d.SeedNodeID == n.NodeID,
			LastComm: n.LastComm, TokenCreated: n.TokenCreated, TokenExpires: n.TokenExpires, TokenUsed: n.TokenUsed,
		}
	}
	return c.JSON(http.StatusOK, server.ClusterDeclarationNodeList{Data: data})
}

// nodenameRegexp is a nodename the provisioning accepts: a host name.
var nodenameRegexp = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$`)

// PostClusterDeclarationNode handles POST /clusters/{cluster_id}/declaration/nodes.
func (a *Api) PostClusterDeclarationNode(c echo.Context, clusterId server.InPathClusterId) error {
	log := echolog.GetLogHandler(c, "PostClusterDeclarationNode")
	if err := requireProvisioning(c); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	var body server.ClusterDeclarationNodeInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if !nodenameRegexp.MatchString(body.Nodename) {
		return JSONProblemf(c, http.StatusBadRequest, "invalid nodename %q", body.Nodename)
	}
	d, err := a.ODB.ClusterDeclaration(ctx, clusterId)
	if err != nil {
		log.Error("cannot read the declaration", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the declaration")
	} else if d == nil {
		return JSONProblemf(c, http.StatusNotFound, "the cluster %s is not declared", clusterId)
	}
	existing, err := a.ODB.NodeClustersByNodename(ctx, body.Nodename)
	if err != nil {
		log.Error("cannot look up the node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot look up the node")
	}
	for nodeID, nodeClusterID := range existing {
		if nodeClusterID != clusterId {
			continue
		}
		node, err := a.ODB.NodeByNodeID(ctx, nodeID)
		if err != nil || node == nil {
			log.Error("cannot read the node", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot read the node")
		}
		return c.JSON(http.StatusCreated, server.ClusterDeclarationNode{
			NodeId: node.NodeID, Nodename: node.Nodename, App: node.App, ClusterId: clusterId,
			Seed: d.SeedNodeID != nil && *d.SeedNodeID == node.NodeID,
		})
	}
	if len(existing) > 0 {
		return JSONProblemf(c, http.StatusConflict,
			"a node %s exists in another cluster: a Manager moves it to this cluster (POST /nodes/{node_id} cluster_id), or delete it", body.Nodename)
	}
	app := d.App
	if body.App != nil && *body.App != "" {
		app = *body.App
	}
	if app == "" {
		return JSONProblemf(c, http.StatusBadRequest, "an app is required, here or in the declaration of the cluster")
	}
	if ok, err := a.ODB.AppExists(ctx, app); err != nil {
		log.Error("cannot verify app", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot verify app")
	} else if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "unknown app %s", app)
	}
	if err := requireProvisioningApps(c, ctx, log, a.ODB, clusterId, d.App, app); err != nil {
		return err
	}
	teamResponsible := ""
	if user := UserInfoFromContext(c); user != nil {
		if userID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64); err == nil {
			if group, ok, err := a.ODB.UserDefaultGroup(ctx, userID); err == nil && ok {
				teamResponsible = group
			}
		}
	}
	nodeID := uuid.New().String()
	if err := a.ODB.InsertNode(ctx, body.Nodename, teamResponsible, app, nodeID, clusterId); err != nil {
		log.Error("cannot create the node", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create the node")
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Debug("cannot notify changes", logkey.Error, err)
	}
	a.auditLog(c, ctx, log, cdb.LogEntry{
		Action: "node.add", Fmt: "node %(node)s declared in cluster %(cluster)s",
		Dict:   map[string]any{"node": body.Nodename, "cluster": d.ClusterName},
		NodeID: uuidRef(nodeID),
	})
	return c.JSON(http.StatusCreated, server.ClusterDeclarationNode{
		NodeId: nodeID, Nodename: body.Nodename, App: app, ClusterId: clusterId,
	})
}
