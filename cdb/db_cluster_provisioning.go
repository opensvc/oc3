package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type (
	// DBClusterDeclaration is a cluster the collector provisions.
	DBClusterDeclaration struct {
		ClusterID   string
		ClusterName string
		App         string
		// Heartbeats is the JSON array of the hb sections.
		Heartbeats   string
		SeedNodeID   *string
		SeedNodename *string
		CreatedBy    string
		Created      time.Time
		Updated      time.Time
	}

	// DBBootstrapNode is the node a bootstrap token was issued for.
	DBBootstrapNode struct {
		NodeID    string
		Nodename  string
		App       string
		ClusterID string
	}

	// DBClusterEnrollment is the request of a node to join its cluster.
	DBClusterEnrollment struct {
		ID              int64
		ClusterID       string
		NodeID          string
		Nodename        string
		NodeAddr        string
		State           string
		ClaimedByNodeID *string
		ClaimedBy       *string
		Message         string
		Created         time.Time
		Updated         time.Time
		Expires         time.Time
	}
)

// ErrClusterDeclared is a cluster declared already.
var ErrClusterDeclared = errors.New("the cluster is declared already")

const clusterDeclarationSelect = "SELECT d.cluster_id, d.cluster_name, d.app, d.heartbeats, d.seed_node_id, n.nodename," +
	" d.created_by, d.created, d.updated" +
	" FROM cluster_declarations d LEFT JOIN nodes n ON n.node_id = d.seed_node_id"

// InsertClusterDeclaration declares a cluster, and records it in clusters when
// the collector does not know it yet, so that it is listed before its first
// report. ErrClusterDeclared when a declaration exists.
func (oDb *DB) InsertClusterDeclaration(ctx context.Context, d DBClusterDeclaration) error {
	defer logDuration("InsertClusterDeclaration", time.Now())
	const query = "INSERT INTO cluster_declarations" +
		" (cluster_id, cluster_name, app, heartbeats, seed_node_id, created_by, created, updated)" +
		" VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())"
	if _, err := oDb.ExecContext(ctx, query, d.ClusterID, d.ClusterName, d.App, d.Heartbeats, d.SeedNodeID, d.CreatedBy); err != nil {
		if isDuplicate(err) {
			return ErrClusterDeclared
		}
		return fmt.Errorf("InsertClusterDeclaration: %w", err)
	}
	const queryCluster = "INSERT IGNORE INTO clusters (cluster_name, cluster_id) VALUES (?, ?)"
	if _, err := oDb.ExecContext(ctx, queryCluster, d.ClusterName, d.ClusterID); err != nil {
		return fmt.Errorf("InsertClusterDeclaration: clusters: %w", err)
	}
	oDb.SetChange("cluster_declarations", "clusters")
	return nil
}

// ClusterDeclaration returns the declaration of a cluster, nil when none.
func (oDb *DB) ClusterDeclaration(ctx context.Context, clusterID string) (*DBClusterDeclaration, error) {
	defer logDuration("ClusterDeclaration", time.Now())
	var d DBClusterDeclaration
	var seedNodeID, seedNodename sql.NullString
	err := oDb.DB.QueryRowContext(ctx, clusterDeclarationSelect+" WHERE d.cluster_id = ?", clusterID).Scan(
		&d.ClusterID, &d.ClusterName, &d.App, &d.Heartbeats, &seedNodeID, &seedNodename,
		&d.CreatedBy, &d.Created, &d.Updated)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("ClusterDeclaration: %w", err)
	}
	if seedNodeID.Valid {
		d.SeedNodeID = &seedNodeID.String
	}
	if seedNodename.Valid {
		d.SeedNodename = &seedNodename.String
	}
	return &d, nil
}

// UpdateClusterDeclaration changes the app and the heartbeats of a declared
// cluster: false when it is not declared.
func (oDb *DB) UpdateClusterDeclaration(ctx context.Context, clusterID, app, heartbeats string) (bool, error) {
	defer logDuration("UpdateClusterDeclaration", time.Now())
	const query = "UPDATE cluster_declarations SET app = ?, heartbeats = ?, updated = NOW() WHERE cluster_id = ?"
	result, err := oDb.ExecContext(ctx, query, app, heartbeats, clusterID)
	if err != nil {
		return false, fmt.Errorf("UpdateClusterDeclaration: %w", err)
	}
	// The row is matched even when nothing changes: CLIENT_FOUND_ROWS is not
	// set, so look it up rather than trust the count.
	if n, _ := result.RowsAffected(); n > 0 {
		oDb.SetChange("cluster_declarations")
		return true, nil
	}
	d, err := oDb.ClusterDeclaration(ctx, clusterID)
	return d != nil, err
}

// DeleteClusterDeclaration deletes the declaration of a cluster, the bootstrap
// tokens issued for it and its enrollments: false when it is not
// declared. The cluster row goes too while no node reported for it.
func (oDb *DB) DeleteClusterDeclaration(ctx context.Context, clusterID string) (bool, error) {
	defer logDuration("DeleteClusterDeclaration", time.Now())
	result, err := oDb.ExecContext(ctx, "DELETE FROM cluster_declarations WHERE cluster_id = ?", clusterID)
	if err != nil {
		return false, fmt.Errorf("DeleteClusterDeclaration: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return false, nil
	}
	for _, query := range []string{
		"DELETE FROM node_bootstrap_tokens WHERE cluster_id = ?",
		"DELETE FROM cluster_enrollments WHERE cluster_id = ?",
		"DELETE FROM clusters WHERE cluster_id = ? AND cluster_data IS NULL",
	} {
		if _, err := oDb.ExecContext(ctx, query, clusterID); err != nil {
			return true, fmt.Errorf("DeleteClusterDeclaration: %w", err)
		}
	}
	oDb.SetChange("cluster_declarations", "cluster_enrollments", "clusters")
	return true, nil
}

// ClusterReporting returns whether a cluster has reported to the collector,
// and the name it reported.
func (oDb *DB) ClusterReporting(ctx context.Context, clusterID string) (name string, reporting bool, err error) {
	err = oDb.DB.QueryRowContext(ctx,
		"SELECT cluster_name FROM clusters WHERE cluster_id = ? AND cluster_data IS NOT NULL LIMIT 1", clusterID).Scan(&name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("ClusterReporting: %w", err)
	}
	return name, true, nil
}

// ClusterReportingNodeID returns a node of the cluster that reported to the
// collector, the most recently, "" when none.
func (oDb *DB) ClusterReportingNodeID(ctx context.Context, clusterID string) (string, error) {
	var nodeID string
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT node_id FROM nodes WHERE cluster_id = ? AND last_comm IS NOT NULL ORDER BY last_comm DESC LIMIT 1",
		clusterID).Scan(&nodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("ClusterReportingNodeID: %w", err)
	}
	return nodeID, nil
}

// ElectClusterSeed makes nodeID the seed of a declared cluster without one,
// and returns the seed of the cluster: nodeID when elected, the node elected
// before otherwise. The update is the election: of concurrent bootstraps, one
// sets it.
func (oDb *DB) ElectClusterSeed(ctx context.Context, clusterID, nodeID string) (string, error) {
	defer logDuration("ElectClusterSeed", time.Now())
	const query = "UPDATE cluster_declarations SET seed_node_id = ?, updated = NOW()" +
		" WHERE cluster_id = ? AND seed_node_id IS NULL"
	if _, err := oDb.ExecContext(ctx, query, nodeID, clusterID); err != nil {
		return "", fmt.Errorf("ElectClusterSeed: %w", err)
	}
	var seed sql.NullString
	if err := oDb.DB.QueryRowContext(ctx,
		"SELECT seed_node_id FROM cluster_declarations WHERE cluster_id = ?", clusterID).Scan(&seed); err != nil {
		return "", fmt.Errorf("ElectClusterSeed: %w", err)
	}
	oDb.SetChange("cluster_declarations")
	return seed.String, nil
}

// InsertNodeBootstrapToken records the token hash of a node, valid ttl, after
// deleting the unused tokens of the node. It returns when it expires.
func (oDb *DB) InsertNodeBootstrapToken(ctx context.Context, nodeID, clusterID, tokenHash string, ttl time.Duration, createdBy string) (time.Time, error) {
	defer logDuration("InsertNodeBootstrapToken", time.Now())
	if _, err := oDb.ExecContext(ctx,
		"DELETE FROM node_bootstrap_tokens WHERE node_id = ? AND used IS NULL", nodeID); err != nil {
		return time.Time{}, fmt.Errorf("InsertNodeBootstrapToken: %w", err)
	}
	const query = "INSERT INTO node_bootstrap_tokens (node_id, cluster_id, token_hash, expires, created_by, created)" +
		" VALUES (?, ?, ?, DATE_ADD(NOW(), INTERVAL ? SECOND), ?, NOW())"
	result, err := oDb.ExecContext(ctx, query, nodeID, clusterID, tokenHash, int64(ttl/time.Second), createdBy)
	if err != nil {
		return time.Time{}, fmt.Errorf("InsertNodeBootstrapToken: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return time.Time{}, fmt.Errorf("InsertNodeBootstrapToken: %w", err)
	}
	var expires time.Time
	if err := oDb.DB.QueryRowContext(ctx, "SELECT expires FROM node_bootstrap_tokens WHERE id = ?", id).Scan(&expires); err != nil {
		return time.Time{}, fmt.Errorf("InsertNodeBootstrapToken: %w", err)
	}
	oDb.SetChange("node_bootstrap_tokens")
	return expires, nil
}

// BootstrapTokenNode returns the node an unused, unexpired token was issued
// for, nil when none.
func (oDb *DB) BootstrapTokenNode(ctx context.Context, tokenHash string) (*DBBootstrapNode, error) {
	defer logDuration("BootstrapTokenNode", time.Now())
	const query = "SELECT n.node_id, COALESCE(n.nodename, ''), COALESCE(n.app, ''), t.cluster_id" +
		" FROM node_bootstrap_tokens t JOIN nodes n ON n.node_id = t.node_id" +
		" WHERE t.token_hash = ? AND t.used IS NULL AND t.expires > NOW()"
	var n DBBootstrapNode
	err := oDb.DB.QueryRowContext(ctx, query, tokenHash).Scan(&n.NodeID, &n.Nodename, &n.App, &n.ClusterID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("BootstrapTokenNode: %w", err)
	}
	return &n, nil
}

// NodeBootstrappedInto returns whether a node was bootstrapped into a cluster:
// it used a token issued for it.
func (oDb *DB) NodeBootstrappedInto(ctx context.Context, nodeID, clusterID string) (bool, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM node_bootstrap_tokens WHERE node_id = ? AND cluster_id = ? AND used IS NOT NULL",
		nodeID, clusterID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("NodeBootstrappedInto: %w", err)
	}
	return n > 0, nil
}

// UseBootstrapToken marks a token used: false when it was used meanwhile, or
// expired. The update is the use: of concurrent bootstraps, one uses it.
func (oDb *DB) UseBootstrapToken(ctx context.Context, tokenHash string) (bool, error) {
	defer logDuration("UseBootstrapToken", time.Now())
	const query = "UPDATE node_bootstrap_tokens SET used = NOW()" +
		" WHERE token_hash = ? AND used IS NULL AND expires > NOW()"
	result, err := oDb.ExecContext(ctx, query, tokenHash)
	if err != nil {
		return false, fmt.Errorf("UseBootstrapToken: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("UseBootstrapToken: %w", err)
	}
	oDb.SetChange("node_bootstrap_tokens")
	return n == 1, nil
}

const clusterEnrollmentSelect = "SELECT id, cluster_id, node_id, nodename, node_addr, state, claimed_by_node_id, claimed_by," +
	" message, created, updated, expires FROM cluster_enrollments"

func scanClusterEnrollment(scan func(...any) error) (*DBClusterEnrollment, error) {
	var e DBClusterEnrollment
	var claimedByNodeID, claimedBy sql.NullString
	if err := scan(&e.ID, &e.ClusterID, &e.NodeID, &e.Nodename, &e.NodeAddr, &e.State, &claimedByNodeID, &claimedBy,
		&e.Message, &e.Created, &e.Updated, &e.Expires); err != nil {
		return nil, err
	}
	if claimedByNodeID.Valid {
		e.ClaimedByNodeID = &claimedByNodeID.String
	}
	if claimedBy.Valid {
		e.ClaimedBy = &claimedBy.String
	}
	return &e, nil
}

// ExpireClusterEnrollments marks expired the enrollments of a cluster pending
// past their expiry, and failed those claimed without a result 15 minutes past
// it: the member that claimed them stopped.
func (oDb *DB) ExpireClusterEnrollments(ctx context.Context, clusterID string) error {
	for _, query := range []string{
		"UPDATE cluster_enrollments SET state = 'expired', updated = NOW()" +
			" WHERE cluster_id = ? AND state = 'pending' AND expires <= NOW()",
		"UPDATE cluster_enrollments SET state = 'failed', message = 'no result from the member that claimed it', updated = NOW()" +
			" WHERE cluster_id = ? AND state = 'claimed' AND expires <= DATE_SUB(NOW(), INTERVAL 15 MINUTE)",
	} {
		if _, err := oDb.ExecContext(ctx, query, clusterID); err != nil {
			return fmt.Errorf("ExpireClusterEnrollments: %w", err)
		}
	}
	return nil
}

// InsertClusterEnrollment records the pending enrollment of a node, valid ttl,
// the pending one it had before expired.
func (oDb *DB) InsertClusterEnrollment(ctx context.Context, clusterID, nodeID, nodename, nodeAddr string, ttl time.Duration) (*DBClusterEnrollment, error) {
	defer logDuration("InsertClusterEnrollment", time.Now())
	if _, err := oDb.ExecContext(ctx,
		"UPDATE cluster_enrollments SET state = 'expired', message = 'replaced', updated = NOW() WHERE node_id = ? AND state = 'pending'",
		nodeID); err != nil {
		return nil, fmt.Errorf("InsertClusterEnrollment: %w", err)
	}
	const query = "INSERT INTO cluster_enrollments" +
		" (cluster_id, node_id, nodename, node_addr, state, message, created, updated, expires)" +
		" VALUES (?, ?, ?, ?, 'pending', '', NOW(), NOW(), DATE_ADD(NOW(), INTERVAL ? SECOND))"
	result, err := oDb.ExecContext(ctx, query, clusterID, nodeID, nodename, nodeAddr, int64(ttl/time.Second))
	if err != nil {
		return nil, fmt.Errorf("InsertClusterEnrollment: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("InsertClusterEnrollment: %w", err)
	}
	oDb.SetChange("cluster_enrollments")
	return oDb.ClusterEnrollment(ctx, clusterID, id)
}

// ClusterEnrollment returns an enrollment of a cluster, nil when none.
func (oDb *DB) ClusterEnrollment(ctx context.Context, clusterID string, id int64) (*DBClusterEnrollment, error) {
	e, err := scanClusterEnrollment(oDb.DB.QueryRowContext(ctx,
		clusterEnrollmentSelect+" WHERE id = ? AND cluster_id = ?", id, clusterID).Scan)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("ClusterEnrollment: %w", err)
	}
	return e, nil
}

// ClusterEnrollments returns the last enrollments of a cluster, newest first.
func (oDb *DB) ClusterEnrollments(ctx context.Context, clusterID string, limit int) ([]*DBClusterEnrollment, error) {
	defer logDuration("ClusterEnrollments", time.Now())
	rows, err := oDb.DB.QueryContext(ctx,
		clusterEnrollmentSelect+" WHERE cluster_id = ? ORDER BY id DESC LIMIT ?", clusterID, limit)
	if err != nil {
		return nil, fmt.Errorf("ClusterEnrollments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	l := make([]*DBClusterEnrollment, 0)
	for rows.Next() {
		e, err := scanClusterEnrollment(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("ClusterEnrollments: %w", err)
		}
		l = append(l, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ClusterEnrollments: %w", err)
	}
	return l, nil
}

// NextPendingClusterEnrollment returns the id of the oldest pending, unexpired
// enrollment of a cluster of another node than nodeID, 0 when none.
func (oDb *DB) NextPendingClusterEnrollment(ctx context.Context, clusterID, nodeID string) (int64, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM cluster_enrollments WHERE cluster_id = ? AND state = 'pending' AND expires > NOW() AND node_id <> ?"+
			" ORDER BY id LIMIT 1", clusterID, nodeID).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("NextPendingClusterEnrollment: %w", err)
	}
	return id, nil
}

// ClaimClusterEnrollment marks a pending enrollment claimed by a node: false
// when another one claimed it meanwhile, or it expired. The update is the
// claim: of concurrent members, one gets it.
func (oDb *DB) ClaimClusterEnrollment(ctx context.Context, id int64, nodeID, nodename string) (bool, error) {
	defer logDuration("ClaimClusterEnrollment", time.Now())
	const query = "UPDATE cluster_enrollments SET state = 'claimed', claimed_by_node_id = ?, claimed_by = ?, updated = NOW()" +
		" WHERE id = ? AND state = 'pending' AND expires > NOW()"
	result, err := oDb.ExecContext(ctx, query, nodeID, nodename, id)
	if err != nil {
		return false, fmt.Errorf("ClaimClusterEnrollment: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("ClaimClusterEnrollment: %w", err)
	}
	oDb.SetChange("cluster_enrollments")
	return n == 1, nil
}

// SetClusterEnrollmentResult records how a claimed enrollment ended, by the
// node that claimed it: false when it is not claimed by it, or no longer.
func (oDb *DB) SetClusterEnrollmentResult(ctx context.Context, id int64, nodeID, state, message string) (bool, error) {
	defer logDuration("SetClusterEnrollmentResult", time.Now())
	const query = "UPDATE cluster_enrollments SET state = ?, message = ?, updated = NOW()" +
		" WHERE id = ? AND state = 'claimed' AND claimed_by_node_id = ?"
	result, err := oDb.ExecContext(ctx, query, state, message, id, nodeID)
	if err != nil {
		return false, fmt.Errorf("SetClusterEnrollmentResult: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("SetClusterEnrollmentResult: %w", err)
	}
	oDb.SetChange("cluster_enrollments")
	return n == 1, nil
}

type (
	// DBClusterDeclarationSummary is a declared cluster with the counts of
	// its nodes and pending enrollments.
	DBClusterDeclarationSummary struct {
		DBClusterDeclaration
		NodeCount          int
		ReportingCount     int
		PendingEnrollments int
	}

	// DBClusterDeclarationNode is a node of a declared cluster, with its last
	// bootstrap token.
	DBClusterDeclarationNode struct {
		NodeID       string
		Nodename     string
		App          string
		ClusterID    string
		LastComm     *time.Time
		TokenCreated *time.Time
		TokenExpires *time.Time
		TokenUsed    *time.Time
	}
)

// ClusterDeclarations returns the declared clusters, by name, with their
// counts.
func (oDb *DB) ClusterDeclarations(ctx context.Context) ([]*DBClusterDeclarationSummary, error) {
	defer logDuration("ClusterDeclarations", time.Now())
	const query = "SELECT d.cluster_id, d.cluster_name, d.app, d.heartbeats, d.seed_node_id, sn.nodename," +
		" d.created_by, d.created, d.updated," +
		" (SELECT COUNT(*) FROM nodes n WHERE n.cluster_id = d.cluster_id)," +
		" (SELECT COUNT(*) FROM nodes n WHERE n.cluster_id = d.cluster_id AND n.last_comm IS NOT NULL)," +
		" (SELECT COUNT(*) FROM cluster_enrollments e WHERE e.cluster_id = d.cluster_id AND e.state = 'pending' AND e.expires > NOW())" +
		" FROM cluster_declarations d LEFT JOIN nodes sn ON sn.node_id = d.seed_node_id" +
		" ORDER BY d.cluster_name, d.cluster_id"
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ClusterDeclarations: %w", err)
	}
	defer func() { _ = rows.Close() }()
	l := make([]*DBClusterDeclarationSummary, 0)
	for rows.Next() {
		var d DBClusterDeclarationSummary
		var seedNodeID, seedNodename sql.NullString
		if err := rows.Scan(&d.ClusterID, &d.ClusterName, &d.App, &d.Heartbeats, &seedNodeID, &seedNodename,
			&d.CreatedBy, &d.Created, &d.Updated, &d.NodeCount, &d.ReportingCount, &d.PendingEnrollments); err != nil {
			return nil, fmt.Errorf("ClusterDeclarations: %w", err)
		}
		if seedNodeID.Valid {
			d.SeedNodeID = &seedNodeID.String
		}
		if seedNodename.Valid {
			d.SeedNodename = &seedNodename.String
		}
		l = append(l, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ClusterDeclarations: %w", err)
	}
	return l, nil
}

// ClusterDeclarationNodes returns the nodes of a declared cluster: those with
// its cluster_id and those bootstrapped into it, by nodename, with their last
// bootstrap token for the cluster.
func (oDb *DB) ClusterDeclarationNodes(ctx context.Context, clusterID string) ([]*DBClusterDeclarationNode, error) {
	defer logDuration("ClusterDeclarationNodes", time.Now())
	const query = "SELECT n.node_id, COALESCE(n.nodename, ''), COALESCE(n.app, ''), COALESCE(n.cluster_id, ''), n.last_comm," +
		" t.created, t.expires, t.used" +
		" FROM nodes n LEFT JOIN node_bootstrap_tokens t ON t.id = (" +
		"   SELECT MAX(t2.id) FROM node_bootstrap_tokens t2 WHERE t2.node_id = n.node_id AND t2.cluster_id = ?)" +
		" WHERE n.cluster_id = ? OR n.node_id IN (SELECT t3.node_id FROM node_bootstrap_tokens t3 WHERE t3.cluster_id = ?)" +
		" ORDER BY n.nodename"
	rows, err := oDb.DB.QueryContext(ctx, query, clusterID, clusterID, clusterID)
	if err != nil {
		return nil, fmt.Errorf("ClusterDeclarationNodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	l := make([]*DBClusterDeclarationNode, 0)
	for rows.Next() {
		var n DBClusterDeclarationNode
		var lastComm, created, expires, used sql.NullTime
		if err := rows.Scan(&n.NodeID, &n.Nodename, &n.App, &n.ClusterID, &lastComm, &created, &expires, &used); err != nil {
			return nil, fmt.Errorf("ClusterDeclarationNodes: %w", err)
		}
		for _, p := range []struct {
			v   sql.NullTime
			dst **time.Time
		}{{lastComm, &n.LastComm}, {created, &n.TokenCreated}, {expires, &n.TokenExpires}, {used, &n.TokenUsed}} {
			if p.v.Valid {
				t := p.v.Time
				*p.dst = &t
			}
		}
		l = append(l, &n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ClusterDeclarationNodes: %w", err)
	}
	return l, nil
}

// NodeClustersByNodename returns the cluster_id of the nodes named nodename,
// by node_id.
func (oDb *DB) NodeClustersByNodename(ctx context.Context, nodename string) (map[string]string, error) {
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT node_id, COALESCE(cluster_id, '') FROM nodes WHERE nodename = ?", nodename)
	if err != nil {
		return nil, fmt.Errorf("NodeClustersByNodename: %w", err)
	}
	defer func() { _ = rows.Close() }()
	m := make(map[string]string)
	for rows.Next() {
		var nodeID, clusterID string
		if err := rows.Scan(&nodeID, &clusterID); err != nil {
			return nil, fmt.Errorf("NodeClustersByNodename: %w", err)
		}
		m[nodeID] = clusterID
	}
	return m, rows.Err()
}

// ClusterNodeApps returns the apps of the nodes the collector has in the
// cluster, the ones a user must be responsible for to provision it.
func (oDb *DB) ClusterNodeApps(ctx context.Context, clusterID string) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT DISTINCT app FROM nodes WHERE cluster_id = ?", clusterID)
	if err != nil {
		return nil, fmt.Errorf("ClusterNodeApps: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var l []string
	for rows.Next() {
		var app sql.NullString
		if err := rows.Scan(&app); err != nil {
			return nil, fmt.Errorf("ClusterNodeApps: %w", err)
		}
		l = append(l, app.String)
	}
	return l, rows.Err()
}
