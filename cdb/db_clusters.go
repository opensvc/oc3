package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// clustersTable is the clusters as listed: one row of the clusters table each, with
// what the daemon status they hold (cluster_data) says of them, and the nodes and
// services of the collector that name them. A derived table under the name of the
// table, so that these computed columns are sorted and filtered as the others.
const clustersTable = `(SELECT c.id, c.cluster_id, c.cluster_name,
	(SELECT COUNT(*) FROM nodes n WHERE n.cluster_id = c.cluster_id) AS node_count,
	(SELECT COUNT(*) FROM services s WHERE s.cluster_id = c.cluster_id) AS svc_count,
	(SELECT GROUP_CONCAT(DISTINCT n.version ORDER BY n.version SEPARATOR ', ')
		FROM nodes n WHERE n.cluster_id = c.cluster_id AND n.version <> '') AS agent_versions,
	REPLACE(REPLACE(REPLACE(JSON_EXTRACT(c.cluster_data, '$.data.cluster.config.nodes'),
		'[', ''), ']', ''), '"', '') AS cluster_nodes,
	JSON_VALUE(c.cluster_data, '$.data.cluster.config.quorum') AS quorum,
	JSON_VALUE(c.cluster_data, '$.data.cluster.status.is_frozen') AS frozen,
	JSON_VALUE(c.cluster_data, '$.data.cluster.status.is_compat') AS compat,
	JSON_VALUE(c.cluster_data, '$.data.cluster.config.listener.port') AS listener_port,
	JSON_VALUE(c.cluster_data, '$.updated_at') AS cluster_updated
	FROM clusters c) AS clusters`

// clustersVisibility restricts the clusters to those holding a node or a service
// the caller may see, by the apps their groups are responsible for, as the nodes
// and services lists do. A manager sees them all.
func clustersVisibility(groups []string, isManager bool) (string, []any) {
	if isManager {
		return "", nil
	}
	cleanGroups := cleanGroups(groups)
	if len(cleanGroups) == 0 {
		return " AND 1=0", nil
	}
	apps := "SELECT a.app FROM apps a" +
		" JOIN apps_responsibles ar ON ar.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ar.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(cleanGroups)) + ")"
	args := make([]any, 0, 2*len(cleanGroups))
	for range 2 {
		for _, g := range cleanGroups {
			args = append(args, g)
		}
	}
	return " AND (clusters.cluster_id IN (SELECT nodes.cluster_id FROM nodes WHERE nodes.app IN (" + apps + "))" +
		" OR clusters.cluster_id IN (SELECT services.cluster_id FROM services WHERE services.svc_app IN (" + apps + ")))", args
}

func (oDb *DB) queryClusters(ctx context.Context, clusterID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("queryClusters: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM " + clustersTable + " WHERE clusters.id > 0"
	visibility, args := clustersVisibility(p.Groups, p.IsManager)
	query += visibility
	if clusterID != "" {
		query += " AND clusters.cluster_id = ?"
		args = append(args, clusterID)
	}
	conds, filterArgs := p.FilterConditions()
	for _, cond := range conds {
		query += " AND " + cond
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("clusters.cluster_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("queryClusters: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetClusters returns the clusters the caller may see.
func (oDb *DB) GetClusters(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryClusters(ctx, "", p)
}

// GetCluster returns one cluster, by its cluster_id, if the caller may see it.
func (oDb *DB) GetCluster(ctx context.Context, clusterID string, p ListParams) ([]map[string]any, error) {
	return oDb.queryClusters(ctx, clusterID, p)
}

// ClusterName returns the name of a cluster by its cluster_id, and whether the
// collector knows it.
func (oDb *DB) ClusterName(ctx context.Context, clusterID string) (string, bool, error) {
	var name string
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT cluster_name FROM clusters WHERE cluster_id = ? LIMIT 1", clusterID).Scan(&name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("clusterName: %w", err)
	}
	return name, true, nil
}

// ClusterNodesResponsibility counts the nodes of the collector naming the
// cluster, and names those the caller is not responsible for: their app is not
// one their groups are responsible for. A manager is responsible for them all.
func (oDb *DB) ClusterNodesResponsibility(ctx context.Context, clusterID string, groups []string, isManager bool) (int, []string, error) {
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT COALESCE(nodename, ''), COALESCE(app, '') FROM nodes WHERE cluster_id = ? ORDER BY nodename", clusterID)
	if err != nil {
		return 0, nil, fmt.Errorf("clusterNodesResponsibility: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type member struct{ nodename, app string }
	var members []member
	for rows.Next() {
		var m member
		if err := rows.Scan(&m.nodename, &m.app); err != nil {
			return 0, nil, fmt.Errorf("clusterNodesResponsibility: %w", err)
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, fmt.Errorf("clusterNodesResponsibility: %w", err)
	}
	if isManager {
		return len(members), nil, nil
	}
	apps, err := oDb.AppsForGroups(ctx, groups)
	if err != nil {
		return 0, nil, fmt.Errorf("clusterNodesResponsibility: %w", err)
	}
	var out []string
	for _, m := range members {
		if !slices.ContainsFunc(apps, func(a string) bool { return strings.EqualFold(a, m.app) }) {
			out = append(out, m.nodename)
		}
	}
	return len(members), out, nil
}

// ClusterLiveNode returns a node of the cluster seen alive in the last 15
// minutes, running the om3 agent ("v3." or "3." versions) and reading its own
// queue (pull or feed), to post a cluster action for: nil when there is none.
func (oDb *DB) ClusterLiveNode(ctx context.Context, clusterID string) (*NodeActionTarget, error) {
	const query = "SELECT node_id, COALESCE(nodename, ''), COALESCE(os_name, ''), COALESCE(action_type, '')," +
		" COALESCE(collector, ''), COALESCE(connect_to, '')" +
		" FROM nodes WHERE cluster_id = ? AND last_comm > NOW() - INTERVAL 15 MINUTE" +
		" AND (version LIKE 'v3.%' OR version LIKE '3.%')" +
		" AND COALESCE(action_type, '') IN ('', 'pull', 'feed')" +
		" ORDER BY nodename LIMIT 1"
	var t NodeActionTarget
	err := oDb.DB.QueryRowContext(ctx, query, clusterID).
		Scan(&t.NodeID, &t.Nodename, &t.OSName, &t.ActionType, &t.Collector, &t.ConnectTo)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("clusterLiveNode: %w", err)
	}
	return &t, nil
}

// ClusterServicesResponsibility names the services of the collector naming the
// cluster the caller is not responsible for: their app is not one their groups
// are responsible for. A manager is responsible for them all.
func (oDb *DB) ClusterServicesResponsibility(ctx context.Context, clusterID string, groups []string, isManager bool) ([]string, error) {
	if isManager {
		return nil, nil
	}
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT COALESCE(svcname, ''), COALESCE(svc_app, '') FROM services WHERE cluster_id = ? ORDER BY svcname", clusterID)
	if err != nil {
		return nil, fmt.Errorf("clusterServicesResponsibility: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type member struct{ svcname, app string }
	var members []member
	for rows.Next() {
		var m member
		if err := rows.Scan(&m.svcname, &m.app); err != nil {
			return nil, fmt.Errorf("clusterServicesResponsibility: %w", err)
		}
		members = append(members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clusterServicesResponsibility: %w", err)
	}
	if len(members) == 0 {
		return nil, nil
	}
	apps, err := oDb.AppsForGroups(ctx, groups)
	if err != nil {
		return nil, fmt.Errorf("clusterServicesResponsibility: %w", err)
	}
	var out []string
	for _, m := range members {
		if !slices.ContainsFunc(apps, func(a string) bool { return strings.EqualFold(a, m.app) }) {
			out = append(out, m.svcname)
		}
	}
	return out, nil
}

// clusterCascadeTables hold rows of a cluster by its cluster_id, deleted with
// it once its nodes and services are: what a cluster is in the collector
// besides them. orchestration_run_targets stays: the history of the runs.
var clusterCascadeTables = []string{
	"service_ids",
	"hbmon",
	"hbmon_log",
	"hbmon_log_last",
	"cluster_declarations",
	"node_bootstrap_tokens",
	"cluster_enrollments",
	"node_cluster_claims",
	"oidc_provider_grants",
	"clusters",
}

// clusterMemberIDs returns the ids of a column of a table for the rows naming
// the cluster.
func (oDb *DB) clusterMemberIDs(ctx context.Context, query, clusterID string) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, query, clusterID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

// DeleteClusterCascade deletes a cluster from the collector: its nodes and its
// services with what refers to them (DeleteNodesCascade,
// DeleteServicesCascade), its heartbeats, its declaration with its bootstrap
// tokens and enrollments, the om3 grants given on it, then its row. It
// returns the ids of the nodes deleted, whose files are left to the caller
// once the deletion committed (see util/nodefiles), and the count of the
// services deleted. To be run in a transaction.
func (oDb *DB) DeleteClusterCascade(ctx context.Context, clusterID string) (nodeIDs []string, services int, err error) {
	defer logDuration("DeleteClusterCascade", time.Now())
	if clusterID == "" {
		return nil, 0, fmt.Errorf("DeleteClusterCascade: empty cluster_id")
	}
	nodeIDs, err = oDb.clusterMemberIDs(ctx, "SELECT node_id FROM nodes WHERE cluster_id = ?", clusterID)
	if err != nil {
		return nil, 0, fmt.Errorf("DeleteClusterCascade nodes: %w", err)
	}
	svcIDs, err := oDb.clusterMemberIDs(ctx, "SELECT svc_id FROM services WHERE cluster_id = ?", clusterID)
	if err != nil {
		return nil, 0, fmt.Errorf("DeleteClusterCascade services: %w", err)
	}
	if err := oDb.DeleteNodesCascade(ctx, nodeIDs); err != nil {
		return nil, 0, fmt.Errorf("DeleteClusterCascade: %w", err)
	}
	if err := oDb.DeleteServicesCascade(ctx, svcIDs); err != nil {
		return nil, 0, fmt.Errorf("DeleteClusterCascade: %w", err)
	}
	for _, t := range clusterCascadeTables {
		if _, err := oDb.ExecContext(ctx, "DELETE FROM "+t+" WHERE cluster_id = ?", clusterID); err != nil {
			return nil, 0, fmt.Errorf("DeleteClusterCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	return nodeIDs, len(svcIDs), nil
}
