package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type (
	// ClusterClaim is what the collector made of the cluster id a node
	// reports.
	ClusterClaim int

	// ConfirmedClusterClaim is a node a member of its cluster confirmed.
	ConfirmedClusterClaim struct {
		NodeID   string
		Nodename string
	}
)

const (
	// ClusterClaimNone: no cluster id reported, or the node is unknown.
	ClusterClaimNone ClusterClaim = iota
	// ClusterClaimMember: the node is a confirmed member of the cluster already.
	ClusterClaimMember
	// ClusterClaimNew: the collector does not know the cluster, the node is
	// its first member: om3 cluster ids are random, created with the cluster.
	ClusterClaimNew
	// ClusterClaimOrphan: the collector knows the cluster by its services or
	// its data, but none of its nodes: the node takes it over.
	ClusterClaimOrphan
	// ClusterClaimPending: the cluster has members, or is declared for
	// provisioning: the node joins it once a confirmed member lists it.
	ClusterClaimPending
)

func (c ClusterClaim) String() string {
	switch c {
	case ClusterClaimMember:
		return "member"
	case ClusterClaimNew:
		return "new"
	case ClusterClaimOrphan:
		return "orphan"
	case ClusterClaimPending:
		return "pending"
	default:
		return "none"
	}
}

// Confirmed returns whether the node is a member of the cluster it claims.
func (c ClusterClaim) Confirmed() bool {
	switch c {
	case ClusterClaimMember, ClusterClaimNew, ClusterClaimOrphan:
		return true
	default:
		return false
	}
}

// ClaimClusterMembership records the cluster id the node nodeID reports.
//
// A node can not make itself a member of a cluster: the collector would let
// it report for the nodes of the cluster, claim the enrollments of the nodes
// joining it, read the compliance of its services. nodes.cluster_id changes
// to the cluster claimed only when the collector does not know any node of
// it, nor a declaration; otherwise the claim waits in node_cluster_claims
// until a member confirms it (ConfirmClusterClaims).
func (oDb *DB) ClaimClusterMembership(ctx context.Context, nodeID, clusterID string) (ClusterClaim, error) {
	if clusterID == "" || nodeID == "" {
		return ClusterClaimNone, nil
	}
	var current string
	err := oDb.DB.QueryRowContext(ctx, "SELECT cluster_id FROM nodes WHERE node_id = ?", nodeID).Scan(&current)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ClusterClaimNone, nil
	case err != nil:
		return ClusterClaimNone, fmt.Errorf("ClaimClusterMembership node: %w", err)
	}
	if current == clusterID {
		return ClusterClaimMember, oDb.deleteClusterClaim(ctx, nodeID)
	}

	var members, declared, known int
	const queryKnown = "SELECT" +
		" (SELECT COUNT(*) FROM nodes WHERE cluster_id = ? AND node_id != ?)," +
		" (SELECT COUNT(*) FROM cluster_declarations WHERE cluster_id = ?)," +
		" (SELECT COUNT(*) FROM services WHERE cluster_id = ?) + (SELECT COUNT(*) FROM clusters WHERE cluster_id = ?)"
	if err := oDb.DB.QueryRowContext(ctx, queryKnown, clusterID, nodeID, clusterID, clusterID, clusterID).Scan(&members, &declared, &known); err != nil {
		return ClusterClaimNone, fmt.Errorf("ClaimClusterMembership cluster: %w", err)
	}
	if members > 0 || declared > 0 {
		const query = "INSERT INTO node_cluster_claims (node_id, cluster_id, created, updated) VALUES (?, ?, NOW(), NOW())" +
			" ON DUPLICATE KEY UPDATE" +
			" created = IF(cluster_id = VALUES(cluster_id), created, VALUES(created))," +
			" cluster_id = VALUES(cluster_id), updated = VALUES(updated)"
		if _, err := oDb.ExecContext(ctx, query, nodeID, clusterID); err != nil {
			return ClusterClaimNone, fmt.Errorf("ClaimClusterMembership claim: %w", err)
		}
		oDb.SetChange("node_cluster_claims")
		return ClusterClaimPending, nil
	}
	if err := oDb.setNodeClusterID(ctx, nodeID, clusterID); err != nil {
		return ClusterClaimNone, fmt.Errorf("ClaimClusterMembership: %w", err)
	}
	if known > 0 {
		return ClusterClaimOrphan, nil
	}
	return ClusterClaimNew, nil
}

// ConfirmClusterClaims makes members of the cluster the nodes claiming it
// whose names the daemon status of a confirmed member lists. The caller is
// responsible for the member being confirmed. A name claimed by several
// nodes, or borne by a member already, confirms none: the collector can not
// tell which node the member means.
func (oDb *DB) ConfirmClusterClaims(ctx context.Context, clusterID string, nodenames []string) ([]ConfirmedClusterClaim, error) {
	if clusterID == "" || len(nodenames) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(nodenames)), ",")
	args := make([]any, 0, len(nodenames)+1)
	args = append(args, clusterID)
	for _, s := range nodenames {
		args = append(args, s)
	}
	query := "SELECT n.node_id, n.nodename," +
		" (SELECT COUNT(*) FROM nodes m WHERE m.cluster_id = c.cluster_id AND m.nodename = n.nodename)" +
		" FROM node_cluster_claims c JOIN nodes n ON n.node_id = c.node_id" +
		" WHERE c.cluster_id = ? AND n.nodename IN (" + placeholders + ")"
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ConfirmClusterClaims: %w", err)
	}
	var (
		candidates []ConfirmedClusterClaim
		byName     = make(map[string]int)
	)
	for rows.Next() {
		var (
			c       ConfirmedClusterClaim
			members int
		)
		if err := rows.Scan(&c.NodeID, &c.Nodename, &members); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("ConfirmClusterClaims scan: %w", err)
		}
		key := strings.ToLower(c.Nodename)
		if members > 0 {
			// borne by a member: never confirmed
			byName[key] += 2
		}
		byName[key]++
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("ConfirmClusterClaims rows: %w", err)
	}
	_ = rows.Close()

	var confirmed []ConfirmedClusterClaim
	for _, c := range candidates {
		if byName[strings.ToLower(c.Nodename)] != 1 {
			continue
		}
		if err := oDb.setNodeClusterID(ctx, c.NodeID, clusterID); err != nil {
			return confirmed, fmt.Errorf("ConfirmClusterClaims: %w", err)
		}
		confirmed = append(confirmed, c)
	}
	return confirmed, nil
}

// setNodeClusterID makes the node a member of the cluster, dropping its
// claim.
func (oDb *DB) setNodeClusterID(ctx context.Context, nodeID, clusterID string) error {
	if _, err := oDb.ExecContext(ctx, "UPDATE nodes SET cluster_id = ? WHERE node_id = ?", clusterID, nodeID); err != nil {
		return fmt.Errorf("set node cluster id: %w", err)
	}
	oDb.SetChange("nodes")
	return oDb.deleteClusterClaim(ctx, nodeID)
}

func (oDb *DB) deleteClusterClaim(ctx context.Context, nodeID string) error {
	if count, err := oDb.execCountContext(ctx, "DELETE FROM node_cluster_claims WHERE node_id = ?", nodeID); err != nil {
		return fmt.Errorf("delete cluster claim: %w", err)
	} else if count > 0 {
		oDb.SetChange("node_cluster_claims")
	}
	return nil
}

// DashboardUpdateNodeClusterClaims raises an alert on the nodes claiming a
// cluster no member confirmed for maxAge: an om3 join is confirmed by the
// next daemon status of a member, seconds later. Left pending, the claim is a
// node reporting a cluster it does not belong to, a misconfiguration or a
// node trying to pass for a member, or a name several nodes claim.
func (oDb *DB) DashboardUpdateNodeClusterClaims(ctx context.Context, maxAge time.Duration) error {
	now, err := oDb.databaseNow(ctx)
	if err != nil {
		return err
	}
	u := alertsUpsert{
		source: `SELECT
		  "node cluster not confirmed" AS dash_type,
		  "" AS svc_id,
		  2 AS dash_severity,
		  "claims the cluster %(cluster_id)s, not confirmed by its members" AS dash_fmt,
		  CONCAT('{"cluster_id": "', c.cluster_id, '"}') AS dash_dict,
		  c.created AS dash_created,
		  MD5(CONCAT('{"cluster_id": "', c.cluster_id, '"}')) AS dash_dict_md5,
		  n.node_env AS dash_env,
		  ? AS dash_updated,
		  c.node_id,
		  NULL AS dash_instance
		FROM node_cluster_claims c
		JOIN nodes n ON n.node_id = c.node_id
		WHERE c.created < ? - INTERVAL ? SECOND`,
		args:    []any{now, now, maxAgeSeconds(maxAge)},
		columns: positionalAlertColumns,
		update:  []string{"dash_updated = s.dash_updated", "dash_fmt = s.dash_fmt", "dash_dict = s.dash_dict", "dash_dict_md5 = s.dash_dict_md5"},
	}
	if count, err := oDb.upsertAlerts(ctx, u); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	const request = `DELETE FROM dashboard WHERE dash_type = "node cluster not confirmed" AND (dash_updated < ? OR dash_updated IS NULL)`
	if count, err := oDb.execCountContext(ctx, request, now); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}
