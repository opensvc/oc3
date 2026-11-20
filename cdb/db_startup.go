package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrStartupNodeUnknown tells a node the service has no instance on.
var ErrStartupNodeUnknown = errors.New("the service has no instance on this node")

// StartupRequest asks for the startup graph of a service on a node, the first
// of its nodes by name when NodeID is empty.
type StartupRequest struct {
	SvcID        string
	NodeID       string
	ShowDisabled bool
	Groups       []string
	IsManager    bool
}

// GetServiceStartup returns the startup graph of a service on one of its
// nodes, as the historical collector's json_startup_data: nil when the
// service is not one the caller may see, those of the applications its
// groups are responsible for unless a manager. A service whose configuration
// was never received has nodes and no vertices.
func (oDb *DB) GetServiceStartup(ctx context.Context, r StartupRequest) (*Startup, error) {
	scope, scopeArgs := appScope("svc_app", r.Groups, r.IsManager)
	var svcID, svcname string
	var config sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT svc_id, COALESCE(svcname, ''), svc_config FROM services WHERE svc_id = ? AND "+scope,
		append([]any{r.SvcID}, scopeArgs...)...).Scan(&svcID, &svcname, &config)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("startup service: %w", err)
	}

	rows, err := oDb.DB.QueryContext(ctx, "SELECT DISTINCT n.node_id, COALESCE(n.nodename, '') FROM svcmon m JOIN nodes n ON n.node_id = m.node_id"+
		" WHERE m.svc_id = ? ORDER BY n.nodename", svcID)
	if err != nil {
		return nil, fmt.Errorf("startup nodes: %w", err)
	}
	var nodes []StartupNode
	for rows.Next() {
		var n StartupNode
		if err := rows.Scan(&n.NodeID, &n.Nodename); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("startup nodes: %w", err)
		}
		nodes = append(nodes, n)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("startup nodes: %w", err)
	}

	var node *StartupNode
	for i := range nodes {
		if r.NodeID == "" || nodes[i].NodeID == r.NodeID {
			node = &nodes[i]
			break
		}
	}
	if node == nil && r.NodeID != "" {
		return nil, ErrStartupNodeUnknown
	}

	states := map[string]StartupResourceState{}
	encapStates := map[string]map[string]StartupResourceState{}
	if node != nil {
		rows, err := oDb.DB.QueryContext(ctx, "SELECT COALESCE(rid, ''), COALESCE(res_status, ''), COALESCE(res_type, ''), COALESCE(res_desc, ''),"+
			" COALESCE(vmname, ''), COALESCE(res_disable, ''), COALESCE(res_optional, ''), COALESCE(res_monitor, '')"+
			" FROM resmon WHERE svc_id = ? AND node_id = ?", svcID, node.NodeID)
		if err != nil {
			return nil, fmt.Errorf("startup resources: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var rid, vmname, disable, optional, monitor string
			var st StartupResourceState
			if err := rows.Scan(&rid, &st.Status, &st.Type, &st.Desc, &vmname, &disable, &optional, &monitor); err != nil {
				return nil, fmt.Errorf("startup resources: %w", err)
			}
			st.Disabled, st.Optional, st.Monitor = disable == "T", optional == "T", monitor == "T"
			if vmname != "" {
				if encapStates[vmname] == nil {
					encapStates[vmname] = map[string]StartupResourceState{}
				}
				encapStates[vmname][rid] = st
				continue
			}
			states[rid] = st
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("startup resources: %w", err)
		}
	}

	var s *Startup
	if config.Valid && config.String != "" && node != nil {
		s = BuildStartup(config.String, svcname, node.Nodename, states, encapStates, r.ShowDisabled)
	} else {
		s = &Startup{Vertices: []StartupVertex{}, Edges: []StartupEdge{}}
	}
	s.SvcID, s.Svcname = svcID, svcname
	if node != nil {
		s.NodeID, s.Nodename = node.NodeID, node.Nodename
	}
	s.Nodes = nodes
	if s.Nodes == nil {
		s.Nodes = []StartupNode{}
	}
	return s, nil
}
