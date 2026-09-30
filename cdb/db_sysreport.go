package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SysrepSecurePatterns returns the regular expressions marking the sysreport
// paths whose content is sensitive.
func (oDb *DB) SysrepSecurePatterns(ctx context.Context) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT pattern FROM sysrep_secure WHERE pattern IS NOT NULL ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("sysrepSecurePatterns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var patterns []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, fmt.Errorf("sysrepSecurePatterns: %w", err)
		}
		patterns = append(patterns, p)
	}
	return patterns, rows.Err()
}

// SysrepAllow is an authorization to read sensitive sysreport paths: the paths
// matching Pattern, on the nodes of the filterset FsetID.
type SysrepAllow struct {
	Pattern string
	FsetID  int
}

// SysrepAllows returns the authorizations given to any of the groups.
func (oDb *DB) SysrepAllows(ctx context.Context, groupIDs []int64) ([]SysrepAllow, error) {
	if len(groupIDs) == 0 {
		return nil, nil
	}
	args := make([]any, len(groupIDs))
	for i, id := range groupIDs {
		args[i] = id
	}
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT pattern, fset_id FROM sysrep_allow WHERE fset_id > 0 AND group_id IN ("+Placeholders(len(groupIDs))+")", args...)
	if err != nil {
		return nil, fmt.Errorf("sysrepAllows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var allows []SysrepAllow
	for rows.Next() {
		var a SysrepAllow
		if err := rows.Scan(&a.Pattern, &a.FsetID); err != nil {
			return nil, fmt.Errorf("sysrepAllows: %w", err)
		}
		allows = append(allows, a)
	}
	return allows, rows.Err()
}

// NodeTeamResponsible returns the team responsible for a node; false when
// there is no such node.
func (oDb *DB) NodeTeamResponsible(ctx context.Context, nodeID string) (string, bool, error) {
	var team sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT team_responsible FROM nodes WHERE node_id = ? LIMIT 1", nodeID).Scan(&team)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("nodeTeamResponsible: %w", err)
	}
	return team.String, true, nil
}
