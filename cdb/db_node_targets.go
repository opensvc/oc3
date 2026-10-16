package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

// GetNodeTargets returns the array ports a node sees through its host bus
// adapters, as the historical /nodes/<id>/targets listed them: the stor_zone
// rows of the node joined to the array owning each target port. A target port no
// array declares is left out, as the historical inner join did. A caller who is
// not a Manager sees the targets of the nodes of the apps their groups are
// responsible for.
func (oDb *DB) GetNodeTargets(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getNodeTargets: no columns selected")
	}
	q := From(schema.TStorZone).
		RawSelect(p.SelectExprs...).
		LeftJoinOn(schema.TStorArrayTgtid, "stor_array_tgtid.array_tgtid = stor_zone.tgt_id").
		LeftJoinOn(schema.TStorArray, "stor_array.id = stor_array_tgtid.array_id").
		Where(schema.StorZoneNodeID, "=", nodeID).
		WhereRaw("stor_array.id IS NOT NULL")
	q = applyNodeAppAuth(q, nodeID, p.Groups, p.IsManager)
	q = q.WhereFilters(p.Filters)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("getNodeTargets: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("stor_array.array_name, stor_zone.hba_id, stor_zone.tgt_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeTargets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
