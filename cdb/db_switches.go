package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

// GetSwitches lists the ports of the SAN switches, from the v_switches view: one
// row per port, with the name of what is plugged on its other end.
//
// No access control beyond authentication, as in the historical SAN switches
// table: a switch port has no owner, and the view is the map of the fabric.
func (oDb *DB) GetSwitches(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.getSwitches(ctx, nil, p)
}

// GetSwitchPort returns the port of a SAN switch of the given id, none when unknown.
func (oDb *DB) GetSwitchPort(ctx context.Context, id int, p ListParams) ([]map[string]any, error) {
	return oDb.getSwitches(ctx, &id, p)
}

func (oDb *DB) getSwitches(ctx context.Context, id *int, p ListParams) ([]map[string]any, error) {
	q := From(schema.TVSwitches).
		RawSelect(p.SelectExprs...).
		Where(schema.VSwitchesID, ">", 0)
	if id != nil {
		q = q.Where(schema.VSwitchesID, "=", *id)
	}
	query, args, err := q.WhereFilters(p.Filters).Build()
	if err != nil {
		return nil, fmt.Errorf("GetSwitches: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("v_switches.sw_name, v_switches.sw_index, v_switches.sw_portstate")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetSwitches: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
