package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

type (
	VFilterset struct {
		FsetName    string
		FsetID      int
		JoinID      int
		FOrder      int
		FID         *int
		EncapFsetID *int
		FLogOp      string
		FTable      *string
		FField      *string
		FValue      *string
		FOp         *string
		FLabel      *string
	}

	Filterset struct {
		ID      int
		Name    string
		Author  string
		Updated time.Time
		Stats   bool
	}

	FiltersetIDsMap map[int]FiltersetIDs

	FiltersetIDs struct {
		NodeIDs string
		SvcIDs  string
	}
)

type FiltersetRef struct {
	ID   int    `json:"id"`
	Name string `json:"fset_name,omitempty"`
}

type FiltersetThreshold struct {
	ChkType     string
	ChkInstance string
	ChkLow      string
	ChkHigh     string
}

// FiltersetByIDOrName returns the (id, fset_name) of a filterset
func (oDb *DB) FiltersetByIDOrName(ctx context.Context, idOrName string) (int, string, error) {
	var (
		query string
		args  []any
	)
	if _, err := strconv.Atoi(idOrName); err == nil {
		query = "SELECT id, fset_name FROM gen_filtersets WHERE id = ?"
		args = []any{idOrName}
	} else {
		query = "SELECT id, fset_name FROM gen_filtersets WHERE fset_name = ?"
		args = []any{idOrName}
	}
	row := oDb.DB.QueryRowContext(ctx, query, args...)
	var (
		id   int
		name sql.NullString
	)
	if err := row.Scan(&id, &name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, "", nil
		}
		return 0, "", fmt.Errorf("FiltersetByIDOrName: %w", err)
	}
	return id, name.String, nil
}

// FiltersetUsageEncapFiltersets lists the filtersets encapsulating the given fsetID.
func (oDb *DB) FiltersetUsageEncapFiltersets(ctx context.Context, fsetID int) ([]FiltersetRef, error) {
	const query = `SELECT gen_filtersets.fset_name, gen_filtersets.id
		FROM gen_filtersets_filters
		JOIN gen_filtersets ON gen_filtersets.id = gen_filtersets_filters.fset_id
		WHERE gen_filtersets_filters.encap_fset_id = ?
		GROUP BY gen_filtersets.fset_name
		ORDER BY gen_filtersets.fset_name`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageEncapFiltersets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetRef, 0)
	for rows.Next() {
		var (
			name sql.NullString
			id   int
		)
		if err := rows.Scan(&name, &id); err != nil {
			return nil, fmt.Errorf("FiltersetUsageEncapFiltersets scan: %w", err)
		}
		out = append(out, FiltersetRef{ID: id, Name: name.String})
	}
	return out, nil
}

// FiltersetUsageRulesets lists the comp_rulesets attached to the given filterset.
func (oDb *DB) FiltersetUsageRulesets(ctx context.Context, fsetID int) ([]FiltersetRef, error) {
	const query = `SELECT comp_rulesets.ruleset_name, comp_rulesets.id
		FROM comp_rulesets_filtersets
		JOIN comp_rulesets ON comp_rulesets.id = comp_rulesets_filtersets.ruleset_id
		WHERE comp_rulesets_filtersets.fset_id = ?
		ORDER BY comp_rulesets.ruleset_name`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageRulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetRef, 0)
	for rows.Next() {
		var (
			name sql.NullString
			id   int
		)
		if err := rows.Scan(&name, &id); err != nil {
			return nil, fmt.Errorf("FiltersetUsageRulesets scan: %w", err)
		}
		out = append(out, FiltersetRef{ID: id, Name: name.String})
	}
	return out, nil
}

// FiltersetUsageThresholds returns the gen_filterset_check_threshold rows for the given filterset.
func (oDb *DB) FiltersetUsageThresholds(ctx context.Context, fsetID int) ([]FiltersetThreshold, error) {
	const query = `SELECT chk_type, chk_instance, chk_low, chk_high
		FROM gen_filterset_check_threshold
		WHERE fset_id = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageThresholds: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetThreshold, 0)
	for rows.Next() {
		var t FiltersetThreshold
		if err := rows.Scan(&t.ChkType, &t.ChkInstance, &t.ChkLow, &t.ChkHigh); err != nil {
			return nil, fmt.Errorf("FiltersetUsageThresholds scan: %w", err)
		}
		out = append(out, t)
	}
	return out, nil
}

// GetFilterset returns a single gen_filtersets row by id or fset_name.
func (oDb *DB) GetFilterset(ctx context.Context, idOrName string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFilterset: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM gen_filtersets WHERE "
	args := []any{}
	if _, err := strconv.Atoi(idOrName); err == nil {
		query += "gen_filtersets.id = ?"
		args = append(args, idOrName)
	} else {
		query += "gen_filtersets.fset_name = ?"
		args = append(args, idOrName)
	}
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFilterset: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetFiltersets returns rows from the gen_filtersets table.
func (oDb *DB) GetFiltersets(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFiltersets: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM gen_filtersets WHERE gen_filtersets.id > 0"
	args := []any{}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("gen_filtersets.fset_name, gen_filtersets.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFiltersets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetVFiltersetsWithEncap(ctx context.Context, fsetID int) (l []VFilterset, err error) {
	l, err = oDb.GetVFiltersets(ctx, fsetID)
	if err != nil {
		return
	}
	var more []VFilterset
	for _, e := range l {
		if e.EncapFsetID == nil {
			continue
		}
		if l, err := oDb.GetVFiltersets(ctx, *e.EncapFsetID); err != nil {
			return nil, err
		} else {
			more = append(more, l...)
		}
	}
	l = append(l, more...)
	return
}

func (oDb *DB) GetVFiltersets(ctx context.Context, fsetID int) (l []VFilterset, err error) {
	const query = `
			SELECT
				fset_name,
				fset_id,
				join_id,
				f_order,
				f_id,
				encap_fset_id,
				f_log_op,
				f_table,
				f_field,
				f_value,
				f_op,
				f_label
			FROM
				v_gen_filtersets
			WHERE
				fset_id=?
			ORDER BY
				f_order, f_id
		`

	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var e VFilterset
		if err = rows.Scan(&e.FsetName, &e.FsetID, &e.JoinID, &e.FOrder, &e.FID, &e.EncapFsetID, &e.FLogOp, &e.FTable, &e.FField, &e.FValue, &e.FOp, &e.FLabel); err != nil {
			return
		}
		l = append(l, e)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}

func (oDb *DB) ResolveFilterset(ctx context.Context, fsetID int, refField string) (l []string, err error) {
	var (
		query  string
		args   []any
		encap  func(VFilterset) error
		direct func(VFilterset) error
	)
	direct = func(e VFilterset) error {
		if e.FOp == nil || e.FValue == nil || e.FTable == nil || e.FField == nil {
			return nil
		}
		var q, not, pre, sub string
		switch e.FLogOp {
		case "AND NOT", "OR NOT":
			not = "NOT"
		}
		switch refField {
		case "svc_id":
			pre = fmt.Sprintf(`SELECT svc_id FROM services WHERE svc_id != "" AND svc_id %s IN`, not)
			switch *e.FTable {
			case "nodes", "node_ip", "node_hba", "packages", "patches", "diskinfo":
				sub = fmt.Sprintf("SELECT svc_id FROM services JOIN svcmon USING (svc_id) JOIN %s USING (node_id)", *e.FTable)
			case "apps":
				sub = fmt.Sprintf("SELECT svc_id FROM services JOIN apps ON apps.app = services.svc_app")
			case "services":
				sub = fmt.Sprintf("SELECT svc_id FROM services")
			default:
				sub = fmt.Sprintf("SELECT svc_id FROM services JOIN %s USING (svc_id)", *e.FTable)
			}
		case "node_id":
			pre = fmt.Sprintf(`SELECT node_id FROM nodes WHERE node_id != "" AND node_id %s IN`, not)
			switch *e.FTable {
			case "services":
				sub = fmt.Sprintf("SELECT node_id FROM nodes JOIN svcmon USING (node_id) JOIN %s USING (svc_id)", *e.FTable)
			case "apps":
				sub = fmt.Sprintf("SELECT node_id FROM nodes JOIN apps ON apps.app = nodes.app")
			case "nodes":
				sub = fmt.Sprintf("SELECT node_id FROM nodes")
			default:
				sub = fmt.Sprintf("SELECT node_id FROM nodes JOIN %s USING (node_id)", *e.FTable)
			}
		}
		switch *e.FOp {
		case "=", "<=", ">=", "<", ">", "!=", "LIKE", "NOT LIKE":
			q = fmt.Sprintf(`%s (%s WHERE %s.%s %s ?)`, pre, sub, *e.FTable, *e.FField, *e.FOp)
			args = append(args, *e.FValue)
		case "IN", "NOT IN":
			values := strings.Split(*e.FValue, ",")
			q = fmt.Sprintf(`%s (%s WHERE %s.%s %s (%s))`, pre, sub, *e.FTable, *e.FField, *e.FOp, Placeholders(len(values)))
			for _, v := range values {
				args = append(args, v)
			}
		default:
			return fmt.Errorf("unexpected f_op: %s", *e.FOp)
		}
		if query == "" {
			query += q
			return nil
		}
		switch e.FLogOp {
		case "OR NOT", "OR":
			query = fmt.Sprintf("(%s) UNION %s", query, q)
		case "AND NOT", "AND":
			query = fmt.Sprintf("(%s) INTERSECT %s", query, q)
		default:
			return fmt.Errorf("unexpected f_log_op: %s", e.FLogOp)
		}
		return nil
	}
	encap = func(e VFilterset) error {
		l, err := oDb.GetVFiltersets(ctx, *e.EncapFsetID)
		if err != nil {
			return err
		}
		for _, e := range l {
			if e.EncapFsetID == nil {
				if err := direct(e); err != nil {
					return err
				}
			} else {
				if err := encap(e); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := encap(VFilterset{FLogOp: "AND", EncapFsetID: &fsetID}); err != nil {
		return nil, err
	}
	slog.Debug(fmt.Sprintf("%d: %s, %v", fsetID, query, args))
	if query == "" {
		return
	}
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return
		}
		l = append(l, id)
	}
	slog.Debug(fmt.Sprintf("%d: %v", fsetID, l))
	if err = rows.Err(); err != nil {
		return
	}
	return
}

func (oDb *DB) ResolveFiltersets(ctx context.Context) (FiltersetIDsMap, error) {
	m := make(FiltersetIDsMap)
	filtersets, err := oDb.GetStatsFiltersets(ctx)
	if err != nil {
		return nil, err
	}
	toString := func(l []string) string {
		for i, s := range l {
			l[i] = fmt.Sprintf(`"%s"`, s)
		}
		return strings.Join(l, ",")
	}
	for _, filterset := range filtersets {
		var e FiltersetIDs
		if l, err := oDb.ResolveFilterset(ctx, filterset.ID, "node_id"); err != nil {
			return nil, err
		} else {
			e.NodeIDs = toString(l)
		}
		if l, err := oDb.ResolveFilterset(ctx, filterset.ID, "svc_id"); err != nil {
			return nil, err
		} else {
			e.SvcIDs = toString(l)
		}
		m[filterset.ID] = e
		slog.Debug(fmt.Sprintf("%d: %#v", filterset.ID, e))
	}
	return m, nil
}

/*
	// ResolveFiltersetsFromCache is not needed as its only user is the metrics task
	// that refreshed the cache ... just use the ResolveFiltersets() directly from this
	// task.
	func (oDb *DB) ResolveFiltersetsFromCache(ctx context.Context) (FiltersetIDsMap, error) {
		const query = `
			SELECT
			    fset_id,
			    GROUP_CONCAT(IF(obj_type = 'svc_id', CONCAT('"', obj_id, '"'), NULL)) AS svc_ids,
			    GROUP_CONCAT(IF(obj_type = 'node_id', CONCAT('"', obj_id, '"'), NULL)) AS node_ids
			FROM
			    fset_cache
			GROUP BY
			    fset_id;
			`
		m := make(FiltersetIDsMap)
		rows, err := oDb.DB.QueryContext(ctx, query)
		if err != nil {
			return m, err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var (
				id              int
				svcIDs, nodeIDs sql.NullString
			)
			if err = rows.Scan(&id, &svcIDs, &nodeIDs); err != nil {
				return m, err
			}
			e := FiltersetIDs{}
			if nodeIDs.Valid {
				e.NodeIDs = nodeIDs.String
			} else {
				e.NodeIDs = "NULL"
			}
			if svcIDs.Valid {
				e.SvcIDs = svcIDs.String
			} else {
				e.SvcIDs = "NULL"
			}
			m[id] = e
		}
		if err = rows.Err(); err != nil {
			return m, err
		}
		return m, nil
	}
*/

func (oDb *DB) GetStatsFiltersets(ctx context.Context) (fsets []Filterset, err error) {
	const query = `
                SELECT id, fset_name, fset_author, fset_updated, fset_stats
                FROM gen_filtersets
                WHERE fset_stats="T"`
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var fset Filterset
		if err = rows.Scan(&fset.ID, &fset.Name, &fset.Author, &fset.Updated, &fset.Stats); err != nil {
			return
		}
		fsets = append(fsets, fset)
	}
	if err = rows.Err(); err != nil {
		return
	}
	return
}
