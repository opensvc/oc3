package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MetricSamples is the result of a metric request: its columns, in order, and its
// rows. Truncated says that the request returned more rows than were kept.
type MetricSamples struct {
	Columns   []string
	Rows      [][]any
	Truncated bool
}

// MetricVisible returns the request of a metric the caller may see, and whether
// there is one.
func (oDb *DB) MetricVisible(ctx context.Context, id string, groups []string, isManager bool) (string, bool, error) {
	visibility, args := metricsVisibility(groups, isManager)
	var request sql.NullString
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT metric_sql FROM metrics WHERE metrics.id = ?"+visibility, append([]any{id}, args...)...).Scan(&request)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("MetricVisible: %w", err)
	}
	return request.String, true, nil
}

// ReportVisible returns the definition of a report the caller may see, and whether
// there is one.
func (oDb *DB) ReportVisible(ctx context.Context, id string, groups []string, isManager bool) (string, bool, error) {
	visibility, args := reportsVisibility(groups, isManager)
	var definition sql.NullString
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT report_yaml FROM reports WHERE reports.id = ?"+visibility, append([]any{id}, args...)...).Scan(&definition)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ReportVisible: %w", err)
	}
	return definition.String, true, nil
}

var uuidLike = regexp.MustCompile(`^[0-9a-fA-F-]+$`)

// quotedIDs is an SQL list of the ids, for an IN (...): ids are UUIDs, checked as
// such before being written into the request. An empty list matches nothing.
func quotedIDs(ids []string) string {
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		if uuidLike.MatchString(id) {
			quoted = append(quoted, "'"+id+"'")
		}
	}
	if len(quoted) == 0 {
		return "''"
	}
	return strings.Join(quoted, ",")
}

// visibleIDs lists the ids the query returns for the caller's groups.
func (oDb *DB) visibleIDs(ctx context.Context, query string, groups []string) ([]string, error) {
	cleanGroups := cleanGroups(groups)
	if len(cleanGroups) == 0 {
		return nil, nil
	}
	args := make([]any, len(cleanGroups))
	for i, g := range cleanGroups {
		args[i] = g
	}
	rows, err := oDb.DB.QueryContext(ctx, fmt.Sprintf(query, Placeholders(len(cleanGroups))), args...)
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
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// MetricScope is what the %%fset_node_ids%% and %%fset_svc_ids%% placeholders of a
// metric request stand for when the caller reads it: the historical collector used
// the filterset of the session, which oc3 does not have; here they are the nodes and
// services the caller may see — all of them for a manager.
func (oDb *DB) MetricScope(ctx context.Context, groups []string, isManager bool) (nodes, services string, err error) {
	if isManager {
		return "SELECT node_id FROM nodes", "SELECT svc_id FROM services", nil
	}
	const responsibleApps = "SELECT a.app FROM apps a" +
		" JOIN apps_responsibles ar ON ar.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ar.group_id" +
		" WHERE ag.role IN (%s)"
	nodeIDs, err := oDb.visibleIDs(ctx, "SELECT node_id FROM nodes WHERE app IN ("+responsibleApps+")", groups)
	if err != nil {
		return "", "", fmt.Errorf("MetricScope nodes: %w", err)
	}
	svcIDs, err := oDb.visibleIDs(ctx, "SELECT svc_id FROM services WHERE svc_app IN ("+responsibleApps+")", groups)
	if err != nil {
		return "", "", fmt.Errorf("MetricScope services: %w", err)
	}
	return quotedIDs(nodeIDs), quotedIDs(svcIDs), nil
}

// RunMetricRequest runs a metric request in a read-only transaction and keeps at
// most limit rows. Numbers come back as numbers, dates and texts as text.
func (oDb *DB) RunMetricRequest(ctx context.Context, request string, limit int) (*MetricSamples, error) {
	if oDb.dbPool == nil {
		return nil, fmt.Errorf("RunMetricRequest: no connection pool")
	}
	// Read-only, on a connection of its own: a request that writes is refused by
	// the database, and the transaction is rolled back whatever it read.
	tx, err := oDb.dbPool.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("RunMetricRequest: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	samples := &MetricSamples{Columns: columns, Rows: [][]any{}}
	for rows.Next() {
		if len(samples.Rows) >= limit {
			samples.Truncated = true
			break
		}
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]any, len(columns))
		for i, value := range values {
			row[i] = sampleValue(value, types[i].DatabaseTypeName())
		}
		samples.Rows = append(samples.Rows, row)
	}
	return samples, rows.Err()
}

// sampleValue turns what the driver returns into a JSON value: the bytes of a
// numeric column into a number, of the others into text.
func sampleValue(value any, dbType string) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		text := string(v)
		switch dbType {
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "UNSIGNED TINYINT", "UNSIGNED SMALLINT",
			"UNSIGNED MEDIUMINT", "UNSIGNED INT", "UNSIGNED BIGINT", "YEAR":
			if n, err := strconv.ParseInt(text, 10, 64); err == nil {
				return n
			}
		case "DECIMAL", "FLOAT", "DOUBLE":
			if f, err := strconv.ParseFloat(text, 64); err == nil {
				return f
			}
		}
		return text
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	default:
		return v
	}
}

// ChartVisible returns the definition of a chart the caller may see, and whether
// there is one.
func (oDb *DB) ChartVisible(ctx context.Context, id string, groups []string, isManager bool) (string, bool, error) {
	visibility, args := chartsVisibility(groups, isManager)
	var definition sql.NullString
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT chart_yaml FROM charts WHERE charts.id = ?"+visibility, append([]any{id}, args...)...).Scan(&definition)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("ChartVisible: %w", err)
	}
	return definition.String, true, nil
}
