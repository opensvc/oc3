package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// MetricFields are the editable properties of a metric. A nil field is left as it
// is; ClearInstanceIndex sets metric_col_instance_index to NULL.
type MetricFields struct {
	Name               *string
	SQL                *string
	ValueIndex         *int
	InstanceIndex      *int
	ClearInstanceIndex bool
	InstanceLabel      *string
	Historize          *string
}

// MetricRow is a metric as the change log describes it.
type MetricRow struct {
	ID            int
	Name          string
	SQL           string
	ValueIndex    sql.NullInt64
	InstanceIndex sql.NullInt64
	InstanceLabel string
	Historize     string
}

// metricsVisibility restricts the metrics to those published to one of the
// caller's groups, as the historical collector did for whoever is not a Manager.
func metricsVisibility(groups []string, isManager bool) (string, []any) {
	if isManager {
		return "", nil
	}
	cleanGroups := cleanGroups(groups)
	if len(cleanGroups) == 0 {
		return " AND 1=0", nil
	}
	args := make([]any, len(cleanGroups))
	for i, g := range cleanGroups {
		args[i] = g
	}
	return " AND metrics.id IN (" +
		"SELECT mtp.metric_id FROM metric_team_publication mtp" +
		" JOIN auth_group ag ON ag.id = mtp.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(cleanGroups)) + "))", args
}

func (oDb *DB) queryMetrics(ctx context.Context, metricID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("queryMetrics: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM metrics WHERE metrics.id > 0"
	visibility, args := metricsVisibility(p.Groups, p.IsManager)
	query += visibility
	if metricID != "" {
		query += " AND metrics.id = ?"
		args = append(args, metricID)
	}
	conds, filterArgs := p.FilterConditions()
	for _, cond := range conds {
		query += " AND " + cond
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("metrics.metric_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("queryMetrics: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetMetrics returns the metrics the caller may see.
func (oDb *DB) GetMetrics(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryMetrics(ctx, "", p)
}

// GetMetric returns one metric, if the caller may see it.
func (oDb *DB) GetMetric(ctx context.Context, metricID string, p ListParams) ([]map[string]any, error) {
	return oDb.queryMetrics(ctx, metricID, p)
}

// MetricRowByID returns a metric, or nil when there is none with this id.
func (oDb *DB) MetricRowByID(ctx context.Context, id int) (*MetricRow, error) {
	var m MetricRow
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id, metric_name, COALESCE(metric_sql, ''), metric_col_value_index,"+
			" metric_col_instance_index, COALESCE(metric_col_instance_label, ''),"+
			" COALESCE(metric_historize, 'F') FROM metrics WHERE id = ?", id).
		Scan(&m.ID, &m.Name, &m.SQL, &m.ValueIndex, &m.InstanceIndex, &m.InstanceLabel, &m.Historize)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("MetricRowByID: %w", err)
	}
	return &m, nil
}

// MetricByName returns the id of the metric of this name.
func (oDb *DB) MetricByName(ctx context.Context, name string) (int, bool, error) {
	var id int
	err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM metrics WHERE metric_name = ? LIMIT 1", name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("MetricByName: %w", err)
	}
	return id, true, nil
}

// metricAssignments are the SET clauses of the fields given.
func metricAssignments(f MetricFields) ([]string, []any) {
	var sets []string
	var args []any
	add := func(clause string, value any) {
		sets = append(sets, clause)
		args = append(args, value)
	}
	if f.Name != nil {
		add("metric_name = ?", *f.Name)
	}
	if f.SQL != nil {
		add("metric_sql = ?", *f.SQL)
	}
	if f.ValueIndex != nil {
		add("metric_col_value_index = ?", *f.ValueIndex)
	}
	if f.ClearInstanceIndex {
		sets = append(sets, "metric_col_instance_index = NULL")
	} else if f.InstanceIndex != nil {
		add("metric_col_instance_index = ?", *f.InstanceIndex)
	}
	if f.InstanceLabel != nil {
		add("metric_col_instance_label = ?", *f.InstanceLabel)
	}
	if f.Historize != nil {
		add("metric_historize = ?", *f.Historize)
	}
	return sets, args
}

// InsertMetric creates a metric and returns its id. The name is required.
func (oDb *DB) InsertMetric(ctx context.Context, f MetricFields, author string) (int, error) {
	if f.Name == nil {
		return 0, fmt.Errorf("InsertMetric: no name")
	}
	sets, args := metricAssignments(f)
	sets = append(sets, "metric_author = ?", "metric_created = NOW()")
	args = append(args, author)
	res, err := oDb.ExecContext(ctx, "INSERT INTO metrics SET "+strings.Join(sets, ", "), args...)
	if err != nil {
		return 0, fmt.Errorf("InsertMetric: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertMetric lastInsertId: %w", err)
	}
	oDb.SetChange("metrics")
	return int(id), nil
}

// UpdateMetric changes the fields given of a metric.
func (oDb *DB) UpdateMetric(ctx context.Context, id int, f MetricFields) error {
	sets, args := metricAssignments(f)
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	if _, err := oDb.ExecContext(ctx, "UPDATE metrics SET "+strings.Join(sets, ", ")+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("UpdateMetric: %w", err)
	}
	oDb.SetChange("metrics")
	return nil
}

// AddMetricPublication publishes a metric to a team.
func (oDb *DB) AddMetricPublication(ctx context.Context, metricID int, groupID int64) error {
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO metric_team_publication (metric_id, group_id) VALUES (?, ?)", metricID, groupID); err != nil {
		return fmt.Errorf("AddMetricPublication: %w", err)
	}
	oDb.SetChange("metric_team_publication")
	return nil
}
