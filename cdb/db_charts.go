package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// chartsVisibility restricts the charts to those published to one of the
// caller's groups, as the historical collector did for whoever is not a Manager.
func chartsVisibility(groups []string, isManager bool) (string, []any) {
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
	return " AND charts.id IN (" +
		"SELECT ctp.chart_id FROM chart_team_publication ctp" +
		" JOIN auth_group ag ON ag.id = ctp.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(cleanGroups)) + "))", args
}

func (oDb *DB) queryCharts(ctx context.Context, chartID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("queryCharts: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM charts WHERE charts.id > 0"
	visibility, args := chartsVisibility(p.Groups, p.IsManager)
	query += visibility
	if chartID != "" {
		query += " AND charts.id = ?"
		args = append(args, chartID)
	}
	conds, filterArgs := p.FilterConditions()
	for _, cond := range conds {
		query += " AND " + cond
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("charts.chart_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("queryCharts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetCharts returns the charts the caller may see.
func (oDb *DB) GetCharts(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryCharts(ctx, "", p)
}

// GetChart returns one chart, if the caller may see it.
func (oDb *DB) GetChart(ctx context.Context, chartID string, p ListParams) ([]map[string]any, error) {
	return oDb.queryCharts(ctx, chartID, p)
}

// ChartByName returns the id of the chart of this name.
func (oDb *DB) ChartByName(ctx context.Context, name string) (int, bool, error) {
	var id int
	err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM charts WHERE chart_name = ? LIMIT 1", name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("ChartByName: %w", err)
	}
	return id, true, nil
}

// InsertChart creates a chart and returns its id.
func (oDb *DB) InsertChart(ctx context.Context, name, yamlText string) (int, error) {
	res, err := oDb.ExecContext(ctx, "INSERT INTO charts (chart_name, chart_yaml) VALUES (?, ?)", name, yamlText)
	if err != nil {
		return 0, fmt.Errorf("InsertChart: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertChart lastInsertId: %w", err)
	}
	oDb.SetChange("charts")
	return int(id), nil
}

// AddChartTeams publishes a chart to a team and makes the team responsible for
// it, as the historical collector does for the team of the chart's author.
func (oDb *DB) AddChartTeams(ctx context.Context, chartID int, groupID int64) error {
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO chart_team_publication (chart_id, group_id) VALUES (?, ?)", chartID, groupID); err != nil {
		return fmt.Errorf("AddChartTeams publication: %w", err)
	}
	oDb.SetChange("chart_team_publication")
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO chart_team_responsible (chart_id, group_id) VALUES (?, ?)", chartID, groupID); err != nil {
		return fmt.Errorf("AddChartTeams responsible: %w", err)
	}
	oDb.SetChange("chart_team_responsible")
	return nil
}
