package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// reportsVisibility restricts the reports to those published to one of the
// caller's groups, as the historical collector did for whoever is not a Manager.
func reportsVisibility(groups []string, isManager bool) (string, []any) {
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
	return " AND reports.id IN (" +
		"SELECT rtp.report_id FROM report_team_publication rtp" +
		" JOIN auth_group ag ON ag.id = rtp.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(cleanGroups)) + "))", args
}

func (oDb *DB) queryReports(ctx context.Context, reportID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("queryReports: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM reports WHERE reports.id > 0"
	visibility, args := reportsVisibility(p.Groups, p.IsManager)
	query += visibility
	if reportID != "" {
		query += " AND reports.id = ?"
		args = append(args, reportID)
	}
	conds, filterArgs := p.FilterConditions()
	for _, cond := range conds {
		query += " AND " + cond
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("reports.report_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("queryReports: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetReports returns the reports the caller may see.
func (oDb *DB) GetReports(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.queryReports(ctx, "", p)
}

// GetReport returns one report, if the caller may see it.
func (oDb *DB) GetReport(ctx context.Context, reportID string, p ListParams) ([]map[string]any, error) {
	return oDb.queryReports(ctx, reportID, p)
}

// ReportByName returns the id of the report of this name.
func (oDb *DB) ReportByName(ctx context.Context, name string) (int, bool, error) {
	var id int
	err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM reports WHERE report_name = ? LIMIT 1", name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("ReportByName: %w", err)
	}
	return id, true, nil
}

// InsertReport creates a report and returns its id.
func (oDb *DB) InsertReport(ctx context.Context, name, yamlText string) (int, error) {
	res, err := oDb.ExecContext(ctx, "INSERT INTO reports (report_name, report_yaml) VALUES (?, ?)", name, yamlText)
	if err != nil {
		return 0, fmt.Errorf("InsertReport: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertReport lastInsertId: %w", err)
	}
	oDb.SetChange("reports")
	return int(id), nil
}

// AddReportTeams publishes a report to a team and makes the team responsible for
// it, as the historical collector does for the team of the report's author.
func (oDb *DB) AddReportTeams(ctx context.Context, reportID int, groupID int64) error {
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO report_team_publication (report_id, group_id) VALUES (?, ?)", reportID, groupID); err != nil {
		return fmt.Errorf("AddReportTeams publication: %w", err)
	}
	oDb.SetChange("report_team_publication")
	if _, err := oDb.ExecContext(ctx,
		"INSERT INTO report_team_responsible (report_id, group_id) VALUES (?, ?)", reportID, groupID); err != nil {
		return fmt.Errorf("AddReportTeams responsible: %w", err)
	}
	oDb.SetChange("report_team_responsible")
	return nil
}
