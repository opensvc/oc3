package cdb

import (
	"context"
	"database/sql"
	"fmt"
)

// FiltersetUsingRef is a filterset holding the object being looked up, with its
// number of entries: a filterset whose only entry is removed selects nothing.
type FiltersetUsingRef struct {
	ID      int    `json:"id"`
	Name    string `json:"fset_name"`
	LogOp   string `json:"f_log_op"`
	Entries int    `json:"entries"`
}

// FiltersetUserRef is a user whose session filter is a filterset.
type FiltersetUserRef struct {
	ID    int    `json:"id"`
	Email string `json:"email"`
	Name  string `json:"name"`
}

// FiltersetComparisonRef is a statistics comparison including a filterset.
type FiltersetComparisonRef struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// FiltersetSysreportGrant lets a team read the sysreport files matching a pattern
// on the nodes of a filterset.
type FiltersetSysreportGrant struct {
	ID      int    `json:"id"`
	Role    string `json:"role"`
	Pattern string `json:"pattern"`
}

// usingFiltersets lists the filtersets with an entry matching cond, with the
// operator of that entry and their number of entries.
func (oDb *DB) usingFiltersets(ctx context.Context, name, cond string, arg int) ([]FiltersetUsingRef, error) {
	query := `SELECT f.id, f.fset_name, MIN(ff.f_log_op),
			(SELECT COUNT(*) FROM gen_filtersets_filters n WHERE n.fset_id = f.id)
		FROM gen_filtersets_filters ff
		JOIN gen_filtersets f ON f.id = ff.fset_id
		WHERE ` + cond + `
		GROUP BY f.id, f.fset_name
		ORDER BY f.fset_name`
	rows, err := oDb.DB.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetUsingRef, 0)
	for rows.Next() {
		var (
			ref      FiltersetUsingRef
			fsetName sql.NullString
			logOp    sql.NullString
		)
		if err := rows.Scan(&ref.ID, &fsetName, &logOp, &ref.Entries); err != nil {
			return nil, fmt.Errorf("%s scan: %w", name, err)
		}
		ref.Name, ref.LogOp = fsetName.String, logOp.String
		out = append(out, ref)
	}
	return out, rows.Err()
}

// FilterUsageFiltersets lists the filtersets holding the filter fID.
func (oDb *DB) FilterUsageFiltersets(ctx context.Context, fID int) ([]FiltersetUsingRef, error) {
	// A filter entry carries the filter id; a nested filterset entry carries 0.
	return oDb.usingFiltersets(ctx, "FilterUsageFiltersets", "ff.f_id = ?", fID)
}

// FiltersetUsageParents lists the filtersets nesting the filterset fsetID.
func (oDb *DB) FiltersetUsageParents(ctx context.Context, fsetID int) ([]FiltersetUsingRef, error) {
	return oDb.usingFiltersets(ctx, "FiltersetUsageParents", "ff.encap_fset_id = ?", fsetID)
}

// FiltersetUsageUsers lists the users whose session filter is the filterset.
func (oDb *DB) FiltersetUsageUsers(ctx context.Context, fsetID int) ([]FiltersetUserRef, error) {
	const query = `SELECT u.id, COALESCE(u.email, ''),
			TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, '')))
		FROM gen_filterset_user fu
		JOIN auth_user u ON u.id = fu.user_id
		WHERE fu.fset_id = ?
		ORDER BY u.email`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageUsers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetUserRef, 0)
	for rows.Next() {
		var ref FiltersetUserRef
		if err := rows.Scan(&ref.ID, &ref.Email, &ref.Name); err != nil {
			return nil, fmt.Errorf("FiltersetUsageUsers scan: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// FiltersetUsageComparisons lists the statistics comparisons including the filterset.
func (oDb *DB) FiltersetUsageComparisons(ctx context.Context, fsetID int) ([]FiltersetComparisonRef, error) {
	const query = `SELECT c.id, c.name
		FROM stats_compare_fset cf
		JOIN stats_compare c ON c.id = cf.compare_id
		WHERE cf.fset_id = ?
		ORDER BY c.name`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageComparisons: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetComparisonRef, 0)
	for rows.Next() {
		var ref FiltersetComparisonRef
		if err := rows.Scan(&ref.ID, &ref.Name); err != nil {
			return nil, fmt.Errorf("FiltersetUsageComparisons scan: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// FiltersetUsageSysreportGrants lists the sysreport access grants on the nodes of
// the filterset.
func (oDb *DB) FiltersetUsageSysreportGrants(ctx context.Context, fsetID int) ([]FiltersetSysreportGrant, error) {
	const query = `SELECT a.id, COALESCE(g.role, ''), a.pattern
		FROM sysrep_allow a
		LEFT JOIN auth_group g ON g.id = a.group_id
		WHERE a.fset_id = ?
		ORDER BY g.role, a.pattern`
	rows, err := oDb.DB.QueryContext(ctx, query, fsetID)
	if err != nil {
		return nil, fmt.Errorf("FiltersetUsageSysreportGrants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]FiltersetSysreportGrant, 0)
	for rows.Next() {
		var ref FiltersetSysreportGrant
		if err := rows.Scan(&ref.ID, &ref.Role, &ref.Pattern); err != nil {
			return nil, fmt.Errorf("FiltersetUsageSysreportGrants scan: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}
