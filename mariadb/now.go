package mariadb

import (
	"context"
	"database/sql"
	"fmt"
)

func Now(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "SELECT NOW()")
	if err != nil {
		return "", err
	}
	if rows == nil {
		return "", fmt.Errorf("no result rows for SELECT NOW()")
	}
	defer rows.Close()
	if !rows.Next() {
		return "", fmt.Errorf("no result rows next for SELECT NOW()")
	}
	var s string
	if err := rows.Scan(&s); err != nil {
		return "", err
	}
	return s, nil
}
