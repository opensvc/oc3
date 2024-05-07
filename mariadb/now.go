package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func Now(ctx context.Context, db *sql.DB) (time.Time, error) {
	var t time.Time
	rows, err := db.QueryContext(ctx, "SELECT NOW()")
	if err != nil {
		return t, err
	}
	if rows == nil {
		return t, fmt.Errorf("no result rows for SELECT NOW()")
	}
	defer rows.Close()
	if !rows.Next() {
		return t, fmt.Errorf("no result rows next for SELECT NOW()")
	}
	if err := rows.Scan(&t); err != nil {
		return t, err
	}
	return t, nil
}
