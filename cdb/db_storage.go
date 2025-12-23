package cdb

import (
	"context"
	"fmt"
)

func (oDb *DB) PurgeStorArrayOutdated(ctx context.Context) error {
	var query = `DELETE FROM stor_array
		WHERE
		  array_model LIKE "vdisk%" AND
		  array_updated < DATE_SUB(NOW(), INTERVAL 2 DAY)`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("purge stor_array: %w", err)
	} else if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count stor_array deleted: %w", err)
	} else if affected > 0 {
		oDb.SetChange("stor_array")
	}
	return nil
}
