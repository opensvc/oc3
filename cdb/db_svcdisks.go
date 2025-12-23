package cdb

import (
	"context"
	"fmt"
)

func (oDb *DB) PurgeSvcdisksOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM svcdisks
		WHERE
		  disk_updated < DATE_SUB(NOW(), INTERVAL 2 DAY)`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("purge svcdisks: %w", err)
	} else if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count svcdisks deleted: %w", err)
	} else if affected > 0 {
		oDb.SetChange("svcdisks")
	}
	return nil
}
