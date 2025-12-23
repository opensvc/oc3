package cdb

import (
	"context"
)

func (oDb *DB) PurgeSvcdisksOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM svcdisks
		WHERE
		  disk_updated < DATE_SUB(NOW(), INTERVAL 2 DAY)`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("svcdisks")
	}
	return nil
}
