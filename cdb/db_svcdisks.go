package cdb

import (
	"context"
)

func (oDb *DB) PurgeSvcdisksOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM svcdisks
		WHERE
		  disk_updated < DATE_SUB(NOW(), INTERVAL 2 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("svcdisks")
	}
	return nil
}
