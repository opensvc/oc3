package cdb

import (
	"context"
	"time"
)

func (oDb *DB) PurgeSvcdisksOutdated(ctx context.Context, maxAge time.Duration) error {
	var query = `DELETE
		FROM svcdisks
		WHERE
		  disk_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`
	if count, err := oDb.execCountContext(ctx, query, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("svcdisks")
	}
	return nil
}
