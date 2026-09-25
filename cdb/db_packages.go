package cdb

import (
	"context"
	"time"
)

func (oDb *DB) PurgePackagesOutdated(ctx context.Context, maxAge time.Duration) error {
	var query = `DELETE
		FROM packages
		WHERE
		  pkg_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`
	if count, err := oDb.execCountContext(ctx, query, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("packages")
	}
	return nil
}

func (oDb *DB) PurgePatchesOutdated(ctx context.Context, maxAge time.Duration) error {
	var query = `DELETE
		FROM patches
		WHERE
		  patch_updated < DATE_SUB(NOW(), INTERVAL ? SECOND)`
	if count, err := oDb.execCountContext(ctx, query, maxAgeSeconds(maxAge)); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("patches")
	}
	return nil
}
