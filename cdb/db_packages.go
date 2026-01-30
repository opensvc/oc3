package cdb

import "context"

func (oDb *DB) PurgePackagesOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM packages
		WHERE
		  pkg_updated < DATE_SUB(NOW(), INTERVAL 100 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("packages")
	}
	return nil
}

func (oDb *DB) PurgePatchesOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM patches
		WHERE
		  patch_updated < DATE_SUB(NOW(), INTERVAL 100 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("patches")
	}
	return nil
}
