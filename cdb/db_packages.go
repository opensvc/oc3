package cdb

import "context"

func (oDb *DB) PurgePackagesOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM packages
		WHERE
		  pkg_updated < DATE_SUB(NOW(), INTERVAL 100 DAY)`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("packages")
	}
	return nil
}

func (oDb *DB) PurgePatchesOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM patches
		WHERE
		  patch_updated < DATE_SUB(NOW(), INTERVAL 100 DAY)`
	if result, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return err
	} else if affected, err := result.RowsAffected(); err != nil {
		return err
	} else if affected > 0 {
		oDb.SetChange("patches")
	}
	return nil
}
