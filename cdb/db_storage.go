package cdb

import (
	"context"
)

func (oDb *DB) PurgeStorArrayOutdated(ctx context.Context) error {
	var query = `DELETE FROM stor_array
		WHERE
		  array_model LIKE "vdisk%" AND
		  array_updated < DATE_SUB(NOW(), INTERVAL 2 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("stor_array")
	}
	return nil
}

func (oDb *DB) UpdateStorArrayDGQuota(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stor_array_dg_quota
             SELECT NULL, dg.id, sd.app_id, NULL
             FROM
               svcdisks sd
               join diskinfo di on sd.disk_id=di.disk_id
               join stor_array ar on (di.disk_arrayid=ar.array_name)
               join stor_array_dg dg on (di.disk_group=dg.dg_name and dg.array_id=ar.id)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("stor_array")
	}
	return nil
}
