package cdb

import (
	"context"
	"fmt"
)

func (oDb *DB) StatDayDiskApp(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_disk_app
             SELECT
               NULL,
               NOW(),
               app,
               SUM(quota_used) AS quota_used,
               SUM(quota) AS quota
             FROM v_disk_quota
             GROUP by app`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_disk_app failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatDayDiskAppDG(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_disk_app_dg
             SELECT
               NULL,
               NOW(),
               dg.id,
               ap.app,
               sd.disk_used,
               dgq.quota
             FROM diskinfo di
             JOIN svcdisks sd ON di.disk_id=sd.disk_id
             JOIN apps ap ON ap.id=sd.app_id
             JOIN stor_array ar ON ar.array_name=di.disk_arrayid
             JOIN stor_array_dg dg ON ar.id=dg.array_id AND dg.dg_name=di.disk_group
             LEFT JOIN stor_array_dg_quota dgq ON ap.id=dgq.app_id AND dg.id=dgq.dg_id`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_disk_app_dg failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatDayDiskArray(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_disk_array
             SELECT
               NULL,
               NOW(),
               t.array_name,
               SUM(t.dg_used),
               SUM(t.dg_size),
               SUM(t.dg_reserved),
               SUM(t.dg_reservable)
             FROM (
               SELECT
                 array_name,
                 dg_used,
                 dg_size,
                 dg_reserved,
                 dg_reservable
               FROM
                 v_disk_quota
               GROUP BY
                 array_name, dg_name
             ) t
             GROUP BY
               t.array_name`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_disk_array failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatDayDiskArrayDG(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_disk_array_dg
             SELECT
               NULL,
               NOW(),
               array_name,
               dg_name,
               dg_used,
               dg_size,
               dg_reserved,
               dg_reservable
             FROM
               v_disk_quota
             GROUP BY
               array_name, dg_name`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_disk_array_dg failed: %w", err)
	}
	return nil
}
