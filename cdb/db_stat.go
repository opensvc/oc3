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

func (oDb *DB) StatDaySvcActionsByLevel(ctx context.Context, lvl string) error {
	var query = fmt.Sprintf(`INSERT IGNORE INTO stat_day_svc (svc_id, day, nb_action_%s)
             SELECT
               a.svc_id,
               DATE(NOW()),
               COUNT(a.id)
             FROM
               svcactions a
             WHERE
               a.begin >= CURDATE()
               AND a.begin < DATE_ADD(CURDATE(), INTERVAL 1 DAY)
               AND a.status = "%s"
             GROUP BY
               a.svc_id
             ON DUPLICATE KEY UPDATE
               nb_action_%s = VALUES(nb_action_%s)`, lvl, lvl, lvl, lvl)
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_svc failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatDaySvcActions(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_svc (svc_id, day, nb_action)
             SELECT
               a.svc_id,
               DATE(NOW()),
               COUNT(a.id)
             FROM
               svcactions a
             WHERE
               a.begin >= CURDATE()
               AND a.begin < DATE_ADD(CURDATE(), INTERVAL 1 DAY)
             GROUP BY
               a.svc_id
             ON DUPLICATE KEY UPDATE
               nb_action = VALUES(nb_action)`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_svc failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatDaySvcDiskSize(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_svc (svc_id, day, disk_size)
             SELECT
               d.svc_id,
               DATE(NOW()),
	       IF(SUM(d.disk_size) IS NULL, 0, SUM(d.disk_size))
             FROM
	       svcdisks d
             WHERE
	       d.disk_local='F'
             GROUP BY
	       d.svc_id
             ON DUPLICATE KEY UPDATE
	       disk_size=VALUES(disk_size)`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_svc failed: %w", err)
	}
	return nil
}

func (oDb *DB) StatDaySvcLocalDiskSize(ctx context.Context) error {
	var query = `INSERT IGNORE INTO stat_day_svc (svc_id, day, local_disk_size)
             SELECT
               d.svc_id,
               DATE(NOW()),
	       IF(SUM(d.disk_size) IS NULL, 0, SUM(d.disk_size))
             FROM
	       svcdisks d
             WHERE
	       d.disk_local='T'
             GROUP BY
	       d.svc_id
             ON DUPLICATE KEY UPDATE
	       local_disk_size=VALUES(local_disk_size)`
	if _, err := oDb.DB.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("update stat_day_svc failed: %w", err)
	}
	return nil
}
