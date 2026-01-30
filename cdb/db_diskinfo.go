package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type (
	DBDiskinfo struct {
		DiskID  string
		ArrayID string
	}
)

// DiskIDFromDiskinfoWithDevIDAndTargetID retrieves a disk ID from the database
// based on device ID, target ID prefix, and exclusion criteria.
// It queries the `diskinfo`, `stor_array`, and `stor_array_tgtid` tables,
// limiting results to a single match.
// Returns the matching disk ID or an empty string if no match is found,
// along with any query execution error.
func (oDb *DB) DiskIDFromDiskinfoWithDevIDAndTargetID(ctx context.Context, devID, targetIDPrefix, excludeDiskID string) (string, error) {
	var (
		querySelect = "SELECT `diskinfo`.`disk_id` FROM `diskinfo`" +
			" LEFT JOIN `stor_array` ON (`diskinfo`.`disk_arrayid` = `stor_array`.`array_name`" +
			" LEFT JOIN `stor_array_tgtid` ON (`stor_array`.`id` = `stor_array_tgtid`.`array_id`)" + "" +
			" WHERE (((`diskinfo`.`disk_devid` = ?') AND (`diskinfo`.`disk_id` <> ?))" +
			"        AND (`stor_array_tgtid`.`array_tgtid` LIKE ?))" +
			" GROUP BY `diskinfo`.`disk_id`" +
			" LIMIT 1"

		diskID string
	)
	if err := oDb.DB.QueryRowContext(ctx, querySelect, devID, excludeDiskID, targetIDPrefix).Scan(&diskID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("QueryRowContext: %w", err)
	}
	return diskID, nil
}

func (oDb *DB) UpdateDiskinfoDiskID(ctx context.Context, previousDiskID, newDiskID string) error {
	var queryUpdate = "UPDATE `diskinfo` SET `disk_id` = ? WHERE `disk_id` = ?"
	if _, err := oDb.DB.ExecContext(ctx, queryUpdate, newDiskID, previousDiskID); err != nil {
		return fmt.Errorf("update diskinfo disk_id failed: %w", err)
	}
	return nil
}

func (oDb *DB) DiskinfoByDiskID(ctx context.Context, diskID string) ([]DBDiskinfo, error) {
	var query = "SELECT `disk_id`, `disk_arrayid` FROM `diskinfo` WHERE `disk_id` = ?"
	if rows, err := oDb.DB.QueryContext(ctx, query, diskID); err != nil {
		return nil, fmt.Errorf("diskinfoByDiskID: %w", err)
	} else {
		defer func() { _ = rows.Close() }()
		diskL := make([]DBDiskinfo, 0)
		for rows.Next() {
			var (
				diskinfo DBDiskinfo
				arrayID  sql.NullString
			)

			if err := rows.Scan(&diskinfo.DiskID, &arrayID); err != nil {
				return nil, fmt.Errorf("scan diskinfo: %w", err)
			}
			diskinfo.ArrayID = arrayID.String
			diskL = append(diskL, diskinfo)
		}
		return diskL, nil
	}
}

func (oDb *DB) UpdateDiskinfoArrayID(ctx context.Context, diskID, arrayID string) (bool, error) {
	var (
		query = "INSERT INTO `diskinfo` (`disk_id`, `disk_arrayid`, `disk_updated`)" +
			" VALUES (?, ?, NOW())" +
			" ON DUPLICATE KEY UPDATE `disk_arrayid` = VALUES(`disk_arrayid`), `disk_updated` = VALUES(disk_updated)"
	)
	if count, err := oDb.execCountContext(ctx, query, diskID, arrayID); err != nil {
		return false, fmt.Errorf("update diskinfo: %w", err)
	} else {
		return count > 0, nil
	}
}

func (oDb *DB) UpdateDiskinfoArrayAndDevIDsAndSize(ctx context.Context, diskID, arrayID, devID string, size int32) (bool, error) {
	var (
		query = "INSERT INTO `diskinfo` (`disk_id`, `disk_arrayid`, `disk_devid`, `disk_size`, `disk_updated`)" +
			" VALUES (?, ?, ?, ?, NOW())" +
			" ON DUPLICATE KEY UPDATE `disk_arrayid` = VALUES(`disk_arrayid`), `disk_devid` = VALUES(`disk_devid`), `disk_size` = VALUES(`disk_size`), `disk_updated` = VALUES(disk_updated)"
	)
	if count, err := oDb.execCountContext(ctx, query, diskID, arrayID, devID, size); err != nil {
		return false, fmt.Errorf("update diskinfo: %w", err)
	} else {
		return count > 0, nil
	}
}

func (oDb *DB) UpdateDiskinfoForDiskSize(ctx context.Context, diskID string, size int32) (bool, error) {
	var (
		query = "INSERT INTO `diskinfo` (`disk_id`, `disk_size`, `disk_updated`)" +
			" VALUES (?, ?, NOW())" +
			" ON DUPLICATE KEY UPDATE `disk_size` = VALUES(`disk_size`), `disk_updated` = VALUES(disk_updated)"
	)
	if count, err := oDb.execCountContext(ctx, query, diskID, size); err != nil {
		return false, fmt.Errorf("update diskinfo: %w", err)
	} else {
		return count > 0, nil
	}
}

func (oDb *DB) UpdateDiskinfoSetMissingArrayID(ctx context.Context, diskID, arrayID string) (bool, error) {
	var (
		query = "UPDATE `diskinfo` SET `disk_arrayid` = ?" +
			" WHERE `disk_id` = ? AND (`disk_arrayid` = '' OR `disk_arrayid` is NULL)"
	)
	if count, err := oDb.execCountContext(ctx, query, arrayID, diskID); err != nil {
		return false, fmt.Errorf("update diskinfo: %w", err)
	} else {
		return count > 0, nil
	}
}

func (oDb *DB) PurgeDiskinfoOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM diskinfo
		WHERE
		  disk_updated < DATE_SUB(NOW(), INTERVAL 2 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return fmt.Errorf("purge diskinfo: %w", err)
	} else if count > 0 {
		oDb.SetChange("diskinfo")
	}
	return nil
}
