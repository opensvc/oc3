package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// updateDiskinfoDiskIDForOpenV updates the disk ID in the diskinfo table for a
// specific WWID if a matching entry exists.
// It is used to update the `diskinfo`.`disk_id` for hds disks.
// Validates the WWID length before processing. Selects and updates the disk_id
// using associated arrays and target IDs.
// Returns an error if no matching disk ID is found, the query fails, or WWID
// is invalid.
func (oDb *opensvcDB) updateDiskinfoDiskIDForOpenV(ctx context.Context, wwid string) error {
	var (
		foundID string

		querySelect = "SELECT `diskinfo`.`disk_id` FROM `diskinfo`" +
			" LEFT JOIN `stor_array` ON (`diskinfo`.`disk_arrayid` = `stor_array`.`array_name`" +
			" LEFT JOIN `stor_array_tgtid` ON (`stor_array`.`id` = `stor_array_tgtid`.`array_id`)" + "" +
			" WHERE (((`diskinfo`.`disk_devid` = ?') AND (`diskinfo`.`disk_id` <> ?))" +
			"        AND (`stor_array_tgtid`.`array_tgtid` LIKE ?))" +
			" GROUP BY `diskinfo`.`disk_id`"

		queryUpdate = "UPDATE `diskinfo` SET `disk_id` = ? WHERE `disk_id` = ?"
	)
	if len(wwid) < 30 {
		return fmt.Errorf("invalid disk id: %s", wwid)
	}
	ldev := strings.ToUpper(wwid[26:28] + ":" + wwid[28:30] + ":" + wwid[30:])
	portnamePrefix := "50" + ldev[2:12] + "%"

	if err := oDb.db.QueryRowContext(ctx, querySelect, ldev, portnamePrefix).Scan(&foundID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("select query failed: %w", err)
	}
	if foundID == "" {
		return fmt.Errorf("select query found empty disk id value")
	}
	if _, err := oDb.db.ExecContext(ctx, queryUpdate, wwid, foundID); err != nil {
		return fmt.Errorf("update diskinfo disk_id failed: %w", err)
	}
	return nil
}
