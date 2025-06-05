package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// diskIDFromDiskinfoWithDevIDAndTargetID retrieves a disk ID from the database
// based on device ID, target ID prefix, and exclusion criteria.
// It queries the `diskinfo`, `stor_array`, and `stor_array_tgtid` tables,
// limiting results to a single match.
// Returns the matching disk ID or an empty string if no match is found,
// along with any query execution error.
func (oDb *opensvcDB) diskIDFromDiskinfoWithDevIDAndTargetID(ctx context.Context, devID, targetIDPrefix, excludeDiskID string) (string, error) {
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
	if err := oDb.db.QueryRowContext(ctx, querySelect, devID, excludeDiskID, targetIDPrefix).Scan(&diskID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("QueryRowContext: %w", err)
	}
	return diskID, nil
}

func (oDb *opensvcDB) updateDiskinfoDiskID(ctx context.Context, previousDiskID, newDiskID string) error {
	var queryUpdate = "UPDATE `diskinfo` SET `disk_id` = ? WHERE `disk_id` = ?"
	if _, err := oDb.db.ExecContext(ctx, queryUpdate, newDiskID, previousDiskID); err != nil {
		return fmt.Errorf("update diskinfo disk_id failed: %w", err)
	}
	return nil
}
