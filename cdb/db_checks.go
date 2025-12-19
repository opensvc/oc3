package cdb

import (
	"context"
	"fmt"
	"log/slog"
)

func (oDb *DB) PurgeChecksOutdated(ctx context.Context) error {
	request := fmt.Sprintf("DELETE FROM `checks_live` WHERE `chk_updated` < DATE_SUB(NOW(), INTERVAL 2 DAY)")
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return fmt.Errorf("delete from checks_live: %w", err)
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count delete from checks_live: %w", err)
	} else if rowAffected > 0 {
		slog.Info(fmt.Sprintf("purged %d entries from table checks_live", rowAffected))
		oDb.SetChange("checks_live")
	}
	return nil
}
