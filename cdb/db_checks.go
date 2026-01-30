package cdb

import (
	"context"
	"fmt"
	"log/slog"
)

func (oDb *DB) PurgeChecksOutdated(ctx context.Context) error {
	request := fmt.Sprintf("DELETE FROM `checks_live` WHERE `chk_updated` < DATE_SUB(NOW(), INTERVAL 2 DAY)")
	if count, err := oDb.execCountContext(ctx, request); err != nil {
		return fmt.Errorf("delete from checks_live: %w", err)
	} else if count > 0 {
		slog.Info(fmt.Sprintf("purged %d entries from table checks_live", count))
		oDb.SetChange("checks_live")
	}
	return nil
}
