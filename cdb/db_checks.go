package cdb

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func (oDb *DB) PurgeChecksOutdated(ctx context.Context, maxAge time.Duration) error {
	request := fmt.Sprintf("DELETE FROM `checks_live` WHERE `chk_updated` < DATE_SUB(NOW(), INTERVAL ? SECOND)")
	if count, err := oDb.execCountContext(ctx, request, maxAgeSeconds(maxAge)); err != nil {
		return fmt.Errorf("delete from checks_live: %w", err)
	} else if count > 0 {
		// TODO: add metrics about purged count
		slog.Debug(fmt.Sprintf("purged %d entries from table checks_live", count))
		oDb.SetChange("checks_live")
	}
	return nil
}
