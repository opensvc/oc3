package worker

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/cache"
)

func (t *Worker) handleDaemonStatus(nodeID string) error {
	ctx := context.Background()
	if err := t.Redis.HDel(ctx, cache.KeyDaemonStatusPending, nodeID).Err(); err != nil {
		return fmt.Errorf("HDEL %s %s: %w", cache.KeyDaemonStatusPending, nodeID, err)
	}
	// TODO
	return nil
}
