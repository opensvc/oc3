package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/opensvc/oc3/cache"
)

type (
	daemonPing struct {
		ctx   context.Context
		redis *redis.Client
		db    DBOperater
		oDb   *opensvcDB
		ev    EventPublisher

		nodeID      string
		clusterID   string
		clusterName string
		nodeApp     string
		nodeEnv     string
		callerNode  *DBNode

		changes map[string]struct{}

		byInstanceName map[string]*DBInstance
		byInstanceID   map[string]*DBInstance
	}
)

func (t *Worker) handleDaemonPing(nodeID string) error {
	defer logDurationInfo(fmt.Sprintf("handleDaemonPing %s with tx %v", nodeID, t.WithTx), time.Now())
	slog.Info(fmt.Sprintf("handleDaemonPing starting for node_id %s", nodeID))
	ctx := context.Background()

	d := daemonPing{
		ctx:    ctx,
		redis:  t.Redis,
		nodeID: nodeID,
		ev:     t.Ev,
	}

	switch t.WithTx {
	case true:
		if tx, err := t.DB.BeginTx(ctx, nil); err != nil {
			return err
		} else {
			d.db = tx
			d.oDb = &opensvcDB{db: tx, tChanges: make(map[string]struct{})}
		}
	case false:
		d.db = t.DB
		d.oDb = &opensvcDB{db: t.DB, tChanges: make(map[string]struct{})}
	}

	type operation struct {
		desc string
		do   func() error
	}

	chain := func(ops ...operation) error {
		for _, op := range ops {
			begin := time.Now()
			err := op.do()
			duration := time.Now().Sub(begin)
			if err != nil {
				operationDuration.
					With(prometheus.Labels{"desc": op.desc, "status": operationStatusFailed}).
					Observe(duration.Seconds())
				return err
			}
			operationDuration.
				With(prometheus.Labels{"desc": op.desc, "status": operationStatusOk}).
				Observe(duration.Seconds())
			slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", op.desc, duration))
		}
		return nil
	}

	err := chain([]operation{
		{"daemonPing/dropPending", d.dropPending},
		{"daemonPing/pushFromTableChanges", d.pushFromTableChanges},
	}...)
	if err != nil {
		if tx, ok := d.db.(DBTxer); ok {
			slog.Debug("handleDaemonPing rollback on error")
			if err := tx.Rollback(); err != nil {
				slog.Error(fmt.Sprintf("handleDaemonPing rollback failed: %s", err))
			}
		}
		return fmt.Errorf("handleDaemonPing node_id %s cluster_id %s: %w", nodeID, d.clusterID, err)
	} else if tx, ok := d.db.(DBTxer); ok {
		slog.Debug("handleDaemonPing commit")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("handleDaemonPing commit: %w", err)
		}
	}
	slog.Info(fmt.Sprintf("handleDaemonPing done for %s", d.nodeID))
	return nil
}

func (d *daemonPing) dropPending() error {
	if err := d.redis.HDel(d.ctx, cache.KeyDaemonPingPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", cache.KeyDaemonStatusPending, d.nodeID, err)
	}
	return nil
}

func (d *daemonPing) pushFromTableChanges() error {
	return pushFromTableChanges(d.ctx, d.oDb, d.ev)
}
