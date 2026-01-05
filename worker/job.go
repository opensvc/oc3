package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/cdb"
)

type (
	// BaseJob is a base struct to compose jobs.
	BaseJob struct {
		ctx   context.Context
		redis *redis.Client
		db    cdb.DBOperater
		oDb   *cdb.DB
		ev    EventPublisher

		name   string
		detail string
		now    time.Time

		// cachePendingH is the cache hash used by BaseJob.dropPending:
		// HDEL <cachePendingH> <cachePendingIDX>
		cachePendingH string

		// cachePendingIDX is the cache id used by BaseJob.dropPending:
		// HDEL <cachePendingH> <cachePendingIDX>
		cachePendingIDX string
	}

	operation struct {
		desc string
		do   func() error

		// blocking stops the operation chain on operation error
		blocking bool

		// condition skips operation if condition returns false
		condition func() bool
	}

	LogResulter interface {
		LogResult()
	}

	Operationer interface {
		Operations() []operation
	}

	DBGetter interface {
		DB() cdb.DBOperater
	}
)

func RunJob(j JobRunner) error {
	name := j.Name()
	detail := j.Detail()
	defer logDurationInfo(fmt.Sprintf("%s %s", name, detail), time.Now())
	slog.Info(fmt.Sprintf("%s starting %s", name, detail))

	ops := j.Operations()

	err := runOps(ops...)
	if err != nil {
		if tx, ok := j.DB().(cdb.DBTxer); ok {
			slog.Debug(fmt.Sprintf("%s rollbacking on error %s", name, detail))
			if err := tx.Rollback(); err != nil {
				slog.Error(fmt.Sprintf("%s rollback on error failed %s: %s", name, detail, err))
			}
		}
		return err
	} else if tx, ok := j.DB().(cdb.DBTxer); ok {
		slog.Debug(fmt.Sprintf("%s commiting %s", name, detail))
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
	if r, ok := j.(LogResulter); ok {
		r.LogResult()
	}
	slog.Info(fmt.Sprintf("%s done %s", name, detail))
	return nil
}

func (j *BaseJob) PrepareDB(ctx context.Context, db *sql.DB, ev EventPublisher, withTx bool) error {
	switch withTx {
	case true:
		if tx, err := db.BeginTx(ctx, nil); err != nil {
			return err
		} else {
			j.db = tx
			j.oDb = &cdb.DB{DB: tx, Session: cdb.NewSession(tx, ev)}
		}
	case false:
		j.db = db
		j.oDb = &cdb.DB{DB: db, Session: cdb.NewSession(db, ev)}
	}
	j.oDb.DBLck = cdb.InitDbLocker(db)
	j.ctx = ctx
	j.ev = ev
	return nil
}

func (j *BaseJob) DB() cdb.DBOperater {
	return j.db
}

func (j *BaseJob) SetRedis(r *redis.Client) {
	j.redis = r
}

func (j *BaseJob) SetEv(ev EventPublisher) {
	j.ev = ev
}

func (j *BaseJob) Name() string {
	return j.name
}

func (j *BaseJob) Detail() string {
	return j.detail
}

func (j *BaseJob) dbNow() (err error) {
	rows, err := j.db.QueryContext(j.ctx, "SELECT NOW()")
	if err != nil {
		return err
	}
	if rows == nil {
		return fmt.Errorf("no result rows for SELECT NOW()")
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("no result rows next for SELECT NOW()")
	}
	if err := rows.Scan(&j.now); err != nil {
		return err
	}
	return nil
}

func runOps(ops ...operation) error {
	for _, op := range ops {
		if op.condition != nil && !op.condition() {
			continue
		}
		begin := time.Now()
		err := op.do()
		duration := time.Since(begin)
		if err != nil {
			operationDuration.
				With(prometheus.Labels{"desc": op.desc, "status": operationStatusFailed}).
				Observe(duration.Seconds())
			if op.blocking {
				continue
			}
			return err
		}
		operationDuration.
			With(prometheus.Labels{"desc": op.desc, "status": operationStatusOk}).
			Observe(duration.Seconds())
		slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", op.desc, duration))
	}
	return nil
}

func (j *BaseJob) dropPending() error {
	if err := j.redis.HDel(j.ctx, j.cachePendingH, j.cachePendingIDX).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", j.cachePendingH, j.cachePendingIDX, err)
	}
	return nil
}

func (d *BaseJob) pushFromTableChanges() error {
	return d.oDb.Session.NotifyChanges(d.ctx)
}

// populateFeedObjectConfigForClusterIDH HSET FeedObjectConfigForClusterIDH <clusterID> with the names of objects
// without config or HDEL FeedObjectConfigForClusterIDH <clusterID> if there are no missing configs.
func (d *BaseJob) populateFeedObjectConfigForClusterIDH(clusterID string, byObjectID map[string]*cdb.DBObject) ([]string, error) {
	needConfig := make(map[string]struct{})
	for _, obj := range byObjectID {
		if obj.NullConfig {
			objName := obj.Svcname
			// TODO: import om3 naming ?
			if strings.Contains(objName, "/svc/") ||
				strings.Contains(objName, "/vol/") ||
				strings.HasPrefix(objName, "svc/") ||
				strings.HasPrefix(objName, "vol/") ||
				!strings.Contains(objName, "/") {
				needConfig[objName] = struct{}{}
			}
		}
	}

	keyName := cachekeys.FeedObjectConfigForClusterIDH

	if len(needConfig) > 0 {
		l := make([]string, 0, len(needConfig))
		for k := range needConfig {
			l = append(l, k)
		}
		if err := d.redis.HSet(d.ctx, keyName, clusterID, strings.Join(l, " ")).Err(); err != nil {
			return l, fmt.Errorf("populateFeedObjectConfigForClusterIDH: HSet %s %s: %w", keyName, clusterID, err)
		}
		return l, nil
	} else {
		if err := d.redis.HDel(d.ctx, keyName, clusterID).Err(); err != nil {
			return nil, fmt.Errorf("populateFeedObjectConfigForClusterIDH: HDEL %s %s: %w", keyName, clusterID, err)
		}
	}
	return nil, nil
}
