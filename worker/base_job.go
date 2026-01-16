package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/opensvc/oc3/cdb"
)

type (
	JobBase struct {
		name   string
		detail string
	}

	operation struct {
		desc string
		do   func(context.Context) error

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

func RunJob(ctx context.Context, j JobRunner) error {
	name := j.Name()
	detail := j.Detail()
	defer logDurationInfo(fmt.Sprintf("%s %s", name, detail), time.Now())
	slog.Info(fmt.Sprintf("%s starting %s", name, detail))

	ops := j.Operations()

	err := runOps(ctx, ops...)
	if err != nil {
		if tx, ok := j.(cdb.DBTxer); ok {
			slog.Debug(fmt.Sprintf("%s rollbacking on error %s", name, detail))
			if err := tx.Rollback(); err != nil {
				slog.Error(fmt.Sprintf("%s rollback on error failed %s: %s", name, detail, err))
			}
		}
		return err
	} else if tx, ok := j.(cdb.DBTxer); ok {
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

func (j *JobBase) Name() string {
	return j.name
}

func (j *JobBase) Detail() string {
	return j.detail
}

func runOps(ctx context.Context, ops ...operation) error {
	for _, op := range ops {
		var err error
		if op.condition != nil && !op.condition() {
			continue
		}
		begin := time.Now()
		err = op.do(ctx)
		duration := time.Since(begin)
		if err != nil {
			operationDuration.
				With(prometheus.Labels{"desc": op.desc, "status": operationStatusFailed}).
				Observe(duration.Seconds())
			if op.blocking {
				return err
			}
			slog.Warn("%s: non blocking error: %s", op.desc, err)
			continue
		}
		operationDuration.
			With(prometheus.Labels{"desc": op.desc, "status": operationStatusOk}).
			Observe(duration.Seconds())
		slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", op.desc, duration))
	}
	return nil
}

func logDurationInfo(s string, begin time.Time) {
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", s, time.Since(begin)))
}
