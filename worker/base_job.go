package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

type (
	JobBase struct {
		name   string
		detail string

		logger *slog.Logger
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
	jlog := j.Logger().With(logkey.JobName, name)
	defer func(begin time.Time) {
		jlog.Debug(fmt.Sprintf("STAT: %s elapse: %s", name, time.Since(begin)))
	}(time.Now())
	jlog.Debug("starting job")

	ops := j.Operations()

	err := runOps(ctx, jlog, ops...)
	if err != nil {
		if tx, ok := j.(cdb.DBTxer); ok {
			jlog.Debug("call rollback on error")
			if err := tx.Rollback(); err != nil {
				jlog.Error("rollback on error failed", logkey.Error, err)
			}
		}
		return err
	} else if tx, ok := j.(cdb.DBTxer); ok {
		jlog.Debug("call commit")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
	if r, ok := j.(LogResulter); ok {
		r.LogResult()
	}
	jlog.Debug("job done")
	return nil
}

func (j *JobBase) Name() string {
	return j.name
}

func (j *JobBase) Detail() string {
	return j.detail
}

func (j *JobBase) Logger() *slog.Logger {
	return j.logger
}

func runOps(ctx context.Context, jlog *slog.Logger, ops ...operation) error {
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
			// TODO: add metrics
			jlog.Warn(fmt.Sprintf("%s: non blocking error", op.desc), logkey.JobOpName, op.desc, logkey.Error, err)
			continue
		}
		operationDuration.
			With(prometheus.Labels{"desc": op.desc, "status": operationStatusOk}).
			Observe(duration.Seconds())
		jlog.Debug(fmt.Sprintf("STAT: %s elapse: %s", op.desc, duration), logkey.JobOpName, op.desc)
	}
	return nil
}
