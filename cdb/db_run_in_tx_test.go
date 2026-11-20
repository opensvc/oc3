package cdb

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestRetryOnDeadlock checks a transaction is run again when a deadlock
// failed it, and only then: another error is returned at once and as is, for
// the caller to match it.
func TestRetryOnDeadlock(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}
	other := errors.New("refused")

	t.Run("run again after a deadlock", func(t *testing.T) {
		m := testMetrics()
		retries := counterValue(m.ExecTxRetry)
		runs := 0
		err := retryOnDeadlock(context.Background(), 3, m, func() error {
			runs++
			if runs == 1 {
				return deadlock
			}
			return nil
		})
		if err != nil || runs != 2 {
			t.Errorf("err %v after %d runs, want nil after 2", err, runs)
		}
		if got := counterValue(m.ExecTxRetry) - retries; got != 1 {
			t.Errorf("ExecTxRetry grew by %v, want 1", got)
		}
	})

	t.Run("another error is not retried", func(t *testing.T) {
		runs := 0
		err := retryOnDeadlock(context.Background(), 3, testMetrics(), func() error {
			runs++
			return other
		})
		if err != other || runs != 1 {
			t.Errorf("err %v after %d runs, want the error as is after 1", err, runs)
		}
	})

	t.Run("gives up after the attempts", func(t *testing.T) {
		runs := 0
		err := retryOnDeadlock(context.Background(), 2, testMetrics(), func() error {
			runs++
			return deadlock
		})
		if !isDeadlock(err) || runs != 2 {
			t.Errorf("err %v after %d runs, want the deadlock after 2", err, runs)
		}
	})

	t.Run("stops waiting when the context is done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		runs := 0
		err := retryOnDeadlock(ctx, 5, testMetrics(), func() error {
			runs++
			return deadlock
		})
		if !errors.Is(err, context.Canceled) || runs != 1 {
			t.Errorf("err %v after %d runs, want canceled after 1", err, runs)
		}
	})
}
