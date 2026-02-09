package cmd

import (
	"database/sql"
	"fmt"
	"log/slog"
	_ "net/http/pprof"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/worker"
	"github.com/opensvc/oc3/xauth"
)

type (
	workerT struct {
		db      *sql.DB
		redis   *redis.Client
		section string
		runners int
		queues  []string
	}
)

func newWorker(name string, runners int, queues []string) (*workerT, error) {
	if db, err := newDatabase(); err != nil {
		return nil, err
	} else if strings.Contains(name, ".") {
		return nil, fmt.Errorf("unexpected worker name with '.': %s", name)
	} else {
		section := workerSection(name)
		t := &workerT{db: db, section: section, runners: runners, redis: newRedis()}
		if runners < viper.GetInt(t.section+".runners") {
			t.runners = viper.GetInt(t.section + ".runners")
		}
		if len(queues) == 0 {
			queues = viper.GetStringSlice(t.section + ".queues")
		}
		for _, q := range queues {
			t.queues = append(t.queues, cachekeys.QueuePrefix+q)
		}
		return t, nil
	}
}

func (t *workerT) Section() string { return t.section }

func (t *workerT) authMiddleware(publicPath, publicPrefix []string) echo.MiddlewareFunc {
	return AuthMiddleware(union.New(
		xauth.NewPublicStrategy(publicPath, publicPrefix),
	))
}

func (t *workerT) run() error {
	if len(t.queues) == 0 {
		return fmt.Errorf("no queues specified")
	}

	if ok, errC := start(t); ok {
		slog.Info(fmt.Sprintf("%s started", t.Section()))
		go func() {
			if err := <-errC; err != nil {
				slog.Error(fmt.Sprintf("%s stopped: %s", t.Section(), err))
			}
		}()
	}

	w := &worker.Worker{
		DB:      t.db,
		Redis:   t.redis,
		Queues:  t.queues,
		WithTx:  viper.GetBool(t.section + ".tx"),
		Ev:      newEv(),
		Runners: t.runners,
	}
	return w.Run()
}
