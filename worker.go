package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"
)

type (
	Worker struct {
		Redis  *redis.Client
		DB     *sql.DB
		Queues []string
	}
)

func runWorker(queues []string) error {
	w, err := newWorker(queues)
	if err != nil {
		return err
	}
	return w.Run()
}

func newWorker(queues []string) (*Worker, error) {
	logConfig()
	db, err := newDatabase()
	if err != nil {
		return nil, err
	}
	w := &Worker{
		Redis:  newRedis(),
		DB:     db,
		Queues: queues,
	}
	return w, nil
}

func (t *Worker) Run() error {
	slog.Info(fmt.Sprintf("dequeue %s", t.Queues))
	for {
		tasks := t.Redis.BLPop(context.Background(), 5*time.Second, t.Queues...)
		if tasks == nil {
			continue
		}
		result, err := tasks.Result()
		switch err {
		case nil:
		case redis.Nil:
			continue
		default:
			slog.Error(err.Error())
			time.Sleep(time.Second)
			continue
		}
		slog.Info(fmt.Sprintf("BLPOP %s => %s", result[0], result[1]))
	}
	return nil
}
