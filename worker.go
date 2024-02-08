package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cache"
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

func (t *Worker) Asset(nodeId string) error {
	cmd := t.Redis.HGet(context.Background(), cache.KeyAssetHash, nodeId)
	result, err := cmd.Result()
	switch err {
	case nil:
	case redis.Nil:
		return nil
	default:
		return err
	}
	var v any
	if err := json.Unmarshal([]byte(result), &v); err != nil {
		return err
	}
	return nil
}

func (t *Worker) Run() error {
	slog.Info(fmt.Sprintf("dequeue %s", t.Queues))
	for {
		cmd := t.Redis.BLPop(context.Background(), 5*time.Second, t.Queues...)
		result, err := cmd.Result()
		switch err {
		case nil:
		case redis.Nil:
			continue
		default:
			slog.Error(err.Error())
			time.Sleep(time.Second)
			continue
		}
		slog.Info(fmt.Sprintf("BLPOP %s -> %s", result[0], result[1]))
		switch result[0] {
		case cache.KeyAsset:
			err = t.Asset(result[1])
		default:
			slog.Warn(fmt.Sprintf("unsupported queue: %s", result[0]))
		}
		if err != nil {
			slog.Error(err.Error())
		}
	}
	return nil
}
