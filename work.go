package main

import (
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/worker"
)

func work(queues []string) error {
	db, err := newDatabase()
	if err != nil {
		return err
	}
	w := &worker.Worker{
		Redis:  newRedis(),
		DB:     db,
		Queues: queues,
		WithTx: viper.GetBool("feeder.tx"),
	}
	if err != nil {
		return err
	}
	return w.Run()
}
