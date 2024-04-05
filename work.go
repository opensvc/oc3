package main

import (
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/oc2websocket"
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
		Ev: &oc2websocket.T{
			Url: viper.GetString("websocket.url"),
			Key: []byte(viper.GetString("websocket.key")),
		},
	}
	if err != nil {
		return err
	}
	return w.Run()
}
