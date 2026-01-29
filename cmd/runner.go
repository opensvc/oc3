package cmd

import (
	"context"

	"github.com/opensvc/oc3/runner"
)

func startRunner() error {
	if err := setup(); err != nil {
		return err
	}
	db, err := newDatabase()
	if err != nil {
		return err
	}

	ad := &runner.ActionDaemon{
		DB:  db,
		Ev:  newEv(),
		Ctx: context.Background(),
	}

	return ad.Run()
}
