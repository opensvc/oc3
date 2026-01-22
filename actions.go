package main

import (
	"context"

	"github.com/opensvc/oc3/actiond"
)

func actions() error {
	if err := setup(); err != nil {
		return err
	}
	db, err := newDatabase()
	if err != nil {
		return err
	}

	ad := &actiond.ActionDaemon{
		DB:  db,
		Ev:  newEv(),
		Ctx: context.Background(),
	}

	return ad.Run()
}
