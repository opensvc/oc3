package serverhandlers

import (
	"database/sql"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/go-redis/redis/v8"
	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

type (
	Api struct {
		DB    *sql.DB
		Redis *redis.Client
		UI    bool

		// SyncTimeout is the timeout for synchronous api calls
		SyncTimeout time.Duration

		Ev interface {
			EventPublish(eventName string, data map[string]any) error
		}
	}
)

var (
	SCHEMA openapi3.T
)

func (a *Api) cdbSession() *cdb.DB {
	odb := cdb.New(a.DB)
	odb.CreateSession(a.Ev)
	return odb
}

func init() {
	if schema, err := server.GetSwagger(); err == nil {
		SCHEMA = *schema
	}
}
