package feederhandlers

import (
	"database/sql"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/feeder"
)

type (
	Feeder struct {
		DB    *sql.DB
		Redis *redis.Client
		UI    bool

		// SyncTimeout is the timeout for synchronous api calls
		SyncTimeout time.Duration
	}
)

var (
	SCHEMA openapi3.T
)

func init() {
	if schema, err := feeder.GetSwagger(); err == nil {
		SCHEMA = *schema
	}
}
