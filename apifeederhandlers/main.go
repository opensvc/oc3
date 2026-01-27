package apifeederhandlers

import (
	"database/sql"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	redis "github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/apifeeder"
)

type (
	Api struct {
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
	if schema, err := apifeeder.GetSwagger(); err == nil {
		SCHEMA = *schema
	}
}
