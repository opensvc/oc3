package apihandlers

import (
	"database/sql"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/api"
)

type (
	Api struct {
		DB    *sql.DB
		Redis *redis.Client
		UI    bool
	}
)

var (
	SCHEMA openapi3.T
)

func init() {
	if schema, err := api.GetSwagger(); err == nil {
		SCHEMA = *schema
	}
}
