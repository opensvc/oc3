package serverhandlers

import (
	"database/sql"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/xauth"
)

type (
	Api struct {
		DB    *sql.DB
		Redis *redis.Client
		UI    bool
		ODB   *cdb.DB

		// SyncTimeout is the timeout for synchronous api calls
		SyncTimeout time.Duration

		Ev interface {
			EventPublish(eventName string, data map[string]any) error
		}

		// Realtime registers the one-time tokens of the websocket clients with
		// the messenger; nil when no messenger is configured.
		Realtime interface {
			RegisterToken(token string) error
		}

		SubSystem string

		// OIDC is the OpenID Connect sign-in and its sessions; nil when it is not
		// enabled.
		OIDC *xauth.OIDC

		// BasicUsers tells whether users may still sign in with their collector
		// password (HTTP Basic), for the sign-in screen.
		BasicUsers bool
	}
)

var (
	SCHEMA openapi3.T
)

func init() {
	if schema, err := server.GetSwagger(); err == nil {
		SCHEMA = *schema
	}
}
