package handlers

import (
	"database/sql"

	"github.com/go-redis/redis/v8"
)

type (
	Api struct {
		DB    *sql.DB
		Redis *redis.Client
	}
)
