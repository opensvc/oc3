package handlers

import "database/sql"

type (
	Api struct {
		DB *sql.DB
	}
)

func New(db *sql.DB) *Api {
	return &Api{DB: db}
}
