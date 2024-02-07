package main

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

// initDatabase setup database handler.
func newDatabase() (*sql.DB, error) {
	cfg := mysql.Config{
		User:                 viper.GetString("db.username"),
		Passwd:               viper.GetString("db.password"),
		Net:                  "tcp",
		Addr:                 viper.GetString("db.host") + ":" + viper.GetString("db.port"),
		DBName:               "opensvc",
		AllowNativePasswords: true,
	}
	slog.Info("db config.addr=" + cfg.Addr)
	DB, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	DB.SetConnMaxLifetime(time.Minute * 3)
	DB.SetMaxOpenConns(10)
	DB.SetMaxIdleConns(10)
	return DB, nil
}
