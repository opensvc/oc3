package cmd

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"
	_ "time/tzdata"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
)

// initDatabase setup database handler.
func newDatabase() (*sql.DB, error) {
	cdb.InitMetrics()

	cfg := mysql.Config{
		User:                 viper.GetString("db.username"),
		Passwd:               viper.GetString("db.password"),
		Net:                  "tcp",
		Addr:                 viper.GetString("db.host") + ":" + viper.GetString("db.port"),
		DBName:               "opensvc",
		AllowNativePasswords: true,
		ParseTime:            true,
		Loc:                  time.Local,
	}
	slog.Info(fmt.Sprintf("db addr=%s", cfg.Addr))
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// expose the connection pool usage as go_sql_*{db_name="opensvc"}
	if err := prometheus.Register(collectors.NewDBStatsCollector(db, cfg.DBName)); err != nil {
		slog.Warn(fmt.Sprintf("db stats metrics: %s", err))
	}
	return db, nil
}
