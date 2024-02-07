package main

import (
	"fmt"
	"log/slog"
	"os"
)

func start() error {
	if err := initConfig(); err != nil {
		return fmt.Errorf("can't load config: %w", err)
	}
	if err := initDatabase(); err != nil {
		return fmt.Errorf("init database: %w", err)
	}
	if err := initRedis(); err != nil {
		return fmt.Errorf("init redis: %w", err)
	}
	if err := initListener(); err != nil {
		return fmt.Errorf("init listener: %w", err)
	}
	return nil
}

func main() {
	if err := start(); err != nil {
		slog.Info(err.Error())
		os.Exit(1)
	}
}
