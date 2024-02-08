package main

import (
	"log/slog"
	"os"

	"github.com/spf13/viper"
)

// cmd parses the command line and run the selected component.
func cmd(args []string) error {
	if err := initConfig(); err != nil {
		return err
	}
	if err := viper.BindPFlags(root.PersistentFlags()); err != nil {
		return err
	}
	return root.Execute()
}

// main is the program entrypoint. It's the only function using os.Exit, so
// keep it simple.
func main() {
	if err := cmd(os.Args); err != nil {
		slog.Info(err.Error())
		os.Exit(1)
	}
}
