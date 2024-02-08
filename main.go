package main

import (
	"log/slog"
	"os"
)

// cmd parses the command line and run the selected component.
func cmd(args []string) error {
	root := newCmd(args)

	if err := initConfig(); err != nil {
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
