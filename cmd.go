package main

import (
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	debug bool
)

func newCmd(args []string) *cobra.Command {
	root := &cobra.Command{
		Use:   filepath.Base(args[0]),
		Short: "Manage the opensvc collector infrastructure components.",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if debug {
				slog.SetLogLoggerLevel(slog.LevelDebug)
			}

			logConfigDir()
			if err := initConfig(); err != nil {
				return err
			}
			logConfigFileUsed()
			return nil
		},
	}

	root.PersistentFlags().BoolVar(&debug, "debug", false, "set log level to debug")

	apiCmd := cobra.Command{
		Use:   "api",
		Short: "serve the collector api",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listen()
		},
	}

	var maxRunners int
	workerCmd := cobra.Command{
		Use:   "worker",
		Short: "run jobs from a list of queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			return work(maxRunners, args)
		},
	}
	workerCmd.Flags().IntVar(&maxRunners, "runners", 1, "maximun number of worker job runners")

	root.AddCommand(
		&apiCmd,
		&workerCmd,
	)
	return root
}
