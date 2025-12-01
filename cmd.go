package main

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/opensvc/oc3/util/version"
)

var (
	debug bool
)

func cmdWorker() *cobra.Command {
	var maxRunners int
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "run jobs from a list of queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			return work(maxRunners, args)
		},
	}
	cmd.Flags().IntVar(&maxRunners, "runners", 1, "maximun number of worker job runners")
	return cmd
}

func cmdAPI() *cobra.Command {
	return &cobra.Command{
		Use:   "api",
		Short: "serve the collector api",
		RunE: func(cmd *cobra.Command, args []string) error {
			return listen()
		},
	}
}

func cmdRoot(args []string) *cobra.Command {
	cmd := &cobra.Command{
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
			slog.Info(fmt.Sprintf("oc3 vesion: %s", version.Version()))
			return nil
		},
	}
	cmd.PersistentFlags().BoolVar(&debug, "debug", false, "set log level to debug")
	cmd.AddCommand(
		cmdAPI(),
		cmdWorker(),
	)
	return cmd
}
