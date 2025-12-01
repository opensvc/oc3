package main

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/opensvc/oc3/scheduler"
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
			if err := setup(); err != nil {
				return err
			}
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
			if err := setup(); err != nil {
				return err
			}
			return listen()
		},
	}
}

func cmdScheduler() *cobra.Command {
	return &cobra.Command{
		Use:   "scheduler",
		Short: "start running db maintenance tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(); err != nil {
				return err
			}
			db, err := newDatabase()
			if err != nil {
				return err
			}
			sched := &scheduler.Scheduler{
				DB: db,
			}
			return sched.Run()
		},
	}
}

func cmdVersion() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "display the oc3 version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version.Version())
		},
	}
	return cmd
}

func cmdRoot(args []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   filepath.Base(args[0]),
		Short: "Manage the opensvc collector infrastructure components.",
	}
	cmd.PersistentFlags().BoolVar(&debug, "debug", false, "set log level to debug")
	cmd.AddCommand(
		cmdAPI(),
		cmdScheduler(),
		cmdVersion(),
		cmdWorker(),
	)
	return cmd
}

func setup() error {
	if debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	logConfigDir()
	if err := initConfig(); err != nil {
		return err
	}
	logConfigFileUsed()
	slog.Info(fmt.Sprintf("oc3 version: %s", version.Version()))
	return nil
}
