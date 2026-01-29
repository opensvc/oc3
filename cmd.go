package main

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/util/version"
)

var (
	debug bool

	GroupIDSubsystems = "subsystems"
)

func NewGroupSubsystems() *cobra.Group {
	return &cobra.Group{
		ID:    GroupIDSubsystems,
		Title: "Subsystems:",
	}
}

func cmdWorker() *cobra.Command {
	var maxRunners int
	cmd := &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "worker",
		Short:   "run queued jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(); err != nil {
				return err
			}
			queues := make([]string, 0, len(args))
			for _, q := range args {
				queues = append(queues, cachekeys.QueuePrefix+q)
			}
			return startWorker(maxRunners, queues)
		},
	}
	cmd.Flags().IntVar(&maxRunners, "runners", 1, "maximun number of worker job runners")
	return cmd
}

func cmdFeeder() *cobra.Command {
	return &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "feeder",
		Short:   "serve the data feed api to nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(); err != nil {
				return err
			}
			return startFeeder()
		},
	}
}

func cmdApiCollector() *cobra.Command {
	return &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "apicollector",
		Short:   "serve the collector api",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(); err != nil {
				return err
			}
			return startApi()
		},
	}
}

func cmdScheduler() *cobra.Command {
	return &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "scheduler",
		Short:   "start running db maintenance tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startScheduler()
		},
	}
}

func cmdRunner() *cobra.Command {
	return &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "runner",
		Short:   "dispatch actions to nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startRunner()
		},
	}
}

func cmdMessenger() *cobra.Command {
	return &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "messenger",
		Short:   "notify clients of data changes",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startMessenger()
		},
	}
}

func cmdSchedulerList() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "list the tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			return scheduleList()
		},
	}
}

func cmdSchedulerExec() *cobra.Command {
	var name string
	cmd := &cobra.Command{
		Use:   "exec",
		Short: "execute a task inline",
		RunE: func(cmd *cobra.Command, args []string) error {
			return scheduleExec(name)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "the task name")
	return cmd
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
		Use:          filepath.Base(args[0]),
		Short:        "Manage the opensvc collector infrastructure components.",
		SilenceUsage: true,
	}
	cmd.AddGroup(NewGroupSubsystems())
	cmd.PersistentFlags().BoolVar(&debug, "debug", false, "set log level to debug")
	grpScheduler := cmdScheduler()
	grpScheduler.AddCommand(
		cmdSchedulerExec(),
		cmdSchedulerList(),
	)
	cmd.AddCommand(
		cmdFeeder(),
		cmdApiCollector(),
		grpScheduler,
		cmdVersion(),
		cmdWorker(),
		cmdRunner(),
		cmdMessenger(),
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
