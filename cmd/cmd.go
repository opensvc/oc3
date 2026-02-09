package cmd

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"

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
	var name string
	var queues []string
	cmd := &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "worker",
		Short:   "run queued jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			setDefaultWorkerConfig(name)
			if err := setup(); err != nil {
				return err
			}
			if t, err := newWorker(name, maxRunners, queues); err != nil {
				return err
			} else {
				return t.run()
			}
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "worker name")
	cmd.Flags().IntVar(&maxRunners, "runners", 1, "maximum number of worker job runners")
	cmd.Flags().StringSliceVar(&queues, "queues", []string{}, "worker queue to listen to")
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
			if t, err := newFeeder(); err != nil {
				return err
			} else {
				return run(t)
			}
		},
	}
}

func cmdApiCollector() *cobra.Command {
	return &cobra.Command{
		GroupID: GroupIDSubsystems,
		Use:     "server",
		Short:   "serve the collector api to clients",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := setup(); err != nil {
				return err
			}
			if t, err := newServer(); err != nil {
				return err
			} else {
				return run(t)
			}
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

func Root(args []string) *cobra.Command {
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
