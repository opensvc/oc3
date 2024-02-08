package main

import (
	"path/filepath"

	"github.com/spf13/cobra"
)

func newCmd(args []string) *cobra.Command {
	root := &cobra.Command{
		Use:   filepath.Base(args[0]),
		Short: "Manage the opensvc collector infrastructure components.",
	}
	root.AddCommand(
		&cobra.Command{
			Use:   "api",
			Short: "serve the collector api",
			RunE: func(cmd *cobra.Command, args []string) error {
				return listen()
			},
		},
		&cobra.Command{
			Use:   "worker",
			Short: "run jobs from a list of queues",
			RunE: func(cmd *cobra.Command, args []string) error {
				return runWorker(args)
			},
		},
	)
	return root
}
