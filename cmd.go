package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	root = &cobra.Command{
		Use:   filepath.Base(os.Args[0]),
		Short: "Manage the opensvc collector infrastructure components.",
	}
)

func init() {
	root.AddCommand(
		&cobra.Command{
			Use:   "api",
			Short: "serve the collector api",
			RunE: func(cmd *cobra.Command, args []string) error {
				return listen()
			},
		},
	)
}
