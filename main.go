package main

import (
	"os"

	"github.com/opensvc/oc3/cmd"
)

// execute parses the command line and run the selected component.
func execute(args []string) error {
	root := cmd.Root(args)
	return root.Execute()
}

// main is the program entrypoint. It's the only function using os.Exit, so
// keep it simple.
func main() {
	if err := execute(os.Args); err != nil {
		os.Exit(1)
	}
}
