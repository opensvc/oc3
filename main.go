package main

import (
	"os"
)

// cmd parses the command line and run the selected component.
func cmd(args []string) error {
	root := newCmd(args)
	return root.Execute()
}

// main is the program entrypoint. It's the only function using os.Exit, so
// keep it simple.
func main() {
	if err := cmd(os.Args); err != nil {
		os.Exit(1)
	}
}
