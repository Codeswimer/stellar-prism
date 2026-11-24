package main

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

// newDoctorCmd constructs the doctor command from package main.
func newDoctorCmd(stdout, stderr io.Writer) *cobra.Command {
	return cli.NewDoctorCommand(stdout, stderr)
}
