package kv

import (
	"github.com/spf13/cobra"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kv",
		Short: "Read and write key-value pairs.",
	}

	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewPutCommand())
	cmd.AddCommand(NewDelCommand())
	return cmd
}
