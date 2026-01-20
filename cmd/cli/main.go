package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/svfoxat/rafty/internal/cli/kv"
)

var rootCmd = &cobra.Command{
	Use:   "rafty [command]",
	Short: "Rafty CLI - a command-line interface for Rafty",
}

func init() {
	rootCmd.AddCommand(kv.NewCommand())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
