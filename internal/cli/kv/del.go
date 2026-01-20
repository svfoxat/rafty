package kv

import (
	"context"

	"github.com/spf13/cobra"
	rafty "github.com/svfoxat/rafty/internal/api"
	"google.golang.org/grpc"
)

func NewDelCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "del",
		Short: "Delete a value from the key-value store.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial("localhost:12346", grpc.WithInsecure())
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			client := rafty.NewKVServiceClient(conn)

			del, err := client.Delete(context.Background(), &rafty.DeleteRequest{
				Key: args[0],
			})
			if err != nil {
				return err
			}

			if del.Success {
				cmd.Printf("deleted key %s", args[0])
			}
			return nil
		},
	}

	return cmd
}
