package kv

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/svfoxat/rafty/internal/api"
	"google.golang.org/grpc"
)

func NewPutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put <key> <value>",
		Short: "put a value in the key-value store.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial("localhost:12346", grpc.WithInsecure())
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			client := rafty.NewKVServiceClient(conn)

			put, err := client.Put(context.Background(), &rafty.PutRequest{
				Key:   args[0],
				Value: args[1],
			})
			if err != nil {
				return err
			}

			if put.Success {
				cmd.Printf("Successfully put key %s with value %s\n in %dns", args[0], args[1], put.Duration)
			}
			return nil
		},
	}

	return cmd
}
