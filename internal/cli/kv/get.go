package kv

import (
	"context"

	"github.com/spf13/cobra"
	rafty "github.com/svfoxat/rafty/internal/api"
	"google.golang.org/grpc"
)

func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get a value from the key-value store.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := grpc.Dial("localhost:12346", grpc.WithInsecure())
			if err != nil {
				panic(err)
			}
			defer conn.Close()

			client := rafty.NewKVServiceClient(conn)

			put, err := client.Get(context.Background(), &rafty.GetRequest{
				Key: args[0],
			})
			if err != nil {
				return err
			}

			cmd.Println(string(put.Value))
			return nil
		},
	}

	return cmd
}
