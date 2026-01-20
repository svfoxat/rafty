package rafty

import (
	"context"

	proto "github.com/svfoxat/rafty/internal/api"
)

type v1Grpc struct {
	kv *KVStore
}

func (v v1Grpc) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	res, ok := v.kv.Get(request.Key)
	if !ok {
		return &proto.GetResponse{Found: false}, nil
	}

	return &proto.GetResponse{Found: true, Value: string(res)}, nil
}

func (v v1Grpc) Put(ctx context.Context, request *proto.PutRequest) (*proto.PutResponse, error) {
	res, err := v.kv.Set(SetCommand{
		Key:   request.Key,
		Value: request.Value,
		TTL:   0,
	})
	if err != nil {
		return &proto.PutResponse{Success: false}, err
	}

	return &proto.PutResponse{
		Success:  true,
		Id:       int64(res.Index),
		Duration: res.Duration.Milliseconds(),
	}, nil
}

func (v v1Grpc) Delete(ctx context.Context, request *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	err := v.kv.Delete(DeleteCommand{Key: request.Key})
	if err != nil {
		return &proto.DeleteResponse{Success: false}, err
	}

	return &proto.DeleteResponse{Success: true}, nil
}
