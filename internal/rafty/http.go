package rafty

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type V1Http struct {
	kv *KVStore
}

type V1HttpSetCommandResponse struct {
	Index    int32         `json:"index"`
	Duration time.Duration `json:"duration"`
}

func (v V1Http) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	slog.Info("http request", "method", request.Method, "path", request.URL.Path)

	switch request.Method {
	case http.MethodPut:
		if request.URL.Path == "/set" {
			params := request.URL.Query()
			key := params.Get("key")
			value := params.Get("value")

			set, err := v.kv.Set(SetCommand{
				Key:   key,
				Value: value,
				TTL:   0,
			})

			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				writer.Write([]byte(err.Error()))
				return
			}

			res := V1HttpSetCommandResponse{
				Index:    set.Index,
				Duration: set.Duration,
			}

			writer.WriteHeader(http.StatusOK)
			data := json.NewEncoder(writer).Encode(res)
			if data != nil {
				writer.Write([]byte(data.Error()))
			} else {
				writer.Write([]byte("OK"))
			}
		}
	case http.MethodGet:
		if request.URL.Path == "/get" {
			params := request.URL.Query()
			key := params.Get("key")

			val, ok := v.kv.Get(key)
			if !ok {
				writer.WriteHeader(http.StatusNotFound)
			}

			writer.WriteHeader(http.StatusOK)
			writer.Write(val)
		}
	}
}
