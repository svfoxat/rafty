.PHONY: build
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/server cmd/server/main.go
	go build -o bin/cli cmd/cli/main.go

.PHONY: generate
generate:
	go generate ./internal/...
	mkdir -p internal/api
	export PATH=$(PATH):$(shell go env GOPATH)/bin && \
	protoc -I api/proto \
		--go_out=. --go_opt=module=github.com/svfoxat/rafty \
		--go-grpc_out=. --go-grpc_opt=module=github.com/svfoxat/rafty,require_unimplemented_servers=false \
		api/proto/*.proto


.PHONY: tools
tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest