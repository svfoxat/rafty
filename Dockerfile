# Stage 1: Build
FROM golang:1.24.1-alpine AS builder
# Install git (needed for go mod download) and build tools.
RUN apk update && apk add --no-cache git

# Set the working directory.
WORKDIR /build

# Copy go.mod and go.sum first to leverage Docker cache.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code.
COPY . .

# Build the binary statically (using CGO disabled) for Linux.
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o rafty ./cmd/server

# Stage 2: Final image
FROM alpine
# Copy the binary from the builder.
COPY --from=builder /build/rafty /rafty

# Optionally, expose the port that the node listens on.
EXPOSE 12345

# Set the entrypoint.
ENTRYPOINT ["/rafty"]