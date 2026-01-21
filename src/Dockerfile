# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /kvnode ./cmd/kvnode

# Runtime stage
FROM alpine:3.19

# Install CA certificates for HTTPS
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /kvnode /usr/local/bin/kvnode

# Create non-root user
RUN adduser -D -u 1000 kvnode
USER kvnode

# Expose ports
# 4000: libp2p
# 8080: REST API
EXPOSE 4000 8080

ENTRYPOINT ["/usr/local/bin/kvnode"]
