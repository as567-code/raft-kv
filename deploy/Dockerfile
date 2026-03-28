FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /raft-kv-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /raft-kv-client ./cmd/client

FROM alpine:3.21

RUN apk add --no-cache ca-certificates iproute2 iptables

COPY --from=builder /raft-kv-server /usr/local/bin/raft-kv-server
COPY --from=builder /raft-kv-client /usr/local/bin/raft-kv-client

EXPOSE 50051 50061 9090

ENTRYPOINT ["raft-kv-server"]
