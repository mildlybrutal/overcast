ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS builder
WORKDIR /app/services/ingestion

COPY gen/go.mod /app/gen/go.mod
COPY internal/models/go.mod /app/internal/models/go.mod
COPY services/ingestion/go.mod services/ingestion/go.sum ./
RUN go mod download

COPY gen/ /app/gen/
COPY internal/ /app/internal/
COPY services/ingestion/cmd/ ./cmd/
COPY services/ingestion/internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/ingestion cmd/main.go

FROM alpine:3.22
RUN adduser -D -g '' appuser
WORKDIR /app
COPY --from=builder /app/ingestion .
USER appuser

CMD ["./ingestion"]
