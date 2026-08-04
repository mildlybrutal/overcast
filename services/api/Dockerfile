ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS builder
WORKDIR /app/services/api

COPY services/api/go.mod services/api/go.sum ./
RUN go mod download

COPY services/api/cmd/ ./cmd/
COPY services/api/internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/api-service cmd/main.go

FROM alpine:3.22
RUN adduser -D -g '' appuser
WORKDIR /app
COPY --from=builder /app/api-service .
USER appuser

EXPOSE 8080
CMD ["./api-service"]
