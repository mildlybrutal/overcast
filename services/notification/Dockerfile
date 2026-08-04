ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /app/services/notification

COPY services/notification/go.mod services/notification/go.sum ./
RUN go mod download

COPY services/notification/cmd/ ./cmd/
COPY services/notification/internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/notification ./cmd

FROM alpine:3.22

RUN addgroup -S app && adduser -S app -G app

WORKDIR /app
COPY --from=builder /app/notification ./notification

USER app

EXPOSE 8081

ENV PORT=8081
ENV REDIS_ADDR=redis:6379

ENTRYPOINT ["./notification"]
