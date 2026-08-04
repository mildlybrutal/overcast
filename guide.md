# Overcast Run Guide

Overcast is a Go workspace for live cricket data. The local stack runs these services:

- Kafka: event bus for raw match updates
- PostgreSQL: durable match store
- Redis: live match cache and notification pub/sub
- Ingestion: polls CricAPI and publishes match events to Kafka
- Processing: consumes Kafka events and writes PostgreSQL plus Redis
- API: serves match reads over HTTP on port `8080`
- Notification: serves live WebSocket updates on port `8081`

## Requirements

- Docker and Docker Compose
- A CricAPI key for the ingestion service
- Go 1.26 or compatible, if running services outside Docker

## Run Everything With Docker Compose

The root `.env` file should contain:

```text
CRICKET_API_KEY=<your-cricapi-key>
```

Docker Compose reads this file automatically when run from the repository root.

Start all containers:

```sh
docker compose up --build
```

If your Docker Compose uses the older standalone binary, use:

```sh
docker-compose up --build
```

The public endpoints are:

```text
API:          http://localhost:8080
Notification: ws://localhost:8081/ws?match_id=<match-id>
Kafka:        localhost:9092
PostgreSQL:   localhost:5432
Redis:        localhost:6379
```

Useful API checks:

```sh
curl http://localhost:8080/api/v1/matches
curl http://localhost:8080/api/v1/matches/<match-id>
```

To stop the stack:

```sh
docker compose down
```

To also remove the Postgres and Redis volumes:

```sh
docker compose down -v
```

## Configuration

The compose file provides service-to-service defaults. These are the main variables:

| Service | Variable | Compose value |
| --- | --- | --- |
| ingestion | `CRICKET_API_KEY` | `${CRICKET_API_KEY}` |
| ingestion | `KAFKA_BROKER` | `kafka:29092` |
| ingestion | `KAFKA_TOPIC` | `raw-match-events` |
| processing | `KAFKA_BROKER` | `kafka:29092` |
| processing | `KAFKA_TOPIC` | `raw-match-events` |
| processing | `KAFKA_GROUP_ID` | `processing-group` |
| processing | `POSTGRES_CONN` | `postgres://cricket_admin:cricket_password@postgres:5432/cricket_db?sslmode=disable` |
| processing | `REDIS_ADDR` | `redis:6379` |
| api | `PORT` | `8080` |
| api | `POSTGRES_CONN` | `postgres://cricket_admin:cricket_password@postgres:5432/cricket_db?sslmode=disable` |
| api | `REDIS_ADDR` | `redis:6379` |
| notification | `PORT` | `8081` |
| notification | `REDIS_ADDR` | `redis:6379` |

## Run Infrastructure Only

Use this when you want to run Go services directly on your machine:

```sh
docker compose up kafka postgres redis
```

Then run services in separate terminals from the repository root.

Load the CricAPI key for local shell commands:

```sh
set -a
. ./.env
set +a
```

Ingestion:

```sh
cd services/ingestion
KAFKA_BROKER=localhost:9092 \
KAFKA_TOPIC=raw-match-events \
go run ./cmd
```

Processing:

```sh
cd services/processing
KAFKA_BROKER=localhost:9092 \
KAFKA_TOPIC=raw-match-events \
KAFKA_GROUP_ID=processing-group \
POSTGRES_CONN='postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable' \
REDIS_ADDR=localhost:6379 \
go run ./cmd
```

API:

```sh
cd services/api
PORT=8080 \
POSTGRES_CONN='postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable' \
REDIS_ADDR=localhost:6379 \
go run ./cmd
```

Notification:

```sh
cd services/notification
PORT=8081 \
REDIS_ADDR=localhost:6379 \
go run ./cmd
```

## Notification Test

Connect a WebSocket client to:

```text
ws://localhost:8081/ws?match_id=test-match
```

Publish a test update to the Redis channel:

```sh
redis-cli PUBLISH match:updates:test-match '{"type":"score_update"}'
```

The WebSocket client should receive the published JSON payload.

## Verify The Go Workspace

From the repository root:

```sh
go test ./gen/... ./internal/models/... ./services/api/... ./services/ingestion/... ./services/notification/... ./services/processing/...
```

If your environment cannot write to the default Go build cache, use:

```sh
GOCACHE=/tmp/go-build-cache go test ./gen/... ./internal/models/... ./services/api/... ./services/ingestion/... ./services/notification/... ./services/processing/...
```
