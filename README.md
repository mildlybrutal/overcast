# Overcast: Real-Time Sports Event Processing Platform

Overcast is a distributed, high-throughput, low-latency real-time sports event processing platform written in Go. The system continuously ingests live sports data (such as cricket match events) from external providers, processes events via asynchronous event-driven pipelines, stores state in dual storage layers (in-memory cache and relational database), and broadcasts live updates to client applications via REST APIs and WebSockets.

---

## Table of Contents

- [Overview](#overview)
- [What It Does](#what-it-does)
- [How It Does It (Architecture & Component Design)](#how-it-does-it-architecture--component-design)
  - [Microservices Architecture](#microservices-architecture)
  - [System Architecture & Data Pipeline](#system-architecture--data-pipeline)
  - [Notification Service Actor Lifecycle](#notification-service-actor-lifecycle)
- [Why (Architectural Rationale & Design Patterns)](#why-architectural-rationale--design-patterns)
  - [Event-Driven Architecture](#event-driven-architecture)
  - [Protocol Buffers over JSON](#protocol-buffers-over-json)
  - [Dual-Write Caching Pattern](#dual-write-caching-pattern)
  - [Actor Model & Supervisor for WebSockets](#actor-model--supervisor-for-websockets)
  - [Transactional Outbox Pattern](#transactional-outbox-pattern)
- [Repository Structure](#repository-structure)
- [Tech Stack](#tech-stack)
- [Setup and Execution](#setup-and-execution)
  - [Prerequisites](#prerequisites)
  - [Environment Configuration](#environment-configuration)
  - [Running with Docker Compose](#running-with-docker-compose)
  - [Running Microservices Standalone](#running-microservices-standalone)
- [API and Streaming Specifications](#api-and-streaming-specifications)
  - [REST API Endpoints](#rest-api-endpoints)
  - [WebSocket Push Channel](#websocket-push-channel)
- [Verification and Testing](#verification-and-testing)

---

## Overview

Live sports data processing requires processing updates with minimal latency while guaranteeing consistency, scalability under high user concurrency, and reliability during third-party API rate limits or network degradation. Overcast satisfies these constraints using an event-driven Go microservice workspace backed by Apache Kafka, Redis, PostgreSQL, Protocol Buffers, and WebSockets.

---

## What It Does

1. **Live Data Ingestion**: Periodically fetches real-time sports scores and match state updates from external REST APIs (such as CricAPI / CricketData.org).
2. **Binary Event Streaming**: Transforms JSON payloads into Protocol Buffer binary objects and streams events over Apache Kafka.
3. **Dual Persistence & Caching**: Simultaneously updates an in-memory Redis cache for ultra-fast reads and upserts records into PostgreSQL for historical permanence.
4. **Low-Latency REST Queries**: Delivers match information via HTTP endpoints with automatic cache-aside fallback mechanism.
5. **Real-Time Push Notifications**: Pushes live score changes instantly to thousands of connected clients via WebSocket channels managed by an Actor Model system.

---

## How It Does It (Architecture & Component Design)

### Microservices Architecture

Overcast is organized as a Go multi-module workspace (`go.work`) comprising four core services and supporting internal libraries:

1. **Ingestion Service (`services/ingestion`)**
   - Polls external data endpoints at configurable intervals (e.g., 30 seconds to respect API limits).
   - Converts raw third-party JSON data into generated Protocol Buffer structures (`cricket.v1.MatchData`).
   - Serializes messages to binary format and publishes them to the Kafka topic `raw-match-events` using partition balancing (`LeastBytes`).

2. **Processing Service (`services/processing`)**
   - Consumes Protobuf messages from Kafka under the consumer group `processing-group`.
   - Deserializes binary payloads into typed structs.
   - Executes a dual-write pattern:
     - **Redis**: Caches live match data under key `match:live:<id>` with a 10-minute Time-To-Live (TTL).
     - **PostgreSQL**: Performs SQL upserts (`INSERT INTO matches ... ON CONFLICT (id) DO UPDATE`) to persist match scores and status changes.

3. **Query API Service (`services/api`)**
   - Implements a RESTful HTTP service using the Gin framework listening on port `8080`.
   - Utilizes a **Cache-Aside Strategy**:
     1. Queries Redis for live match state.
     2. On cache hit, returns cached JSON response immediately.
     3. On cache miss, queries PostgreSQL database, returns record, and populates cache.

4. **Notification Service (`services/notification`)**
   - Serves real-time WebSocket connections on port `8081` under endpoint `/ws?match_id=<match-id>`.
   - Uses an **Actor Model Architecture**:
     - **Supervisor**: Dynamically instantiates a `MatchActor` when a client connects to a specific match ID.
     - **MatchActor**: Listens to Redis Pub/Sub channels (`match:updates:<match-id>`), maintains connected client WebSocket sessions, and broadcasts updates.
     - **Self-Destruction / Idle Cleanup**: If all clients disconnect from a match, the `MatchActor` enters an idle state and automatically shuts down after a timeout (e.g., 30 to 60 seconds), freeing system memory.

5. **Protobuf & Code Generation (`proto/` & `gen/`)**
   - Defines strict interface definitions (`match.proto`) compiled using `protoc` into Go packages (`gen/cricket/v1`).

---

## System Architecture & Data Pipeline

```mermaid
flowchart TD
    subgraph Ingestion_Layer["Ingestion Layer"]
        ExtAPI["External Sports API<br/>(CricketData / HTTP)"]
        Ingestion["Ingestion Service<br/>(Protobuf Encoder)"]
    end

    subgraph Messaging_Layer["Messaging & Event Bus"]
        Kafka["Apache Kafka Broker<br/>(Topic: raw-match-events)"]
    end

    subgraph Processing_Layer["Processing & Storage Layer"]
        Processing["Processing Service<br/>(Dual-Write Engine)"]
        Redis[("Redis Cache & Pub/Sub<br/>(10m TTL)")]
        Postgres[("PostgreSQL Database<br/>(Durable Persistence)")]
    end

    subgraph Serving_Layer["Serving & Distribution Layer"]
        APIService["Query API Service<br/>(Gin Framework / Port 8080)"]
        NotificationService["Notification Service<br/>(Actor Model / Port 8081)"]
    end

    subgraph Clients["Clients & Consumers"]
        HTTPClient["HTTP REST Clients"]
        WSClient["WebSocket Subscribers"]
    end

    ExtAPI -->|Poll Every 30s| Ingestion
    Ingestion -->|Publish Protobuf Binary| Kafka
    Kafka -->|Consume via processing-group| Processing
    Processing -->|Dual-Write Cache| Redis
    Processing -->|Dual-Write Persistence| Postgres

    HTTPClient -->|GET /api/v1/matches| APIService
    APIService -->|1. Primary Read| Redis
    APIService -->|2. Fallback Read| Postgres

    WSClient -->|Connect ws://localhost:8081/ws| NotificationService
    NotificationService -->|Subscribe match:updates:id| Redis
```

### Notification Service Actor Lifecycle

```mermaid
sequenceDiagram
    autonumber
    actor Client as WebSocket Client
    participant Sup as Actor Supervisor
    participant Actor as MatchActor (match_id)
    participant Redis as Redis Pub/Sub

    Client->>Sup: Connect /ws?match_id=123
    alt MatchActor already active
        Sup->>Actor: Route ClientJoin
    else MatchActor not present
        Sup->>Actor: Spawn MatchActor(123)
        Actor->>Redis: Subscribe channel match:updates:123
        Sup->>Actor: Route ClientJoin
    end

    Redis-->>Actor: Publish live score update
    Actor-->>Client: Broadcast payload to client connection pool

    Client->>Actor: Client Disconnect
    Actor->>Actor: Remaining clients == 0 ? Start idle timer (60s)
    opt Idle timer expires with 0 clients
        Actor->>Sup: Unregister Actor
        Actor->>Redis: Unsubscribe Pub/Sub channel
        Actor->>Actor: Self-Destruct
    end
```

---

## Why (Architectural Rationale & Design Patterns)

### Event-Driven Architecture

- **Problem**: Directly coupling external API ingestion with database updates and user notifications creates bottleneck vulnerabilities, cascading failures, and high latency.
- **Solution**: Kafka acts as a shock absorber. Ingestion operates independently from downstream processing. If processing or database operations slow down, Kafka buffers events without losing data or stalling the ingestion worker.

### Protocol Buffers over JSON

- **Problem**: Serializing and deserializing JSON payloads across microservices over Kafka consumes high network bandwidth and CPU overhead.
- **Solution**: Protocol Buffers (`.proto`) serialize data into compact binary payloads. This dramatically reduces payload size on Kafka, speeds up serialization/deserialization, and enforces a strict contract between services.

### Dual-Write Caching Pattern

- **Problem**: Relational databases like PostgreSQL can become bottlenecked when thousands of users repeatedly poll for live match scores.
- **Solution**: Overcast implements a dual-write pattern during event ingestion and a cache-aside strategy during query execution:
  - High-frequency live requests are served in under 1ms from Redis in-memory storage.
  - PostgreSQL preserves full historical context, structure, and transactional durability.

### Actor Model & Supervisor for WebSockets

- **Problem**: Managing state for thousands of concurrent WebSocket connections per match using global mutexes causes lock contention, memory leaks, and complex race conditions.
- **Solution**: Each active match is assigned an isolated `MatchActor` managed by a `Supervisor`.
  - Messages are processed sequentially through an actor mailbox, eliminating lock contention.
  - Actors dynamically spawn when listeners connect and self-destruct after an idle timeout when all clients disconnect, reclaiming resources.

### Transactional Outbox Pattern

- **Problem**: Writing to a database and publishing an event to Kafka in separate operations can result in inconsistent states if one system succeeds while the other fails.
- **Solution**: The architecture accounts for the Transactional Outbox Pattern: database modifications and event outbox records are committed in a single atomic database transaction (`BEGIN ... COMMIT`). A background outbox worker polls outbox records and guarantees delivery to Kafka.

---

## Repository Structure

```text
overcast/
├── docker-compose.yaml        # Full infrastructure and service stack configuration
├── go.work                    # Go workspace configuration
├── go.work.sum                # Go workspace checksum file
├── ONBOARDING.md              # System onboarding guide and codebase walkthrough
├── guide.md                   # Command reference for running local and Docker stacks
├── proto/                     # Protocol Buffer definitions
│   └── cricket/
│       └── v1/
│           └── match.proto    # Match and score data schema definition
├── gen/                       # Generated Go code from Protocol Buffers
│   └── cricket/
│       └── v1/                # Compiled Go protobuf structs
├── internal/
│   └── models/                # Core domain models and shared Go structs
└── services/                  # Microservices
    ├── api/                   # REST API Service (Gin framework)
    ├── ingestion/             # API Ingestion Worker (Poller & Kafka Producer)
    ├── notification/          # WebSocket Push Service (Actor Model & Redis Pub/Sub)
    └── processing/            # Kafka Consumer & Storage Engine (Postgres & Redis Writer)
```

---

## Tech Stack

- **Language**: Go 1.26+ (Go Workspaces)
- **Message Broker**: Apache Kafka (KRaft mode)
- **Database**: PostgreSQL 16
- **Cache & Pub/Sub**: Redis 7
- **Serialization**: Protocol Buffers (proto3)
- **Web Framework**: Gin Web Framework
- **WebSockets**: Gorilla WebSocket
- **Containerization**: Docker & Docker Compose

---

## Setup and Execution

### Prerequisites

- Docker and Docker Compose
- Go 1.26 or higher (if running microservices outside Docker)
- Third-party API Key (CricAPI key for ingestion)

### Environment Configuration

Create a `.env` file in the root directory:

```env
CRICKET_API_KEY=your_cricapi_key_here
```

### Running with Docker Compose

To build and launch the entire infrastructure and microservices stack:

```bash
docker compose up --build
```

To stop all containers and tear down resources:

```bash
docker compose down
```

To remove persistent database and cache volumes:

```bash
docker compose down -v
```

### Running Microservices Standalone

If you prefer to run infrastructure components in Docker while running Go services locally:

1. **Start Infrastructure**:
   ```bash
   docker compose up kafka postgres redis
   ```

2. **Export Environment Variables**:
   ```bash
   set -a
   source .env
   set +a
   ```

3. **Launch Ingestion Service**:
   ```bash
   cd services/ingestion
   KAFKA_BROKER=localhost:9092 KAFKA_TOPIC=raw-match-events go run ./cmd
   ```

4. **Launch Processing Service**:
   ```bash
   cd services/processing
   KAFKA_BROKER=localhost:9092 \
   KAFKA_TOPIC=raw-match-events \
   KAFKA_GROUP_ID=processing-group \
   POSTGRES_CONN='postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable' \
   REDIS_ADDR=localhost:6379 \
   go run ./cmd
   ```

5. **Launch Query API Service**:
   ```bash
   cd services/api
   PORT=8080 \
   POSTGRES_CONN='postgres://cricket_admin:cricket_password@localhost:5432/cricket_db?sslmode=disable' \
   REDIS_ADDR=localhost:6379 \
   go run ./cmd
   ```

6. **Launch Notification Service**:
   ```bash
   cd services/notification
   PORT=8081 \
   REDIS_ADDR=localhost:6379 \
   go run ./cmd
   ```

---

## API and Streaming Specifications

### REST API Endpoints

Base URL: `http://localhost:8080`

| Method | Endpoint | Description | Cache Strategy |
| --- | --- | --- | --- |
| GET | `/api/v1/matches` | List all historical and ongoing matches | Queries PostgreSQL |
| GET | `/api/v1/matches/:id` | Get details and live score for a specific match | Redis Cache -> Postgres Fallback |

#### Example Request:
```bash
curl -s http://localhost:8080/api/v1/matches
```

### WebSocket Push Channel

Endpoint: `ws://localhost:8081/ws?match_id=<match-id>`

#### Testing Notifications:
Connect a WebSocket client to `ws://localhost:8081/ws?match_id=test-match` and publish a test update via Redis CLI:

```bash
redis-cli PUBLISH match:updates:test-match '{"type":"score_update","score":"185/4"}'
```

Connected WebSocket clients will receive the JSON update instantly.

---

## Verification and Testing

Execute the test suite across all workspace packages from the repository root:

```bash
go test ./gen/... ./internal/models/... ./services/api/... ./services/ingestion/... ./services/notification/... ./services/processing/...
```
