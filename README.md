### Scalable Event-Driven Backend Service (.NET 9 + Kafka)

This project implements the coding challenge using SOLID, Event Sourcing ideas, Clean Code, and good practices.

It exposes a REST API to ingest user activity events and processes them asynchronously using Kafka. A background consumer stores processed events in a thread-safe in-memory store. No database is used.

---

#### Tech Stack
- .NET 9 Minimal APIs
- Confluent.Kafka client
- Hosted background service for Kafka consumer
- Thread-safe in-memory store (`ConcurrentQueue`)
- Dependency Injection and Options pattern
- OpenAPI description (development only)

---

#### API
- POST `/events`
  - Body:
    ```json
    {
      "userId": "string",
      "type": "login|logout|purchase",
      "timestamp": "ISO-8601 (optional)",
      "data": { "any": "optional payload" }
    }
    ```
  - Responses: `202 Accepted` on success, `400 Bad Request` if validation fails

- GET `/events?limit={n}`
  - Returns JSON array of processed events (as stored by consumer)
  - `limit` optional; defaults to all in memory

Note: Eventual consistency is expected between POST and GET.

---

#### How it works (high level)
- Producer: `KafkaEventProducer` validates the DTO and publishes to `KafkaSettings.Topic` with `UserId` as the key.
- Consumer: `KafkaConsumerHostedService` subscribes to the same topic and, for every message, deserializes and appends an `EventEnvelope` to the `InMemoryProcessedEventStore`.
- Query: `GET /events` returns items from the store (optionally limited).

---

#### Run locally
1) Ensure a Kafka broker is available and reachable by the app (default `localhost:9092`). You can use Docker:

```yaml
# docker compose -f kafka-compose.yaml up -d
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports: ["2181:2181"]
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on: [zookeeper]
```

Alternatively use Confluent Cloud free tier and set `Kafka:BootstrapServers` accordingly (and SASL if required).

2) Configure app settings if needed: `WebApplication1/appsettings.json` → `Kafka` section.

3) Run the app:
- From Rider/VS: run the `https` profile.
- CLI: `dotnet run --project WebApplication1`.

The default HTTPS URL is shown on startup (see `launchSettings.json`). The included `.http` file has ready-to-run examples.

---

#### Example with curl
```bash
# POST an event
curl -k -X POST https://localhost:7073/events \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-1",
    "type": "login",
    "timestamp": "2025-01-01T00:00:00Z",
    "data": {"ip":"127.0.0.1"}
  }'

# GET processed events
curl -k https://localhost:7073/events?limit=10
```

---

#### Design notes / decisions
- Separation of concerns: domain contracts vs infrastructure (Kafka/store) vs composition root (`Program.cs`).
- Interfaces (`IEventProducer`, `IProcessedEventStore`) allow swapping implementations for tests or future persistence (e.g., PostgreSQL) without changing API handlers.
- Simple validation at the producer boundary; consumer is resilient and logs errors.
- Thread-safe in-memory store using `ConcurrentQueue` with a simple, optional cap to avoid unbounded memory.
- OpenAPI is enabled in development for discoverability.

Limitations
- No authentication/authorization.
- No schema registry/Avro; using JSON for simplicity.
- In-memory store is volatile and bounded; not suitable for production persistence.
- Basic error handling; no retries/backoff tuning beyond Kafka client defaults.
