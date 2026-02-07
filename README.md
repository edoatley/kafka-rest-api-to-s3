# Kafka REST API (Java 21 + Spring Boot)

High-throughput REST API that ingests events and publishes them to Kafka using Java 21 virtual threads.
Avro is used for binary encoding without relying on a Schema Registry.

## Key Features
- Spring MVC with Java 21 virtual threads (`spring.threads.virtual.enabled=true`)
- Avro SpecificRecord encoding without Schema Registry
- Kafka producer tuned for high throughput (batching, linger, compression)
- OAuth2 Resource Server (JWT) security
- Micrometer + OpenTelemetry tracing
- Testcontainers Kafka integration test

## Requirements
- Java 21
- Gradle (if you do not have a wrapper)
- Docker Desktop (for tests or local Kafka)

## Run Locally (macOS CLI)

### 1) Start Kafka
Using Docker:
```
docker network create kafka-net
docker run -d --name kafka --network kafka-net -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  confluentinc/cp-kafka:7.6.1
```

### 2) Run the app
```
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
gradle bootRun
```

### 3) Send a test event
```
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-1","type":"user.created","payload":"{\"userId\":\"123\"}"}'
```

Use `x-ack-mode` to switch between fire-and-forget and wait-for-ack:
```
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -H "x-ack-mode: wait" \
  -d '{"id":"evt-2","type":"user.updated","payload":"{\"userId\":\"456\"}"}'
```

### 4) Stream a batch and read acks (NDJSON)
Send newline-delimited JSON and receive a newline-delimited ack stream:
```
printf '%s\n' \
  '{"id":"evt-10","type":"user.created","payload":"{\"userId\":\"10\"}"}' \
  '{"id":"evt-11","type":"user.updated","payload":"{\"userId\":\"11\"}"}' \
| curl -N --no-buffer -X POST http://localhost:8080/events/stream \
  -H "Content-Type: application/x-ndjson" \
  -H "x-ack-mode: wait" \
  -H "x-max-in-flight: 1000" \
  --data-binary @-
```

## Run Tests (macOS CLI)
```
gradle test
```

## Notes on Avro Without Schema Registry
- The Kafka producer uses `com.example.kafkarestapi.kafka.AvroEventSerializer`.
- Schema evolution is your responsibility. Ensure both producers and consumers are upgraded
  with compatible Avro schemas.

## Configuration
See `src/main/resources/application.yml` for:
- Producer throughput tuning
- `acks` trade-off between throughput and durability
- Optional Kafka mTLS settings
- OpenTelemetry exporter endpoint
