# Kafka REST API

## Introduction

This project aims to create a high-throughput REST API that ingests events and publishes them to
Kafka using Java 21 virtual threads. Avro is used for binary encoding without relying on a Schema Registry.

## Key Features

- Spring MVC with Java 21 virtual threads (`spring.threads.virtual.enabled=true`)
- Avro SpecificRecord encoding without Schema Registry
- Kafka producer tuned for high throughput (batching, linger, compression)
- OAuth2 Resource Server (JWT) security
- Micrometer + OpenTelemetry tracing
- Testcontainers Kafka integration test

## Requirements

- Java 21
- Gradle 8+ (if you do not have a wrapper)
- Docker Desktop (for tests or local Kafka)

## Run Locally (macOS CLI)

### 1) Start Kafka

Using Docker (KRaft mode):

```bash
docker stop kafka && docker rm kafka
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES='broker,controller' \
  -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093' \
  -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:9092' \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093' \
  -e KAFKA_CONTROLLER_LISTENER_NAMES='CONTROLLER' \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e CLUSTER_ID='MkU3OEVBNTcwNTJENDM2Qk' \
  confluentinc/cp-kafka:latest
```

Review the logs if desired:

```bash
docker logs kafka
```

Create the `events` topic:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic events --partitions 1 --replication-factor 1
```

### 2) Run the app

```
./gradlew localBootRun
```

The `local` profile disables security for local testing and sets Kafka bootstrap servers to
`localhost:9092`. To run with OAuth2 JWT security enabled, either set `app.security.enabled=true`
and provide one of the following:
```
export SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_ISSUER_URI=https://issuer.example.com/
# or
export SPRING_SECURITY_OAUTH2_RESOURCESERVER_JWT_JWK_SET_URI=https://issuer.example.com/.well-known/jwks.json
```

### 3) Send a test event

```
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-1","type":"user.created","payload":"{\"userId\":\"123\"}"}'
```

which responds with:

```json
{"id":"evt-1","status":"queued"}
```

you should see a log message in the application like this:

```output
2026-02-08T11:49:37.726Z  INFO 63678 --- [kafka-rest-api] [-api-producer-1] c.e.k.service.KafkaEventProducer         : Kafka publish succeeded for id=evt-1 topic=events partition=0 offset=2
```

Use `x-ack-mode` to switch between fire-and-forget and wait-for-ack:

```
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -H "x-ack-mode: wait" \
  -d '{"id":"evt-2","type":"user.updated","payload":"{\"userId\":\"456\"}"}'
```

which responds with:

```json
{"id":"evt-2","status":"acked"}
```

you should see a log message in the application like this:

```output
2026-02-08T11:50:41.439Z  INFO 63678 --- [kafka-rest-api] [-api-producer-1] c.e.k.service.KafkaEventProducer         : Kafka publish succeeded for id=evt-2 topic=events partition=0 offset=3
```


### 4) Stream a batch and read acks (NDJSON)

Send newline-delimited JSON and receive a newline-delimited ack stream:

```bash
printf '%s\n' \
  '{"id":"evt-10","type":"user.created","payload":"{\"userId\":\"10\"}"}' \
  '{"id":"evt-11","type":"user.updated","payload":"{\"userId\":\"11\"}"}' \
| curl -N --no-buffer -X POST http://localhost:8080/events/stream \
  -H "Content-Type: application/x-ndjson" \
  -H "x-ack-mode: wait" \
  -H "x-max-in-flight: 1000" \
  --data-binary @-
```

which responds with:

```json
{"id":"evt-10","status":"acked"}
{"id":"evt-11","status":"acked"}
```

you should see log messages in the application like this:

```output
2026-02-08T11:52:21.730Z  INFO 63678 --- [kafka-rest-api] [-api-producer-1] c.e.k.service.KafkaEventProducer         : Kafka publish succeeded for id=evt-10 topic=events partition=0 offset=4
2026-02-08T11:52:21.731Z  INFO 63678 --- [kafka-rest-api] [-api-producer-1] c.e.k.service.KafkaEventProducer         : Kafka publish succeeded for id=evt-11 topic=events partition=0 offset=5
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
