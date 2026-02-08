# Kafka Avro to Parquet Sink

This Spring Cloud Stream application consumes Avro records from Kafka, converts them to Parquet, and writes them to either a local directory (for testing) or to an S3 bucket (for production). It is designed for resilience and configuration-driven topic mappings.

## How it works
- Kafka messages are expected to contain **Avro Object Container File (OCF)** payloads. The schema is inferred from the payload, so no schema registry is required.
- Each message is decoded to one or more Avro records, converted to Parquet, and written as a single Parquet file.
- Output locations are resolved from topic mappings so new topics can be added without code changes.

## Configuration

### Core settings
```yaml
app:
  source-topics: "orders,events"
  dlq-topic: "kafka-s3-sink-dlq"
  batch:
    maxRecords: 500
    flushInterval: PT5S
  local:
    baseDir: /tmp/kafka-s3-sink
  s3:
    region: us-east-1
    bucket: my-parquet-bucket
    prefix: data/
    endpoint: http://localhost:4566
    pathStyle: true
    accessKeyId: test
    secretAccessKey: test
  mappings:
    - topic: orders
      destination: LOCAL
      directory: orders
    - topic: events
      destination: S3
      bucket: my-parquet-bucket
      prefix: events/
```

Batching behavior is controlled by:

- `app.batch.maxRecords` — number of records to accumulate before writing a Parquet file.
- `app.batch.flushInterval` — max time to wait before flushing a partial batch (ISO-8601 duration).

### Local profile (SASL/PLAIN + local output)
Set `SPRING_PROFILES_ACTIVE=local` and provide credentials:
```bash
export KAFKA_USERNAME=local-user
export KAFKA_PASSWORD=local-pass
export LOCAL_OUTPUT_DIR=/tmp/kafka-s3-sink
```

### Production profile (Kerberos + IAM role)
Set `SPRING_PROFILES_ACTIVE=prod` and provide Kerberos values:
```bash
export KAFKA_KEYTAB=/etc/security/keytabs/kafka.keytab
export KAFKA_PRINCIPAL=your-principal@REALM
export AWS_REGION=us-east-1
```
S3 authentication uses the default AWS SDK credential chain (for example, IAM role).

## Running from the command line
```bash
./gradlew bootRun
```
If you need a specific profile:
```bash
SPRING_PROFILES_ACTIVE=local ./gradlew bootRun
```

## Testing
Run all tests:
```bash
./gradlew test
```
Integration tests include LocalStack for S3 uploads.

## Notes
- Avro payloads must be OCF encoded. If your Kafka messages use a different Avro encoding, update the decoder accordingly.
- The Kafka binding destination supports comma-separated topics via `app.source-topics`.
