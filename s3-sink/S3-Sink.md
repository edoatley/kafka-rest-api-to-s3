# Kafka Avro to Parquet Sink

This Spring Cloud Stream application consumes Avro records from Kafka, converts them to Parquet, and writes them to either a local directory (for testing) or to an S3 bucket (for production). It is designed for resilience and configuration-driven topic mappings.

## How it works
- Kafka messages are expected to contain **Avro Object Container File (OCF)** payloads. The schema is inferred from the payload, so no schema registry is required.
- Each message is decoded to one or more Avro records, converted to Parquet, and written as a single Parquet file.
- Output locations are resolved from topic mappings so new topics can be added without code changes.

## Configuration

### Core settings
The defaults below match `s3-sink/src/main/resources/application.yml`.

```yaml
app:
  source-topics: ""
  dlq-topic: "kafka-s3-sink-dlq"
  batch:
    maxRecords: 1
    flushInterval: PT0S
  local:
    baseDir: /tmp/kafka-s3-sink
  s3:
    region: us-east-1
    pathStyle: false
  mappings:
    []
```

Batching behavior is controlled by:

- `app.batch.maxRecords` — number of records to accumulate before writing a Parquet file.
- `app.batch.flushInterval` — max time to wait before flushing a partial batch (ISO-8601 duration).

Topic mappings determine output per topic:
- `LOCAL` mappings require `directory` (subfolder under `app.local.baseDir`)
- `S3` mappings can specify `bucket` and `prefix`; if omitted, they fall back to `app.s3.bucket` and `app.s3.prefix`

### Profiles
| Profile | Kafka auth | Destination | Notes |
| --- | --- | --- | --- |
| `demo` | PLAINTEXT | Local | Uses `LOCAL_OUTPUT_DIR` if set |
| `demo-s3` | PLAINTEXT | S3 (LocalStack) | Uses test credentials, `app.s3.endpoint` |
| `local` | SASL_PLAINTEXT (PLAIN) | Local | Requires `KAFKA_USERNAME`/`KAFKA_PASSWORD` |
| `prod` | SASL_SSL (Kerberos) | S3 | Uses IAM/default AWS credentials |

### Defaults and key options
| Property | Default | Purpose |
| --- | --- | --- |
| `app.source-topics` | `""` | Comma-separated Kafka topics to consume |
| `app.dlq-topic` | `kafka-s3-sink-dlq` | DLQ topic name |
| `app.batch.maxRecords` | `1` | Records per Parquet file |
| `app.batch.flushInterval` | `PT0S` | Max wait before flushing |
| `app.local.baseDir` | `/tmp/kafka-s3-sink` | Local output base dir |
| `app.s3.region` | `us-east-1` | AWS region |
| `app.s3.pathStyle` | `false` | Path-style S3 access |

### DLQ and retry/backoff
Spring Cloud Stream retries and DLQ behavior are configured in `application.yml`:
- Retries: `maxAttempts`, `backOffInitialInterval`, `backOffMaxInterval`, `backOffMultiplier`
- DLQ: `enableDlq=true` and `dlqName=${app.dlq-topic}`

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
