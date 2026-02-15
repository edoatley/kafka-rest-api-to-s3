#!/usr/bin/env bash
#
# Local demo test script for Kafka REST API -> S3 (Parquet) with Schema Registry.
#
# Usage:
#   ./scripts/local-demo-test.sh
#
# Prerequisites: docker compose, curl, aws cli, python3
# Run from project root. Starts the stack if not already running.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
S3_ENDPOINT="http://localhost:4566"
API_URL="http://localhost:8080"
SCHEMA_REGISTRY_URL="http://localhost:8081"

log() { echo "[$(date +%H:%M:%S)] $*"; }
fail() { echo "[$(date +%H:%M:%S)] ERROR: $*" >&2; exit 1; }

# ---

log "=== Local Demo Test ==="
log "Project root: $PROJECT_ROOT"
log ""

log "--- Step 1: Checking stack is running ---"
if ! docker compose ps 2>/dev/null | grep -q "Up"; then
  log "Stack does not appear to be running. Starting..."
  docker compose up --build -d
  log "Waiting 30s for services to initialize (Kafka, Schema Registry, API)..."
  sleep 30
else
  log "Stack already running."
fi

log ""
log "--- Step 2: Waiting for Schema Registry ---"
for i in {1..30}; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$SCHEMA_REGISTRY_URL/subjects" 2>/dev/null) || code="000"
  code="${code:-000}"
  if [ "$code" = "200" ]; then
    log "Schema Registry ready (HTTP $code)"
    break
  fi
  log "  Attempt $i/30: Schema Registry returned HTTP $code, retrying in 2s..."
  sleep 2
  if [ "$i" -eq 30 ]; then
    fail "Schema Registry did not become ready. Check: docker compose logs schema-registry"
  fi
done

log ""
log "--- Step 3: Waiting for API health ---"
for i in {1..60}; do
  body=$(curl -s "$API_URL/actuator/health" 2>/dev/null || echo "")
  if echo "$body" | grep -q '"status":"UP"'; then
    log "API healthy"
    break
  fi
  log "  Attempt $i/60: API not ready, retrying in 2s..."
  sleep 2
  if [ "$i" -eq 60 ]; then
    fail "API did not become healthy. Check: docker compose logs api"
  fi
done

log ""
log "--- Step 4: Warm-up single event (verify Kafka connectivity) ---"
warmup_code=$(curl -s -o /tmp/publish-warmup.json -w '%{http_code}' -X POST "$API_URL/events" \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-warmup","type":"user.created","payload":"{}"}')
if [ "$warmup_code" -lt 200 ] || [ "$warmup_code" -ge 300 ]; then
  log "Warm-up failed (HTTP $warmup_code). Dumping API logs:"
  docker compose logs api --tail 80 2>/dev/null || true
  fail "Warm-up failed (HTTP $warmup_code). Response: $(cat /tmp/publish-warmup.json 2>/dev/null || echo 'none')"
fi
log "Warm-up succeeded (HTTP $warmup_code)"

log ""
log "--- Step 5: Publishing v1 events (OCF format) to /events/stream ---"
tmpfile=$(mktemp)
trap "rm -f $tmpfile" EXIT
python3 - <<'PY' > "$tmpfile"
import json
for i in range(1, 51):
    payload = json.dumps({"userId": f"{i:03d}"})
    print(json.dumps({
        "id": f"evt-{i:03d}",
        "type": "user.created",
        "payload": payload,
    }))
PY

code=$(curl -s -o /tmp/publish-v1.json -w '%{http_code}' -X POST "$API_URL/events/stream" \
  -H "Content-Type: application/x-ndjson" \
  -H "x-ack-mode: wait" \
  --data-binary @"$tmpfile")

if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
  log "v1 publish failed. Dumping API logs:"
  docker compose logs api --tail 80 2>/dev/null || true
  fail "v1 publish failed (HTTP $code). Response: $(cat /tmp/publish-v1.json 2>/dev/null || echo 'none')"
fi
log "v1 publish succeeded (HTTP $code)"

log ""
log "--- Step 6: Publishing v2 events (Schema Registry format) to /events/v2/stream ---"
python3 - <<'PY' > "$tmpfile"
import json
for i in range(51, 101):
    payload = json.dumps({"userId": f"{i:03d}"})
    print(json.dumps({
        "id": f"evt-sr-{i:03d}",
        "type": "user.created",
        "payload": payload,
    }))
PY

code=$(curl -s -o /tmp/publish-v2.json -w '%{http_code}' -X POST "$API_URL/events/v2/stream" \
  -H "Content-Type: application/x-ndjson" \
  -H "x-ack-mode: wait" \
  --data-binary @"$tmpfile")

if [ "$code" -lt 200 ] || [ "$code" -ge 300 ]; then
  log "v2 publish failed. Dumping API logs:"
  docker compose logs api --tail 80 2>/dev/null || true
  fail "v2 publish failed (HTTP $code). Response: $(cat /tmp/publish-v2.json 2>/dev/null || echo 'none')"
fi
log "v2 publish succeeded (HTTP $code)"

log ""
log "--- Step 7: Waiting for S3 sink flush (5s) ---"
sleep 5

log ""
log "--- Step 8: Verifying S3 output (events prefix) ---"
if ! aws --endpoint-url="$S3_ENDPOINT" s3 ls "s3://demo-parquet-bucket/events/" 2>/dev/null | head -5; then
  fail "Could not list events prefix. Check: docker compose logs s3-sink"
fi

topic_prefix=""
for i in {1..30}; do
  topic_prefix=$(aws --endpoint-url="$S3_ENDPOINT" s3 ls "s3://demo-parquet-bucket/events/" 2>/dev/null | awk '/PRE topic=/{print $2}' | head -n1)
  if [ -n "$topic_prefix" ]; then
    break
  fi
  log "  Waiting for events topic prefix... ($i/30)"
  sleep 2
done

if [ -z "$topic_prefix" ]; then
  log "No events topic prefix. Dumping s3-sink logs:"
  docker compose logs s3-sink --tail 50 2>/dev/null || true
  fail "No events topic prefix in S3. Listing: $(aws --endpoint-url="$S3_ENDPOINT" s3api list-objects-v2 --bucket demo-parquet-bucket --prefix events/ --max-keys 50 2>/dev/null || echo 'failed')"
fi
log "v1 output found: events/${topic_prefix}"

log ""
log "--- Step 9: Verifying S3 output (events-schema-registry prefix) ---"
sr_prefix=""
for i in {1..30}; do
  sr_prefix=$(aws --endpoint-url="$S3_ENDPOINT" s3 ls "s3://demo-parquet-bucket/events-schema-registry/" 2>/dev/null | awk '/PRE topic=/{print $2}' | head -n1)
  if [ -n "$sr_prefix" ]; then
    break
  fi
  log "  Waiting for events-schema-registry topic prefix... ($i/30)"
  sleep 2
done

if [ -z "$sr_prefix" ]; then
  log "No events-schema-registry prefix. Dumping s3-sink logs:"
  docker compose logs s3-sink --tail 50 2>/dev/null || true
  fail "No events-schema-registry prefix in S3. Listing: $(aws --endpoint-url="$S3_ENDPOINT" s3api list-objects-v2 --bucket demo-parquet-bucket --prefix events-schema-registry/ --max-keys 50 2>/dev/null || echo 'failed')"
fi
log "v2 output found: events-schema-registry/${sr_prefix}"

log ""
log "--- Step 10: Listing Parquet files ---"
log "v1 (events):"
aws --endpoint-url="$S3_ENDPOINT" s3 ls "s3://demo-parquet-bucket/events/${topic_prefix}" 2>/dev/null | head -5 || true
log "v2 (events-schema-registry):"
aws --endpoint-url="$S3_ENDPOINT" s3 ls "s3://demo-parquet-bucket/events-schema-registry/${sr_prefix}" 2>/dev/null | head -5 || true

log ""
log "=== All checks passed ==="
