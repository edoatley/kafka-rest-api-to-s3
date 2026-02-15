package com.example.kafka.s3.sink.avro;

/**
 * Decodes Avro payloads from Kafka messages into schema and records.
 * Implementations support different wire formats (OCF, Confluent Schema Registry).
 */
public interface AvroDecoderInterface {

	DecodedAvro decode(byte[] payload);
}
