package com.example.kafka.s3.sink.avro;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public record DecodedAvro(Schema schema, List<GenericRecord> records) {
}
