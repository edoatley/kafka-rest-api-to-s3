package com.example.kafka.s3.sink.testutil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

public final class AvroTestData {

	private AvroTestData() {
	}

	public static Schema sampleSchema() {
		String schemaJson = """
				{
				  "type": "record",
				  "name": "SampleEvent",
				  "namespace": "com.example.kafka.s3.sink",
				  "fields": [
				    {"name": "id", "type": "int"},
				    {"name": "payload", "type": "string"}
				  ]
				}
				""";
		return new Schema.Parser().parse(schemaJson);
	}

	public static GenericRecord sampleRecord(Schema schema, int id, String payload) {
		GenericRecord record = new GenericData.Record(schema);
		record.put("id", id);
		record.put("payload", payload);
		return record;
	}

	public static byte[] toContainerBytes(Schema schema, List<GenericRecord> records) {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
			writer.create(schema, outputStream);
			for (GenericRecord record : records) {
				writer.append(record);
			}
			writer.flush();
			return outputStream.toByteArray();
		} catch (IOException ex) {
			throw new IllegalStateException("Failed to encode Avro test payload", ex);
		}
	}
}
