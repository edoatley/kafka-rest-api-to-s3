package com.example.kafka.s3.sink.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

@Component
public class AvroDecoder {

	public DecodedAvro decode(byte[] payload) {
		try (DataFileStream<GenericRecord> stream = new DataFileStream<>(
				new ByteArrayInputStream(payload), new GenericDatumReader<>())) {
			Schema schema = stream.getSchema();
			List<GenericRecord> records = new ArrayList<>();
			while (stream.hasNext()) {
				records.add(stream.next());
			}
			if (records.isEmpty()) {
				throw new AvroDecodingException("Avro payload contained no records", null);
			}
			return new DecodedAvro(schema, records);
		} catch (IOException ex) {
			throw new AvroDecodingException("Failed to decode Avro payload", ex);
		}
	}
}
