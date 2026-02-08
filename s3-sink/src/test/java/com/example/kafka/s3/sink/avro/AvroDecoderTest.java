package com.example.kafka.s3.sink.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import com.example.kafka.s3.sink.testutil.AvroTestData;

class AvroDecoderTest {

	private final AvroDecoder decoder = new AvroDecoder();

	@Test
	void decodesAvroContainerPayload() {
		Schema schema = AvroTestData.sampleSchema();
		GenericRecord record = AvroTestData.sampleRecord(schema, 42, "hello");
		byte[] payload = AvroTestData.toContainerBytes(schema, List.of(record));

		DecodedAvro decoded = decoder.decode(payload);

		assertThat(decoded.schema().getName()).isEqualTo("SampleEvent");
		assertThat(decoded.records()).hasSize(1);
		assertThat(decoded.records().get(0).get("id")).isEqualTo(42);
	}

	@Test
	void rejectsInvalidAvroPayload() {
		byte[] payload = "not-avro".getBytes();

		assertThatThrownBy(() -> decoder.decode(payload))
				.isInstanceOf(AvroDecodingException.class);
	}
}
