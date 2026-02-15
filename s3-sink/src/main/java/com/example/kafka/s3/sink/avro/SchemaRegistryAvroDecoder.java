package com.example.kafka.s3.sink.avro;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Component
@ConditionalOnExpression("'${app.schema-registry-url:}'.trim().length() > 0")
public class SchemaRegistryAvroDecoder {

	private final KafkaAvroDeserializer deserializer;

	public SchemaRegistryAvroDecoder(@Value("${app.schema-registry-url}") String schemaRegistryUrl) {
		Map<String, Object> config = Map.of("schema.registry.url", schemaRegistryUrl);
		this.deserializer = new KafkaAvroDeserializer();
		this.deserializer.configure(config, false);
	}

	public DecodedAvro decode(byte[] payload) {
		Object obj = deserializer.deserialize("", payload);
		if (obj == null) {
			throw new AvroDecodingException("Schema Registry payload deserialized to null", null);
		}
		GenericRecord record = (GenericRecord) obj;
		Schema schema = record.getSchema();
		return new DecodedAvro(schema, List.of(record));
	}
}
