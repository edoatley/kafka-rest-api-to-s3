package com.example.kafka.s3.sink.avro;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
@ConditionalOnExpression("'${app.schema-registry-url:}'.trim().length() > 0")
public class CompositeAvroDecoder implements AvroDecoderInterface {

	private static final byte SCHEMA_REGISTRY_MAGIC = 0;

	private final AvroDecoder ocfDecoder;
	private final SchemaRegistryAvroDecoder schemaRegistryDecoder;

	public CompositeAvroDecoder(AvroDecoder ocfDecoder, SchemaRegistryAvroDecoder schemaRegistryDecoder) {
		this.ocfDecoder = ocfDecoder;
		this.schemaRegistryDecoder = schemaRegistryDecoder;
	}

	@Override
	public DecodedAvro decode(byte[] payload) {
		if (payload != null && payload.length >= 5 && payload[0] == SCHEMA_REGISTRY_MAGIC) {
			return schemaRegistryDecoder.decode(payload);
		}
		return ocfDecoder.decode(payload);
	}
}
