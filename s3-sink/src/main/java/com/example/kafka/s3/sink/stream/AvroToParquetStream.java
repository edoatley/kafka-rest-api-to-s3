package com.example.kafka.s3.sink.stream;

import java.util.function.Consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import com.example.kafka.s3.sink.avro.AvroDecoder;
import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.writer.WriterRouter;

@Configuration
public class AvroToParquetStream {

	@Bean
	public Consumer<Message<byte[]>> avroToParquet(AvroDecoder decoder, WriterRouter router) {
		return message -> {
			String topic = (String) message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC);
			if (topic == null || topic.isBlank()) {
				throw new IllegalArgumentException("Missing Kafka topic header for message");
			}
			DecodedAvro decoded = decoder.decode(message.getPayload());
			router.write(topic, decoded);
		};
	}
}
