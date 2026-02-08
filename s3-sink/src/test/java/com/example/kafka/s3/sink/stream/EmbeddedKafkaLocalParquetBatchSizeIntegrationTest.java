package com.example.kafka.s3.sink.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import com.example.kafka.s3.sink.testutil.AvroTestData;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "local-parquet-batch-topic")
@TestPropertySource(properties = {
		"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
		"spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
		"app.source-topics=local-parquet-batch-topic",
		"app.mappings[0].topic=local-parquet-batch-topic",
		"app.mappings[0].destination=LOCAL",
		"app.mappings[0].directory=out",
		"app.batch.maxRecords=3",
		"app.batch.flushInterval=PT30S"
})
class EmbeddedKafkaLocalParquetBatchSizeIntegrationTest {

	private static final String TOPIC = "local-parquet-batch-topic";

	@TempDir
	static Path tempDir;

	@Autowired
	private KafkaTemplate<String, byte[]> kafkaTemplate;

	@DynamicPropertySource
	static void registerProperties(DynamicPropertyRegistry registry) {
		registry.add("app.local.baseDir", () -> tempDir.toString());
	}

	@Test
	void batchesMultipleMessagesIntoSingleParquetFile() throws Exception {
		Schema schema = AvroTestData.sampleSchema();
		GenericRecord first = AvroTestData.sampleRecord(schema, 1, "batch");
		GenericRecord second = AvroTestData.sampleRecord(schema, 2, "batch");
		GenericRecord third = AvroTestData.sampleRecord(schema, 3, "batch");

		kafkaTemplate.send(TOPIC, AvroTestData.toContainerBytes(schema, List.of(first)))
				.get(5, TimeUnit.SECONDS);
		kafkaTemplate.send(TOPIC, AvroTestData.toContainerBytes(schema, List.of(second)))
				.get(5, TimeUnit.SECONDS);
		kafkaTemplate.send(TOPIC, AvroTestData.toContainerBytes(schema, List.of(third)))
				.get(5, TimeUnit.SECONDS);

		waitForParquetFiles(tempDir, 1, Duration.ofSeconds(10));

		assertThat(countParquetFiles(tempDir)).isEqualTo(1);
	}

	private void waitForParquetFiles(Path baseDir, int expected, Duration timeout) throws Exception {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			if (countParquetFiles(baseDir) >= expected) {
				return;
			}
			Thread.sleep(200);
		}
	}

	private int countParquetFiles(Path baseDir) throws Exception {
		try (var stream = Files.walk(baseDir)) {
			return (int) stream
					.filter(path -> path.toString().endsWith(".parquet"))
					.count();
		}
	}
}
