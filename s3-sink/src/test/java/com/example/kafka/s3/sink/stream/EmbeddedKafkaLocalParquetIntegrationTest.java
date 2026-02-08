package com.example.kafka.s3.sink.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
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
@EmbeddedKafka(partitions = 1, topics = "local-parquet-topic")
@TestPropertySource(properties = {
		"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.cloud.stream.kafka.binder.brokers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
		"spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
		"app.source-topics=local-parquet-topic",
		"app.mappings[0].topic=local-parquet-topic",
		"app.mappings[0].destination=LOCAL",
		"app.mappings[0].directory=out"
})
class EmbeddedKafkaLocalParquetIntegrationTest {

	private static final String TOPIC = "local-parquet-topic";

	@TempDir
	static Path tempDir;

	@Autowired
	private KafkaTemplate<String, byte[]> kafkaTemplate;

	@DynamicPropertySource
	static void registerProperties(DynamicPropertyRegistry registry) {
		registry.add("app.local.baseDir", () -> tempDir.toString());
	}

	@Test
	void writesParquetFileFromEmbeddedKafka() throws Exception {
		Schema schema = AvroTestData.sampleSchema();
		GenericRecord record = AvroTestData.sampleRecord(schema, 12, "embedded");
		byte[] payload = AvroTestData.toContainerBytes(schema, List.of(record));

		kafkaTemplate.send(TOPIC, payload).get(5, TimeUnit.SECONDS);

		Optional<Path> parquetFile = waitForParquetFile(tempDir, Duration.ofSeconds(10));

		assertThat(parquetFile).isPresent();
		assertThat(Files.size(parquetFile.orElseThrow())).isGreaterThan(0);
	}

	private Optional<Path> waitForParquetFile(Path baseDir, Duration timeout) throws Exception {
		Instant deadline = Instant.now().plus(timeout);
		while (Instant.now().isBefore(deadline)) {
			try (var stream = Files.walk(baseDir)) {
				Optional<Path> found = stream
						.filter(path -> path.toString().endsWith(".parquet"))
						.findFirst();
				if (found.isPresent()) {
					return found;
				}
			}
			Thread.sleep(200);
		}
		return Optional.empty();
	}
}
