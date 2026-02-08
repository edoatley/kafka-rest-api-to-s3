package com.example.kafka.s3.sink.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.example.kafka.s3.sink.testutil.AvroTestData;

@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
class AvroToParquetIntegrationTest {

	private static final String TOPIC = "test-topic";

	@TempDir
	static Path tempDir;

	@Autowired
	private InputDestination inputDestination;

	@DynamicPropertySource
	static void registerProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.cloud.function.definition", () -> "avroToParquet");
		registry.add("spring.cloud.stream.function.definition", () -> "avroToParquet");
		registry.add("spring.cloud.stream.bindings.avroToParquet-in-0.destination", () -> TOPIC);
		registry.add("app.local.baseDir", () -> tempDir.toString());
		registry.add("app.mappings[0].topic", () -> TOPIC);
		registry.add("app.mappings[0].destination", () -> "LOCAL");
		registry.add("app.mappings[0].directory", () -> "out");
	}

	@Test
	void writesParquetFileForIncomingMessage() throws Exception {
		Schema schema = AvroTestData.sampleSchema();
		GenericRecord record = AvroTestData.sampleRecord(schema, 9, "integration");
		byte[] payload = AvroTestData.toContainerBytes(schema, List.of(record));
		Message<byte[]> message = MessageBuilder.withPayload(payload)
				.setHeader(KafkaHeaders.RECEIVED_TOPIC, TOPIC)
				.build();

		inputDestination.send(message, TOPIC);

		Optional<Path> parquetFile = waitForParquetFile(tempDir, Duration.ofSeconds(5));

		assertThat(parquetFile).isPresent();
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
