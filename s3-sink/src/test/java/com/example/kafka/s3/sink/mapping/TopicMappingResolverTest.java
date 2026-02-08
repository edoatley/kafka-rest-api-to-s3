package com.example.kafka.s3.sink.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.example.kafka.s3.sink.config.AppProperties;
import com.example.kafka.s3.sink.config.TargetType;
import com.example.kafka.s3.sink.writer.WriteTarget;

class TopicMappingResolverTest {

	@Test
	void resolvesLocalTargetWithBaseDir() {
		AppProperties properties = new AppProperties();
		properties.getLocal().setBaseDir(Path.of("/tmp/base"));
		AppProperties.TopicMapping mapping = new AppProperties.TopicMapping();
		mapping.setTopic("events");
		mapping.setDestination(TargetType.LOCAL);
		mapping.setDirectory("local");
		properties.setMappings(List.of(mapping));

		ParquetPathBuilder builder = new ParquetPathBuilder(fixedClock());
		TopicMappingResolver resolver = new TopicMappingResolver(properties, builder);

		WriteTarget target = resolver.resolve("events");

		assertThat(target.type()).isEqualTo(TargetType.LOCAL);
		assertThat(target.localPath().toString()).contains("/tmp/base/local/topic=events/date=2024-01-01/");
	}

	@Test
	void resolvesS3TargetWithBucket() {
		AppProperties properties = new AppProperties();
		properties.getS3().setBucket("default-bucket");
		AppProperties.TopicMapping mapping = new AppProperties.TopicMapping();
		mapping.setTopic("events");
		mapping.setDestination(TargetType.S3);
		mapping.setPrefix("prefix");
		properties.setMappings(List.of(mapping));

		ParquetPathBuilder builder = new ParquetPathBuilder(fixedClock());
		TopicMappingResolver resolver = new TopicMappingResolver(properties, builder);

		WriteTarget target = resolver.resolve("events");

		assertThat(target.type()).isEqualTo(TargetType.S3);
		assertThat(target.bucket()).isEqualTo("default-bucket");
		assertThat(target.key()).startsWith("prefix/topic=events/date=2024-01-01/");
	}

	private Clock fixedClock() {
		return Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneOffset.UTC);
	}
}
