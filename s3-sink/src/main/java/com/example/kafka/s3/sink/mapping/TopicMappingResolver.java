package com.example.kafka.s3.sink.mapping;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.example.kafka.s3.sink.config.AppProperties;
import com.example.kafka.s3.sink.config.TargetType;
import com.example.kafka.s3.sink.writer.WriteTarget;

@Component
public class TopicMappingResolver {

	private final AppProperties properties;
	private final ParquetPathBuilder pathBuilder;

	public TopicMappingResolver(AppProperties properties, ParquetPathBuilder pathBuilder) {
		this.properties = properties;
		this.pathBuilder = pathBuilder;
	}

	public WriteTarget resolve(String topic) {
		AppProperties.TopicMapping mapping = properties.getMappings().stream()
				.filter(entry -> entry.getTopic() != null && entry.getTopic().equals(topic))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No mapping configured for topic: " + topic));

		TargetType destination = Optional.ofNullable(mapping.getDestination()).orElse(TargetType.LOCAL);
		if (destination == TargetType.LOCAL) {
			return WriteTarget.local(pathBuilder.buildLocalPath(
					properties.getLocal().getBaseDir(), mapping.getDirectory(), topic));
		}

		String bucket = mapping.getBucket() != null ? mapping.getBucket() : properties.getS3().getBucket();
		if (bucket == null || bucket.isBlank()) {
			throw new IllegalArgumentException("No S3 bucket configured for topic: " + topic);
		}
		String prefix = mapping.getPrefix() != null ? mapping.getPrefix() : properties.getS3().getPrefix();
		return WriteTarget.s3(bucket, pathBuilder.buildS3Key(prefix, topic));
	}
}
