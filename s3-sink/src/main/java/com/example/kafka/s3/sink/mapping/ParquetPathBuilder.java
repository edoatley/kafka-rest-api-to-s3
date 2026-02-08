package com.example.kafka.s3.sink.mapping;

import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
public class ParquetPathBuilder {

	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_DATE;
	private final Clock clock;

	public ParquetPathBuilder(Clock clock) {
		this.clock = clock;
	}

	public Path buildLocalPath(Path baseDir, String directory, String topic) {
		String date = LocalDate.now(clock).format(DATE_FORMAT);
		Path prefixPath = resolvePrefix(baseDir, directory);
		return prefixPath
				.resolve("topic=" + topic)
				.resolve("date=" + date)
				.resolve(UUID.randomUUID() + ".parquet");
	}

	public String buildS3Key(String prefix, String topic) {
		String date = LocalDate.now(clock).format(DATE_FORMAT);
		String normalizedPrefix = normalizePrefix(prefix);
		return normalizedPrefix
				+ "topic=" + topic + "/"
				+ "date=" + date + "/"
				+ UUID.randomUUID() + ".parquet";
	}

	private Path resolvePrefix(Path baseDir, String directory) {
		if (directory == null || directory.isBlank()) {
			return baseDir;
		}
		Path provided = Path.of(directory);
		return provided.isAbsolute() ? provided : baseDir.resolve(provided);
	}

	private String normalizePrefix(String prefix) {
		if (prefix == null || prefix.isBlank()) {
			return "";
		}
		String normalized = prefix.trim();
		if (normalized.startsWith("/")) {
			normalized = normalized.substring(1);
		}
		if (!normalized.endsWith("/")) {
			normalized = normalized + "/";
		}
		return normalized;
	}
}
