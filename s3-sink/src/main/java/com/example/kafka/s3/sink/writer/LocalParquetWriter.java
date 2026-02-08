package com.example.kafka.s3.sink.writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.stereotype.Component;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.parquet.ParquetConverter;

@Component
public class LocalParquetWriter {

	private final ParquetConverter parquetConverter;

	public LocalParquetWriter(ParquetConverter parquetConverter) {
		this.parquetConverter = parquetConverter;
	}

	public Path write(DecodedAvro decoded, Path outputPath) {
		try {
			Files.createDirectories(outputPath.getParent());
			parquetConverter.write(decoded, outputPath);
			return outputPath;
		} catch (IOException ex) {
			throw new ParquetWriteException("Failed to write parquet file locally", ex);
		}
	}
}
