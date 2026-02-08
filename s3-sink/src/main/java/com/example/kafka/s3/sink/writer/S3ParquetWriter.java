package com.example.kafka.s3.sink.writer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.stereotype.Component;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.parquet.ParquetConverter;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Component
public class S3ParquetWriter {

	private final ParquetConverter parquetConverter;
	private final S3Client s3Client;

	public S3ParquetWriter(ParquetConverter parquetConverter, S3Client s3Client) {
		this.parquetConverter = parquetConverter;
		this.s3Client = s3Client;
	}

	public void write(DecodedAvro decoded, String bucket, String key) {
		Path tempFile = null;
		try {
			tempFile = Files.createTempFile("kafka-s3-sink-", ".parquet");
			Files.deleteIfExists(tempFile);
			parquetConverter.write(decoded, tempFile);
			PutObjectRequest request = PutObjectRequest.builder()
					.bucket(bucket)
					.key(key)
					.build();
			s3Client.putObject(request, RequestBody.fromFile(tempFile));
		} catch (IOException ex) {
			throw new ParquetWriteException("Failed to write parquet file to S3", ex);
		} finally {
			if (tempFile != null) {
				try {
					Files.deleteIfExists(tempFile);
				} catch (IOException ignored) {
				}
			}
		}
	}
}
