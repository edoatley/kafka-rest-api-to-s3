package com.example.kafka.s3.sink.writer;

import java.nio.file.Path;

import com.example.kafka.s3.sink.config.TargetType;

public record WriteTarget(TargetType type, Path localPath, String bucket, String key) {

	public static WriteTarget local(Path localPath) {
		return new WriteTarget(TargetType.LOCAL, localPath, null, null);
	}

	public static WriteTarget s3(String bucket, String key) {
		return new WriteTarget(TargetType.S3, null, bucket, key);
	}
}
