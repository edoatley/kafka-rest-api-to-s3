package com.example.kafka.s3.sink.writer;

public class ParquetWriteException extends RuntimeException {

	public ParquetWriteException(String message, Throwable cause) {
		super(message, cause);
	}
}
