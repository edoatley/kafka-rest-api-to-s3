package com.example.kafka.s3.sink.avro;

public class AvroDecodingException extends RuntimeException {

	public AvroDecodingException(String message, Throwable cause) {
		super(message, cause);
	}
}
