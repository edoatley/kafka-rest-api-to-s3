package com.example.kafka.s3.sink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaS3SinkApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaS3SinkApplication.class, args);
	}

}
