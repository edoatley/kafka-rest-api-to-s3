package com.example.kafka.s3.sink.config;

import java.util.Optional;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

@Configuration
public class S3ClientConfig {

	@Bean
	public S3Client s3Client(AppProperties properties) {
		AppProperties.S3Properties s3 = properties.getS3();
		S3ClientBuilder builder = S3Client.builder()
				.region(Region.of(s3.getRegion()))
				.serviceConfiguration(S3Configuration.builder()
						.pathStyleAccessEnabled(s3.isPathStyle())
						.build());

		Optional.ofNullable(s3.getEndpoint()).ifPresent(builder::endpointOverride);

		if (s3.getAccessKeyId() != null && s3.getSecretAccessKey() != null) {
			builder.credentialsProvider(StaticCredentialsProvider.create(
					AwsBasicCredentials.create(s3.getAccessKeyId(), s3.getSecretAccessKey())));
		} else {
			builder.credentialsProvider(DefaultCredentialsProvider.create());
		}

		return builder.build();
	}
}
