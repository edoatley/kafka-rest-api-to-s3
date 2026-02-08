package com.example.kafka.s3.sink.writer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.parquet.ParquetConverter;
import com.example.kafka.s3.sink.testutil.AvroTestData;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

@Testcontainers(disabledWithoutDocker = true)
class S3ParquetWriterIntegrationTest {

	@Container
	static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
			DockerImageName.parse("localstack/localstack:3.7.1"))
			.withServices(LocalStackContainer.Service.S3);

	@Test
	void uploadsParquetToS3() {
		try (S3Client client = S3Client.builder()
				.endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
				.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
				.region(Region.of(LOCALSTACK.getRegion()))
				.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
				.build()) {
			String bucket = "parquet-bucket";
			String key = "topic=events/date=2024-01-01/test.parquet";
			client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());

			Schema schema = AvroTestData.sampleSchema();
			GenericRecord record = AvroTestData.sampleRecord(schema, 5, "s3");
			DecodedAvro decoded = new DecodedAvro(schema, List.of(record));

			S3ParquetWriter writer = new S3ParquetWriter(new ParquetConverter(new Configuration()), client);
			writer.write(decoded, bucket, key);

			HeadObjectResponse response = client.headObject(HeadObjectRequest.builder()
					.bucket(bucket)
					.key(key)
					.build());
			assertThat(response.contentLength()).isGreaterThan(0);
		}
	}
}
