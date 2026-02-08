package com.example.kafka.s3.sink;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;

@SpringBootTest(properties = "spring.cloud.stream.bindings.avroToParquet-in-0.destination=unused")
@Import(TestChannelBinderConfiguration.class)
class KafkaS3SinkApplicationTests {

	@Test
	void contextLoads() {
	}

}
