plugins {
	java
	id("org.springframework.boot") version "4.0.2"
	id("io.spring.dependency-management") version "1.1.7"
}

description = "Kafka S3 Sink project for Spring Boot"

extra["springCloudVersion"] = "2025.1.0"

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	implementation("org.springframework.cloud:spring-cloud-stream")
	implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")
	implementation("org.apache.avro:avro:1.11.4")
	implementation("io.confluent:kafka-avro-serializer:7.6.0")
	implementation("org.apache.parquet:parquet-avro:1.14.3")
	implementation("org.apache.hadoop:hadoop-common:3.4.1") {
		exclude(group = "org.slf4j", module = "slf4j-reload4j")
	}
	implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1") {
		exclude(group = "org.slf4j", module = "slf4j-reload4j")
	}
	implementation("software.amazon.awssdk:s3:2.25.66")
	implementation("software.amazon.awssdk:sts:2.25.66")
	annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
	testImplementation("org.springframework.cloud:spring-cloud-stream-test-binder")
	testImplementation("org.testcontainers:junit-jupiter:1.20.6")
	testImplementation("org.testcontainers:localstack:1.20.6")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

dependencyManagement {
	imports {
		mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
	}
}
