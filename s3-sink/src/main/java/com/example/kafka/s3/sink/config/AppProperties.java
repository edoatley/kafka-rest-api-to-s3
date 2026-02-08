package com.example.kafka.s3.sink.config;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

	private String sourceTopics = "";
	private String dlqTopic = "kafka-s3-sink-dlq";
	private List<TopicMapping> mappings = new ArrayList<>();
	private LocalProperties local = new LocalProperties();
	private S3Properties s3 = new S3Properties();

	public String getSourceTopics() {
		return sourceTopics;
	}

	public void setSourceTopics(String sourceTopics) {
		this.sourceTopics = sourceTopics;
	}

	public String getDlqTopic() {
		return dlqTopic;
	}

	public void setDlqTopic(String dlqTopic) {
		this.dlqTopic = dlqTopic;
	}

	public List<TopicMapping> getMappings() {
		return mappings;
	}

	public void setMappings(List<TopicMapping> mappings) {
		this.mappings = mappings;
	}

	public LocalProperties getLocal() {
		return local;
	}

	public void setLocal(LocalProperties local) {
		this.local = local;
	}

	public S3Properties getS3() {
		return s3;
	}

	public void setS3(S3Properties s3) {
		this.s3 = s3;
	}

	public static class TopicMapping {
		private String topic;
		private TargetType destination = TargetType.LOCAL;
		private String directory;
		private String bucket;
		private String prefix;

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public TargetType getDestination() {
			return destination;
		}

		public void setDestination(TargetType destination) {
			this.destination = destination;
		}

		public String getDirectory() {
			return directory;
		}

		public void setDirectory(String directory) {
			this.directory = directory;
		}

		public String getBucket() {
			return bucket;
		}

		public void setBucket(String bucket) {
			this.bucket = bucket;
		}

		public String getPrefix() {
			return prefix;
		}

		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}
	}

	public static class LocalProperties {
		private Path baseDir = Path.of("/tmp/kafka-s3-sink");

		public Path getBaseDir() {
			return baseDir;
		}

		public void setBaseDir(Path baseDir) {
			this.baseDir = baseDir;
		}
	}

	public static class S3Properties {
		private String region = "us-east-1";
		private String bucket;
		private String prefix = "";
		private URI endpoint;
		private boolean pathStyle = false;
		private String accessKeyId;
		private String secretAccessKey;

		public String getRegion() {
			return region;
		}

		public void setRegion(String region) {
			this.region = region;
		}

		public String getBucket() {
			return bucket;
		}

		public void setBucket(String bucket) {
			this.bucket = bucket;
		}

		public String getPrefix() {
			return prefix;
		}

		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}

		public URI getEndpoint() {
			return endpoint;
		}

		public void setEndpoint(URI endpoint) {
			this.endpoint = endpoint;
		}

		public boolean isPathStyle() {
			return pathStyle;
		}

		public void setPathStyle(boolean pathStyle) {
			this.pathStyle = pathStyle;
		}

		public String getAccessKeyId() {
			return accessKeyId;
		}

		public void setAccessKeyId(String accessKeyId) {
			this.accessKeyId = accessKeyId;
		}

		public String getSecretAccessKey() {
			return secretAccessKey;
		}

		public void setSecretAccessKey(String secretAccessKey) {
			this.secretAccessKey = secretAccessKey;
		}
	}
}
