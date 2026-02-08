package com.example.kafka.s3.sink.writer;

import org.springframework.stereotype.Component;

import com.example.kafka.s3.sink.avro.DecodedAvro;
import com.example.kafka.s3.sink.config.TargetType;
import com.example.kafka.s3.sink.mapping.TopicMappingResolver;

@Component
public class WriterRouter {

	private final TopicMappingResolver resolver;
	private final LocalParquetWriter localWriter;
	private final S3ParquetWriter s3Writer;

	public WriterRouter(TopicMappingResolver resolver,
			LocalParquetWriter localWriter,
			S3ParquetWriter s3Writer) {
		this.resolver = resolver;
		this.localWriter = localWriter;
		this.s3Writer = s3Writer;
	}

	public void write(String topic, DecodedAvro decoded) {
		WriteTarget target = resolver.resolve(topic);
		if (target.type() == TargetType.LOCAL) {
			localWriter.write(decoded, target.localPath());
			return;
		}
		s3Writer.write(decoded, target.bucket(), target.key());
	}
}
